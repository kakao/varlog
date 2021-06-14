package executor

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logio"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

func (e *executor) Append(ctx context.Context, data []byte, backups ...snpb.Replica) (types.GLSN, error) {
	// FIXME: e.guard() can be removed, but doing ops to storage after closing should be
	// handled. Mostly, trim and read can be occurred after clsoing storage.
	if err := e.guard(); err != nil {
		return types.InvalidGLSN, err
	}
	defer e.unguard()

	if err := e.mutable(); err != nil {
		return types.InvalidGLSN, err
	}

	twg := newTaskWaitGroup()
	wt := newPrimaryWriteTask(twg, data, backups)
	defer func() {
		wt.release()
		twg.release()
	}()

	// Note: It should be called within the scope of the mutex for stateBarrier.
	wt.validate = func() error {
		if !e.isPrimay() {
			return errors.Wrapf(verrors.ErrInvalid, "backup replica")
		}
		if !snpb.EqualReplicas(e.primaryBackups[1:], wt.backups) {
			return errors.Wrapf(verrors.ErrInvalid, "replicas mismatch: expected=%+v, actual=%+v", e.primaryBackups[1:], wt.backups)
		}
		return nil
	}

	if err := e.writer.send(ctx, wt); err != nil {
		twg.wg.Done()
		twg.wg.Wait()
		return types.InvalidGLSN, err
	}

	twg.wg.Wait()
	glsn := twg.glsn
	err := twg.err
	return glsn, err
}

func (e *executor) Read(ctx context.Context, glsn types.GLSN) (logEntry types.LogEntry, err error) {
	if glsn.Invalid() {
		return types.InvalidLogEntry, errors.WithStack(verrors.ErrInvalid)
	}

	if err := e.guard(); err != nil {
		return types.InvalidLogEntry, err
	}
	defer e.unguard()

	// TODO: consider context to cancel waiting
	if err := e.decider.waitC(ctx, glsn); err != nil {
		return types.InvalidLogEntry, err
	}

	// TODO: check trimmed
	// TODO: need to reconsider this?
	e.deferredTrim.mu.RLock()
	trimGLSN := e.deferredTrim.glsn
	e.deferredTrim.mu.RUnlock()
	if glsn <= trimGLSN {
		return types.InvalidLogEntry, errors.WithStack(verrors.ErrTrimmed)
	}

	// TODO: trivial optimization, is it needed?
	if glsn > e.lsc.localGLSN.localHighWatermark.Load() {
		// no entry
	}

	// reading logs after closing storage can be handled by e.withRecover().
	/*
		err = e.withRecover(func() error {
			logEntry, err = e.storage.Read(glsn)
			return err
		})
		return
	*/

	return e.storage.Read(glsn)
}

type subscribeEnvImpl struct {
	c        chan storage.ScanResult
	begin    types.GLSN
	end      types.GLSN
	lastGLSN types.GLSN
	stopper  struct {
		cancel  context.CancelFunc
		decider *decidableCondition
	}
	err struct {
		mu sync.Mutex
		e  error
	}
	wg sync.WaitGroup
}

var _ logio.SubscribeEnv = (*subscribeEnvImpl)(nil)

func newSubscribeEnvWithContext(ctx context.Context, queueSize int, begin, end types.GLSN, decider *decidableCondition) (*subscribeEnvImpl, context.Context) {
	se := &subscribeEnvImpl{
		c:     make(chan storage.ScanResult, queueSize),
		begin: begin,
		end:   end,
	}
	ctx, cancel := context.WithCancel(ctx)
	se.stopper.cancel = cancel
	se.stopper.decider = decider
	return se, ctx
}

func (sc *subscribeEnvImpl) ScanResultC() <-chan storage.ScanResult {
	return sc.c
}

func (sc *subscribeEnvImpl) Stop() {
	sc.stopper.decider.change(func() {
		sc.stopper.cancel()
	})
	sc.wg.Wait()
}

func (sc *subscribeEnvImpl) Err() error {
	sc.err.mu.Lock()
	defer sc.err.mu.Unlock()
	return sc.err.e
}

func (sc *subscribeEnvImpl) setErr(err error) {
	sc.err.mu.Lock()
	defer sc.err.mu.Unlock()
	if sc.err.e != nil {
		return
	}
	sc.err.e = err
}

func (e *executor) Subscribe(ctx context.Context, begin, end types.GLSN) (logio.SubscribeEnv, error) {
	if begin >= end {
		return nil, errors.WithStack(verrors.ErrInvalid)
	}

	// TODO: need to reconsider this?
	e.deferredTrim.mu.RLock()
	trimGLSN := e.deferredTrim.glsn
	e.deferredTrim.mu.RUnlock()
	if begin <= trimGLSN {
		return nil, errors.WithStack(verrors.ErrTrimmed)
	}

	if err := e.guard(); err != nil {
		return nil, err
	}
	defer e.unguard()

	return e.subscribe(ctx, begin, end)
}

func (e *executor) subscribe(ctx context.Context, begin, end types.GLSN) (*subscribeEnvImpl, error) {
	subEnv, ctx := newSubscribeEnvWithContext(ctx, int(end-begin), begin, end, e.decider)
	subEnv.wg.Add(1)
	go func() {
		defer func() {
			close(subEnv.c)
			subEnv.wg.Done()
		}()
		lastErr := io.EOF
		if err := e.scanLoop(ctx, subEnv); err != nil {
			lastErr = err
		}
		subEnv.setErr(lastErr)
	}()
	return subEnv, nil
}

func (e *executor) scanLoop(ctx context.Context, subEnv *subscribeEnvImpl) error {
	beginGLSN := subEnv.begin
	endGLSN := subEnv.end

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // canceled subscribe
		default:
		}

		decidable := e.decider.decidable(endGLSN - 1)
		err := e.scan(ctx, subEnv, beginGLSN, endGLSN)
		if err != nil {
			return err
		}

		if decidable {
			// ok, end of subscribe!
			return nil
		}

		// wait & re-scan
		// TODO: how can we stop this blocking if subscribe is canceled or e is closed
		if err := e.decider.waitC(ctx, endGLSN-1); err != nil {
			// canceled subscribe or closed LSE
			return err
		}

		beginGLSN = subEnv.lastGLSN + 1
	}
}

func (e *executor) scan(ctx context.Context, subEnv *subscribeEnvImpl, begin, end types.GLSN) error {
	// TODO: wrap storage.Scan by stateBarrier
	scanner := e.storage.Scan(begin, end)
	defer func() {
		_ = scanner.Close()
	}()

	for {
		result := scanner.Next()
		if !result.Valid() {
			if errors.Is(result.Err, io.EOF) {
				return nil
			}
			return result.Err
		}

		subEnv.lastGLSN = result.LogEntry.GLSN

		select {
		case subEnv.c <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (e *executor) Trim(_ context.Context, glsn types.GLSN) error {
	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	updateDeferredTrim := func() types.GLSN {
		e.deferredTrim.mu.Lock()
		defer e.deferredTrim.mu.Unlock()
		trimGLSN := e.deferredTrim.glsn
		if glsn > trimGLSN {
			e.deferredTrim.glsn = glsn
			trimGLSN = glsn
		}
		return trimGLSN
	}

	globalHighWatermark, _ := e.lsc.reportCommitBase()
	if glsn >= globalHighWatermark-e.deferredTrim.safetyGap {
		return errors.New("too high prefix")
	}

	// TODO: design trimming of commit context, then reconsider Trim API
	trimGLSN := updateDeferredTrim()
	// NB: In some cases, localLWM >= localHWM (need to reconsider?)
	e.lsc.localGLSN.localLowWatermark.Store(trimGLSN)
	if glsn >= e.lsc.localGLSN.localHighWatermark.Load() {
		return nil
	}

	return e.storage.DeleteCommitted(trimGLSN + 1)
}
