package executor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/mathutil"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
)

type writerConfig struct {
	queueSize  int
	batchSize  int
	strg       storage.Storage
	lsc        *logStreamContext
	committer  committer
	replicator replicator
	state      stateProvider
}

func (c writerConfig) validate() error {
	if c.queueSize <= 0 {
		return errors.Wrap(verrors.ErrInvalid, "writer: zero or negative queue size")
	}
	if c.batchSize <= 0 {
		return errors.Wrap(verrors.ErrInvalid, "writer: zero or negative batch size")
	}
	if c.strg == nil {
		return errors.Wrap(verrors.ErrInvalid, "writer: no storage")
	}
	if c.lsc == nil {
		return errors.Wrap(verrors.ErrInvalid, "writer: no log stream context")
	}
	if c.committer == nil {
		return errors.Wrap(verrors.ErrInvalid, "writer: no committer")
	}
	if c.replicator == nil {
		return errors.Wrap(verrors.ErrInvalid, "writer: no replicator")
	}
	if c.state == nil {
		return errors.Wrap(verrors.ErrInvalid, "writer: no state provider")
	}
	return nil
}

type WriterOption interface {
	applyWriter(*writerConfig)
}

/*
type writerState int32

const (
	writerStateInit writerState = iota
	writerStateRun
	writerStateClosed
)
*/
const (
	writerStateInit   = 0
	writerStateRun    = 1
	writerStateClosed = 2
)

type writer interface {
	send(ctx context.Context, tb *writeTask) error
	stop()
	waitForDrainage(ctx context.Context) error
}

type writerImpl struct {
	writerConfig

	q writeQueue

	dispatcher struct {
		runner *runner.Runner
		cancel context.CancelFunc
	}

	inflight int64

	// NOTE: atomic variable can be used to stop writer.
	// Can it avoid leak of appendTask absolutely?
	// ws int32

	// NOTE: Mutex is more expensive than atomic variable.
	running struct {
		val bool
		mu  sync.RWMutex
	}

	popCv struct {
		cv *sync.Cond
		mu sync.Mutex
	}

	writeTaskBatch      []*writeTask
	commitWaitTaskBatch []*commitWaitTask
	replicateTaskBatch  []*replicateTask
}

var _ writer = (*writerImpl)(nil)

func newWriter(cfg writerConfig) (*writerImpl, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	w := &writerImpl{
		writerConfig:        cfg,
		writeTaskBatch:      make([]*writeTask, 0, cfg.batchSize),
		commitWaitTaskBatch: make([]*commitWaitTask, 0, cfg.batchSize),
		replicateTaskBatch:  make([]*replicateTask, 0, cfg.batchSize),
	}
	if err := w.init(); err != nil {
		return nil, err
	}
	// w.ws = writerStateRun
	return w, nil
}

func (w *writerImpl) init() error {
	q, err := newWriteQueue(w.queueSize)
	if err != nil {
		return err
	}
	w.q = q

	w.popCv.cv = sync.NewCond(&w.popCv.mu)

	r := runner.New("writer", nil)
	cancel, err := r.Run(w.writeLoop)
	if err != nil {
		return err
	}
	w.dispatcher.runner = r
	w.dispatcher.cancel = cancel
	w.running.val = true

	return nil
}

func (w *writerImpl) send(ctx context.Context, tb *writeTask) error {
	if tb == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}

	/*
		if atomic.LoadInt32(&w.ws) != writerStateRun {
			return errors.WithStack(verrors.ErrClosed)
		}
	*/

	w.running.mu.RLock()
	defer w.running.mu.RUnlock()
	if !w.running.val {
		return errors.WithStack(verrors.ErrClosed)
	}

	if err := w.state.mutableWithBarrier(); err != nil {
		return err
	}
	defer w.state.releaseBarrier()

	// Check whether replicas are correct
	if err := tb.validate(); err != nil {
		return err
	}
	atomic.AddInt64(&w.inflight, 1)
	return w.q.pushWithContext(ctx, tb)
}

func (w *writerImpl) writeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		w.resetBatch()

		if err := w.writeLoopInternal(ctx); err != nil {
			w.state.setSealing()
		}

		w.popCv.cv.L.Lock()
		w.popCv.cv.Signal()
		w.popCv.cv.L.Unlock()
	}
}

func (w *writerImpl) writeLoopInternal(ctx context.Context) error {
	oldLLSN, newLLSN, numPopped, err := w.ready(ctx)
	defer func() {
		atomic.AddInt64(&w.inflight, -numPopped)
	}()
	if err != nil {
		w.batchError(err)
		return err
	}

	if err := w.fanout(ctx, oldLLSN, newLLSN); err != nil {
		// NOTE: ownership of write/replicate tasks is moved.
		return err
	}
	return nil
}

func (w *writerImpl) ready(ctx context.Context) (types.LLSN, types.LLSN, int64, error) {
	numPopped := int64(0)

	t, err := w.q.popWithContext(ctx)
	if err != nil {
		return types.InvalidLLSN, types.InvalidLLSN, numPopped, err
	}
	numPopped++

	oldLLSN := w.lsc.uncommittedLLSNEnd.Load()
	newLLSN := oldLLSN

	// NOTE: These variables check whether tasks in writeQ are mixed by Append RPC and Replicate
	// RPC. They will be removed after stabilizing codebases.
	primary := t.primary
	backup := !t.primary

	if err := w.fillBatch(t, newLLSN); err != nil {
		return types.InvalidLLSN, types.InvalidLLSN, numPopped, err
	}
	newLLSN++

	popSize := mathutil.MinInt(w.batchSize-1, w.q.size())
	for i := 0; i < popSize; i++ {
		t := w.q.pop()
		numPopped++

		if err := w.fillBatch(t, newLLSN); err != nil {
			return types.InvalidLLSN, types.InvalidLLSN, numPopped, err
		}

		newLLSN++

		primary = primary || t.primary
		backup = backup || !t.primary
	}
	// NB: Need to debug
	if primary && backup {
		panic("mixed primary and non-primary requests")
	}
	return oldLLSN, newLLSN, numPopped, nil
}

func (w *writerImpl) fillBatch(wt *writeTask, llsn types.LLSN) error {
	w.writeTaskBatch = append(w.writeTaskBatch, wt)

	//priamry
	if wt.primary {
		// assign llsn
		wt.llsn = llsn
		// put into replicateTaskBatch
		rt := newReplicateTask()
		rt.llsn = wt.llsn
		rt.data = wt.data
		rt.replicas = wt.backups
		w.replicateTaskBatch = append(w.replicateTaskBatch, rt)
		return nil
	}

	// backup: check llsn
	if wt.llsn != llsn {
		return errors.Errorf("llsn mismatch: %d != %d", llsn, wt.llsn)
	}
	return nil
}

func (w *writerImpl) write() (err error) {
	batch := w.strg.NewWriteBatch()

	defer func() {
		err = multierr.Append(err, batch.Close())
		if err != nil {
			w.writeTaskBatchError(w.writeTaskBatch, err)
		}
	}()

	for idx := range w.writeTaskBatch {
		t := w.writeTaskBatch[idx]
		err = batch.Put(t.llsn, t.data)
		if err != nil {
			return err
		}
	}

	err = batch.Apply()
	return
}

func (w *writerImpl) sendToCommitter(ctx context.Context) (err error) {
	idx := 0
	for idx < len(w.commitWaitTaskBatch) {
		cwt := w.commitWaitTaskBatch[idx]
		err = w.committer.sendCommitWaitTask(ctx, cwt)
		if err != nil {
			break
		}
		idx++
	}
	w.commitWaitTaskBatchError(w.commitWaitTaskBatch[idx:], err)
	return err
}

func (w *writerImpl) sendToReplicator(ctx context.Context) (err error) {
	idx := 0
	for idx < len(w.replicateTaskBatch) {
		rt := w.replicateTaskBatch[idx]
		err = w.replicator.send(ctx, rt)
		if err != nil {
			break
		}
		idx++
	}
	w.releaseReplicateTaskBatch(w.replicateTaskBatch[idx:])
	return err
}

func (w *writerImpl) fanout(ctx context.Context, oldLLSN, newLLSN types.LLSN) error {
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		// write pipeline
		if err := w.write(); err != nil {
			return err
		}

		// After successful writing, commitWatiTaskBatch should be made.
		for _, wt := range w.writeTaskBatch {
			var cwt *commitWaitTask
			if wt.primary { // primary
				cwt = newCommitWaitTask(wt.llsn, wt.twg)
			} else { // backup
				cwt = newCommitWaitTask(wt.llsn, nil)
			}
			w.commitWaitTaskBatch = append(w.commitWaitTaskBatch, cwt)
		}

		// NOTE: To avoid race, Below condition expression should be called before
		// commitWaitTask is sent to committer.
		// Assumes that the below expression is executed after commitWaitTask is passed to
		// committer and committer calls the Done of twg very quickly. Since the Done of twg
		// is called, the Append RPC tries to release the writeTask. At that times, if the
		// conditions are executed, the race condition will happend.
		for _, wt := range w.writeTaskBatch {
			// Replication tasks succeed after writing the logs into the
			// storage. Thus here notifies the Replicate RPC handler to return
			// nil.
			if !wt.primary {
				wt.twg.done(nil)
			}
		}

		// NOTE: Updating uncommittedLLSNEnd should be done when the writing logs to the
		// storage succeeds. The reason why the uncommittedLLSNEnd is changed after sending
		// commitWaitTasks to the committer is that the committer compares the size of
		// commitWaitQ with numCommits.
		// See [#VARLOG-444](VARLOG-444).
		defer func() {
			if !w.lsc.uncommittedLLSNEnd.CompareAndSwap(oldLLSN, newLLSN) {
				// NOTE: If this CAS operation fails, it means other goroutine changes
				// uncommittedLLSNEnd of LogStreamContext.
				panic(errors.Errorf(
					"uncommittedLLSNEnd swap failure: current=%d old=%d new=%d: more than one writer?",
					w.lsc.uncommittedLLSNEnd.Load(), oldLLSN, newLLSN,
				))
			}

		}()
		return w.sendToCommitter(ctx)
	})
	grp.Go(func() error {
		// replication
		return w.sendToReplicator(ctx)
	})
	return grp.Wait()
}

func (w *writerImpl) stop() {
	/*
		if !atomic.CompareAndSwapInt32(&w.ws, writerStateRun, writerStateClosed) {
			return
		}
	*/
	w.running.mu.Lock()
	w.running.val = false
	w.running.mu.Unlock()

	w.dispatcher.cancel()
	w.dispatcher.runner.Stop()

	w.drainQueue(errors.WithStack(verrors.ErrClosed))
	w.resetBatch()
}

func (w *writerImpl) drainQueue(err error) {
	for w.q.size() > 0 {
		tb := w.q.pop()
		tb.twg.err = err
		tb.twg.wg.Done()
	}
}

func (w *writerImpl) waitForDrainage(ctx context.Context) error {
	done := make(chan struct{})
	var wg sync.WaitGroup

	defer func() {
		close(done)
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			w.popCv.cv.L.Lock()
			w.popCv.cv.Signal()
			w.popCv.cv.L.Unlock()
		case <-done:
		}
	}()

	w.popCv.cv.L.Lock()
	defer w.popCv.cv.L.Unlock()
	for atomic.LoadInt64(&w.inflight) > 0 && ctx.Err() == nil {
		w.popCv.cv.Wait()
	}
	if w.q.size() == 0 {
		return nil
	}
	return ctx.Err()
}

func (w *writerImpl) resetBatch() {
	w.writeTaskBatch = w.writeTaskBatch[0:0]
	w.commitWaitTaskBatch = w.commitWaitTaskBatch[0:0]
	w.replicateTaskBatch = w.replicateTaskBatch[0:0]
}

// batchError is called only when ready fails, thus its error is writeErr.
func (w *writerImpl) batchError(err error) {
	w.writeTaskBatchError(w.writeTaskBatch, err)
	w.releaseReplicateTaskBatch(w.replicateTaskBatch)
}

func (w *writerImpl) writeTaskBatchError(batch []*writeTask, err error) {
	for i := 0; i < len(batch); i++ {
		wt := batch[i]
		wt.twg.err = err
		wt.twg.wg.Done()
	}
}

func (w *writerImpl) commitWaitTaskBatchError(batch []*commitWaitTask, err error) {
	for i := 0; i < len(batch); i++ {
		cwt := batch[i]
		cwt.twg.done(err)
	}
}

// releaseReplicateTaskBatch releases replicateTasks of the given batch. It is called when sending
// replication tasks to the relicateQ fails.
func (w *writerImpl) releaseReplicateTaskBatch(batch []*replicateTask) {
	for i := 0; i < len(batch); i++ {
		rt := batch[i]
		rt.release()
	}
}
