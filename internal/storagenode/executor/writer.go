package executor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/mathutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type writerConfig struct {
	queueSize  int
	batchSize  int
	strg       storage.Storage
	lsc        *logStreamContext
	committer  committer
	replicator replicator
	state      stateProvider
	metrics    *telemetry.Metrics
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
	if c.metrics == nil {
		return errors.Wrap(verrors.ErrInvalid, "writer: no measurable")
	}
	return nil
}

type WriterOption interface {
	applyWriter(*writerConfig)
}

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

	writeBatch storage.WriteBatch
	// writeTaskBatch []*writeTask
	cwtBatchPool *commitWaitTaskBatchPool
	rtBatchPool  *replicateTaskBatchPool
}

var _ writer = (*writerImpl)(nil)

func newWriter(cfg writerConfig) (*writerImpl, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	w := &writerImpl{
		writerConfig: cfg,
		// writeTaskBatch: make([]*writeTask, 0, cfg.batchSize),
		cwtBatchPool: newCommitWaitTaskBatchPool(cfg.batchSize),
		rtBatchPool:  newReplicateTaskBatchPool(cfg.batchSize),
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
	if err := w.q.pushWithContext(ctx, tb); err != nil {
		atomic.AddInt64(&w.inflight, -1)
		return err
	}
	w.metrics.WriteQueueTasks.Add(ctx, 1)

	return nil
}

func (w *writerImpl) writeLoop(ctx context.Context) {
	for ctx.Err() == nil {
		w.resetBatch()

		if err := w.writeLoopInternal(ctx); err != nil {
			w.state.setSealingWithReason(err)
		}

		w.popCv.cv.L.Lock()
		w.popCv.cv.Signal()
		w.popCv.cv.L.Unlock()
	}
}

func (w *writerImpl) writeLoopInternal(ctx context.Context) error {
	res, err := w.ready(ctx)
	defer func() {
		w.metrics.WriteQueueTasks.Add(ctx, -res.numPopped)
		atomic.AddInt64(&w.inflight, -res.numPopped)
	}()
	if err != nil {
		w.batchError(res.cwtBatch, res.rtBatch, err)
		return err
	}

	if err := w.fanout(ctx, &res); err != nil {
		w.batchError(res.cwtBatch, res.rtBatch, err)
		return err
	}
	return nil
}

type readyResult struct {
	oldLLSN   types.LLSN
	newLLSN   types.LLSN
	numPopped int64
	cwtBatch  []*commitWaitTask
	rtBatch   []*replicateTask
}

func (w *writerImpl) ready(ctx context.Context) (res readyResult, err error) {
	startReady := time.Now()
	defer func() {
		if err != nil && w.writeBatch != nil {
			err = multierr.Append(err, w.writeBatch.Close())
			w.writeBatch = nil
		}
		w.metrics.WriteReadyTime.Record(ctx, time.Since(startReady).Microseconds())
	}()

	res.cwtBatch = w.cwtBatchPool.new()
	res.rtBatch = w.rtBatchPool.new()
	res.numPopped = int64(0)

	wt, err := w.q.popWithContext(ctx)
	if err != nil {
		return res, err
	}
	res.numPopped++
	wt.poppedTime = time.Now()

	w.writeBatch = w.strg.NewWriteBatch()
	res.oldLLSN = w.lsc.uncommittedLLSNEnd.Load()
	res.newLLSN = res.oldLLSN

	// NOTE: These variables check whether tasks in writeQ are mixed by Append RPC and Replicate
	// RPC. They will be removed after stabilizing codebases.
	primary := wt.primary
	backup := !wt.primary

	res.cwtBatch, res.rtBatch, err = w.fillBatch(wt, res.newLLSN, res.cwtBatch, res.rtBatch)
	if err != nil {
		return res, err
	}
	res.newLLSN++

	popSize := mathutil.MinInt(w.batchSize-1, w.q.size())
	for i := 0; i < popSize; i++ {
		wt := w.q.pop()
		primary = primary || wt.primary
		backup = backup || !wt.primary

		res.numPopped++
		wt.poppedTime = time.Now()

		res.cwtBatch, res.rtBatch, err = w.fillBatch(wt, res.newLLSN, res.cwtBatch, res.rtBatch)
		if err != nil {
			return res, err
		}
		res.newLLSN++
	}
	// NB: Need to debug
	if primary && backup {
		panic("mixed primary and non-primary requests")
	}
	return res, nil
}

func (w *writerImpl) fillBatch(wt *writeTask, llsn types.LLSN, cwtBatch []*commitWaitTask, rtBatch []*replicateTask) ([]*commitWaitTask, []*replicateTask, error) {
	var err error

	defer func() {
		wt.twg.done(err)
		wt.release()
	}()

	if !wt.primary && wt.llsn != llsn {
		err = errors.Errorf("llsn mismatch: %d != %d", llsn, wt.llsn)
		return cwtBatch, rtBatch, err
	}

	// TODO(jun): Use write batch of storage rather than writeTaskBatch.
	// w.writeTaskBatch = append(w.writeTaskBatch, wt)
	err = w.writeBatch.Put(llsn, wt.data)
	if err != nil {
		return cwtBatch, rtBatch, err
	}

	// priamry
	if wt.primary {
		twg := wt.twg
		wt.twg = nil

		twg.llsn = llsn

		// assign llsn
		// wt.llsn = llsn

		// replicateTaskBatch
		rt := newReplicateTask()
		rt.llsn = llsn
		rt.data = wt.data
		rt.replicas = wt.backups
		// w.replicateTaskBatch = append(w.replicateTaskBatch, rt)
		rtBatch = append(rtBatch, rt)

		// commitWaitTaskBatch
		cwt := newCommitWaitTask(llsn, twg)
		// w.commitWaitTaskBatch = append(w.commitWaitTaskBatch, cwt)
		cwtBatch = append(cwtBatch, cwt)
		return cwtBatch, rtBatch, nil
	}

	// backup
	cwt := newCommitWaitTask(llsn, nil)
	// w.commitWaitTaskBatch = append(w.commitWaitTaskBatch, cwt)
	cwtBatch = append(cwtBatch, cwt)
	return cwtBatch, rtBatch, nil
}

func (w *writerImpl) write() error {
	batchSize := w.writeBatch.Size()
	w.metrics.WriteBatchSize.Record(context.TODO(), int64(batchSize))

	batch := w.writeBatch
	w.writeBatch = nil
	return multierr.Append(batch.Apply(), batch.Close())

	/*
		// batch := w.strg.NewWriteBatch()

		defer func() {
			w.writeTaskBatch = w.writeTaskBatch[0:0]
			err = multierr.Append(err, batch.Close())
		}()

		idx := 0
		for idx < batchSize {
			wt := w.writeTaskBatch[idx]
			err = batch.Put(wt.llsn, wt.data)
			if err != nil {
				break
			}
			wt.annotate(context.TODO(), w.metrics)
			wt.release()
			idx++
		}
		for i := idx; i < batchSize; i++ {
			wt := w.writeTaskBatch[i]
			wt.annotate(context.TODO(), w.metrics)
			wt.release()
		}
		if err != nil {
			return err
		}
		err = batch.Apply()
		return
	*/
}

func (w *writerImpl) sendReplicateTaskBatch(ctx context.Context, rtBatch []*replicateTask) (err error) {
	now := time.Now()
	idx := 0
	for idx < len(rtBatch) {
		rt := rtBatch[idx]
		rt.createdTime = now
		err = w.replicator.send(ctx, rt)
		if err != nil {
			break
		}
		idx++
	}
	w.releaseReplicateTaskBatch(rtBatch[idx:])
	w.rtBatchPool.release(rtBatch)
	return err
}

func (w *writerImpl) sendCommitWaitTaskBatch(ctx context.Context, cwtBatch []*commitWaitTask) (err error) {
	idx := 0
	for idx < len(cwtBatch) {
		cwt := cwtBatch[idx]
		err = w.committer.sendCommitWaitTask(ctx, cwt)
		if err != nil {
			break
		}
		idx++
	}
	w.commitWaitTaskBatchError(cwtBatch[idx:], err)
	w.cwtBatchPool.release(cwtBatch)
	return err
}

func (w *writerImpl) fanout(ctx context.Context, res *readyResult) error {
	startFanout := time.Now()
	defer func() {
		w.metrics.WriteFanoutTime.Record(ctx, time.Since(startFanout).Microseconds())
	}()

	err := multierr.Append(
		w.sendReplicateTaskBatch(ctx, res.rtBatch),
		w.sendCommitWaitTaskBatch(ctx, res.cwtBatch),
	)
	res.rtBatch = nil
	res.cwtBatch = nil
	w.metrics.WriteFanoutSendTime.Record(ctx, time.Since(startFanout).Microseconds())

	if err != nil {
		err = multierr.Append(err, w.writeBatch.Close())
		w.writeBatch = nil
		// w.writeTaskBatchError(w.writeTaskBatch, err)
		// w.writeTaskBatch = w.writeTaskBatch[0:0]
		return err
	}

	startWrite := time.Now()
	err = w.write()
	now := time.Now()
	w.metrics.WritePureTime.Record(ctx, now.Sub(startWrite).Microseconds())
	/*
		for _, wt := range w.writeTaskBatch {
			wt.processingTime = now
			wt.annotate(ctx, w.metrics)
		}
		w.writeTaskBatchError(w.writeTaskBatch, err)
		w.writeTaskBatch = w.writeTaskBatch[0:0]
	*/

	// NOTE: Updating uncommittedLLSNEnd should be done when the writing logs to the
	// storage succeeds. The reason why the uncommittedLLSNEnd is changed after sending
	// commitWaitTasks to the committer is that the committer compares the size of
	// commitWaitQ with numCommits.
	// See [#VARLOG-444](https://jira.daumkakao.com/browse/VARLOG-444).
	if err == nil && !w.lsc.uncommittedLLSNEnd.CompareAndSwap(res.oldLLSN, res.newLLSN) {
		// NOTE: If this CAS operation fails, it means other goroutine changes
		// uncommittedLLSNEnd of LogStreamContext.
		panic(errors.Errorf(
			"uncommittedLLSNEnd swap failure: current=%d old=%d new=%d: more than one writer?",
			w.lsc.uncommittedLLSNEnd.Load(), res.oldLLSN, res.newLLSN,
		))
	}
	w.metrics.WriteFanoutWriteTime.Record(ctx, time.Since(startWrite).Microseconds())
	return err
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
	// NOTE: Remained commitWaitTasks in commitWaitTaskBatch should be cleared.
	// Some Append RPCs can wait for taskWaitGroups to be done.
	w.batchError(nil, nil, verrors.ErrClosed)
	w.resetBatch()
}

// drainQueue clears writeTasks in writeQueue.
func (w *writerImpl) drainQueue(err error) {
	for w.q.size() > 0 {
		wt := w.q.pop()
		// In a primary replica, twg in replicateTask is not nil.
		if wt.twg != nil {
			wt.twg.err = err
			wt.twg.wg.Done()
		}
		wt.release()
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
	// w.writeTaskBatch = w.writeTaskBatch[0:0]
}

// batchError is called only when ready fails, thus its error is writeErr.
func (w *writerImpl) batchError(cwtBatch []*commitWaitTask, rtBatch []*replicateTask, err error) {
	// w.writeTaskBatchError(w.writeTaskBatch, err)
	if cwtBatch != nil {
		w.commitWaitTaskBatchError(cwtBatch, err)
		w.cwtBatchPool.release(cwtBatch)
	}
	if rtBatch != nil {
		w.releaseReplicateTaskBatch(rtBatch)
		w.rtBatchPool.release(rtBatch)
	}
}

// writeTaskBatchError notifies the argument err to taskWaitGroups of writeTaskBatch if the twg is
// not nil.
func (w *writerImpl) writeTaskBatchError(batch []*writeTask, err error) {
	for i := 0; i < len(batch); i++ {
		wt := batch[i]
		wt.twg.done(err)
		wt.release()
	}
}

// commitWaitTaskBatchError notifies the argument err to taskWaitGroups of commitWaitTask if the twg
// is not nil.
// Note that Append RPCs can wait for taskWaitGroups to be done.
func (w *writerImpl) commitWaitTaskBatchError(batch []*commitWaitTask, err error) {
	for i := 0; i < len(batch); i++ {
		cwt := batch[i]
		cwt.twg.done(err)
		cwt.release()
	}
}

// releaseReplicateTaskBatch releases replicateTasks in the argument batch.
// It is called when sending replicateTask to the replicator fails.
func (w *writerImpl) releaseReplicateTaskBatch(batch []*replicateTask) {
	for i := 0; i < len(batch); i++ {
		rt := batch[i]
		rt.release()
	}
}
