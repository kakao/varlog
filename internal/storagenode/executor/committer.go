package executor

//go:generate mockgen -self_package github.com/kakao/varlog/internal/storagenode/executor -package executor -source committer.go -destination committer_mock.go -mock_names committer=MockCommitter

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/mathutil"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
)

type committerConfig struct {
	commitTaskQueueSize int
	commitTaskBatchSize int

	commitQueueSize int

	strg    storage.Storage
	lsc     *logStreamContext
	decider *decidableCondition
	state   stateProvider
}

func (c committerConfig) validate() error {
	if c.commitTaskQueueSize <= 0 {
		return errors.Wrap(verrors.ErrInvalid, "committer : zero or negative queue size")
	}
	if c.commitTaskBatchSize <= 0 {
		return errors.Wrap(verrors.ErrInvalid, "committer : zero or negative batch size")
	}
	if c.commitQueueSize <= 0 {
		return errors.Wrap(verrors.ErrInvalid, "committer : zero or negative queue size")
	}
	if c.strg == nil {
		return errors.Wrap(verrors.ErrInvalid, "committer: no storage")
	}
	if c.lsc == nil {
		return errors.Wrap(verrors.ErrInvalid, "committer: no log stream context")
	}
	if c.decider == nil {
		return errors.Wrap(verrors.ErrInvalid, "committer: no decider")
	}
	if c.state == nil {
		return errors.Wrap(verrors.ErrInvalid, "committer: no state provider")
	}
	return nil
}

type committer interface {
	sendCommitWaitTask(ctx context.Context, cwt *commitWaitTask) error
	sendCommitTask(ctx context.Context, ct *commitTask) error
	drainCommitWaitQ(err error)
	stop()
	waitForDrainageOfCommitTasks(ctx context.Context) error
	commitDirectly(cc storage.CommitContext, requireCommitWaitTasks bool) error
}

type committerImpl struct {
	committerConfig

	commitTaskQ     commitTaskQueue
	commitTaskBatch []*commitTask

	commitWaitQ commitWaitQueue

	dispatcher struct {
		runner *runner.Runner
		cancel context.CancelFunc
		mu     sync.Mutex
	}

	running struct {
		val bool
		mu  sync.RWMutex
	}

	inflightCommitTasks struct {
		cnt int64
		cv  *sync.Cond
		mu  sync.Mutex
	}

	inflightCommitWaitTasks int64
}

var _ committer = (*committerImpl)(nil)

func newCommitter(cfg committerConfig) (*committerImpl, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &committerImpl{
		committerConfig: cfg,
		commitTaskBatch: make([]*commitTask, 0, cfg.commitTaskBatchSize),
	}
	if err := c.init(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *committerImpl) init() error {
	c.dispatcher.mu.Lock()
	defer c.dispatcher.mu.Unlock()
	if c.dispatcher.runner != nil {
		return nil
	}

	commitTaskQ, err := newCommitTaskQueue(c.commitTaskQueueSize)
	if err != nil {
		return err
	}
	c.commitTaskQ = commitTaskQ

	c.inflightCommitTasks.cv = sync.NewCond(&c.inflightCommitTasks.mu)

	commitWaitQ, err := newCommitWaitQueue()
	if err != nil {
		return err
	}
	c.commitWaitQ = commitWaitQ

	r := runner.New("committer", nil)
	cancel, err := r.Run(c.commitLoop)
	if err != nil {
		return err
	}
	c.dispatcher.runner = r
	c.dispatcher.cancel = cancel
	c.running.val = true

	return nil
}

func (c *committerImpl) sendCommitWaitTask(_ context.Context, cwt *commitWaitTask) error {
	if cwt == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}

	c.running.mu.RLock()
	defer c.running.mu.RUnlock()
	if !c.running.val {
		return errors.WithStack(verrors.ErrClosed)
	}

	if err := c.state.mutableWithBarrier(); err != nil {
		return err
	}
	defer c.state.releaseBarrier()

	atomic.AddInt64(&c.inflightCommitWaitTasks, 1)
	return c.commitWaitQ.push(cwt)
}

func (c *committerImpl) sendCommitTask(ctx context.Context, ct *commitTask) error {
	if ct == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}

	c.running.mu.RLock()
	defer c.running.mu.RUnlock()
	if !c.running.val {
		return errors.WithStack(verrors.ErrClosed)
	}

	if err := c.state.committableWithBarrier(); err != nil {
		return err
	}
	defer c.state.releaseBarrier()

	atomic.AddInt64(&c.inflightCommitTasks.cnt, 1)
	return c.commitTaskQ.pushWithContext(ctx, ct)
}

func (c *committerImpl) commitLoop(ctx context.Context) {
	for ctx.Err() == nil {
		c.resetBatch()

		if err := c.commitLoopInternal(ctx); err != nil {
			c.state.setSealing()
		}

		c.inflightCommitTasks.cv.L.Lock()
		c.inflightCommitTasks.cv.Signal()
		c.inflightCommitTasks.cv.L.Unlock()
	}
}

func (c *committerImpl) commitLoopInternal(ctx context.Context) error {
	numPoppedCTs, err := c.ready(ctx)
	defer func() {
		atomic.AddInt64(&c.inflightCommitTasks.cnt, -numPoppedCTs)
	}()
	if err != nil {
		// sealing
		return err
	}

	if err := c.commit(ctx); err != nil {
		// sealing
		return err
	}
	/*
		numPoppedCWTs, err := c.commit(ctx)
		defer func() {
			atomic.AddInt64(&c.inflightCommitWaitTasks, -numPoppedCWTs)
		}()
		if err != nil {
			// sealing
			return err
		}
	*/
	return nil
}

func (c *committerImpl) ready(ctx context.Context) (int64, error) {
	numPopped := int64(0)

	ct, err := c.commitTaskQ.popWithContext(ctx)
	if err != nil {
		return numPopped, err
	}
	numPopped++

	globalHighWatermark, _ := c.lsc.reportCommitBase()
	if ct.stale(globalHighWatermark) {
		ct.release()
	} else {
		c.commitTaskBatch = append(c.commitTaskBatch, ct)
	}

	popSize := mathutil.MinInt(c.commitTaskBatchSize-len(c.commitTaskBatch), c.commitTaskQ.size())
	for i := 0; i < popSize; i++ {
		ct := c.commitTaskQ.pop()
		numPopped++

		if ct.stale(globalHighWatermark) {
			ct.release()
			continue
		}
		c.commitTaskBatch = append(c.commitTaskBatch, ct)
	}
	return numPopped, nil
}

func (c *committerImpl) commit(ctx context.Context) error {
	// numPoppedCWTs := int64(0)

	// NOTE: Sort is not needed, and it needs to be evaluated whether it is helpful or not.
	// - How many commit messages are processed at one time?
	// - What is the proper batch size?
	// - Maybe incoming commit messages are already sorted or partially sorted, is it helpful
	// sorting again here?
	sort.Slice(c.commitTaskBatch, func(i, j int) bool {
		return c.commitTaskBatch[i].highWatermark < c.commitTaskBatch[j].highWatermark
	})

	for _, ct := range c.commitTaskBatch {
		globalHighWatermark, _ := c.lsc.reportCommitBase()
		if ct.stale(globalHighWatermark) {
			continue
		}

		if err := c.commitInternal(ctx, ct); err != nil {
			return err
		}
		/*
			numPopped, err := c.commitInternal(ctx, ct)
			numPoppedCWTs += numPopped
			if err != nil {
				return numPoppedCWTs, err
			}
		*/
	}
	return nil
}

func (c *committerImpl) commitInternal(_ context.Context, ct *commitTask) error {
	_, uncommittedLLSNBegin := c.lsc.reportCommitBase()
	if uncommittedLLSNBegin != ct.committedLLSNBegin {
		// skip this commit
		// See #VARLOG-453 (VARLOG-453).
		return nil
	}

	uncommittedLLSNEnd := c.lsc.uncommittedLLSNEnd.Load()
	numUncommitted := int(uncommittedLLSNEnd - uncommittedLLSNBegin)
	numCommits := int(ct.committedGLSNEnd - ct.committedGLSNBegin)

	// When the LSE is in sealing, the number of uncommitted logs can be less than the number of
	// committed logs.
	if numUncommitted < numCommits {
		// like c.commitQ.size() < numCommits
		// skip this commit
		// NB: recovering phase?
		// MR just sends past commit messages to recovered SN that has no written logs
		return nil
	}

	// NOTE: It seems to be similar to the above condition. The actual purpose of this
	// condition is to avoid an invalid commit situation that the number of commitWaitTasks is
	// less than numCommits.
	// `numUncommitted` might be greater than or equal to the `numCommits`, but the number of
	// commitWaitTasks can be not. Since uncommittedLLSNEnd of the log stream context is
	// increased whenever each log entry is written to the storage, it doesn't represent the
	// size of commitWaitQueue. For instance, a batch of log entries could be written to the
	// storage, however, it is failed to push them into the commitWaitQueue.
	// See [#VARLOG-444](VARLOG-444).
	if c.commitWaitQ.size() < numCommits {
		return nil
	}

	commitContext := storage.CommitContext{
		HighWatermark:      ct.highWatermark,
		PrevHighWatermark:  ct.prevHighWatermark,
		CommittedGLSNBegin: ct.committedGLSNBegin,
		CommittedGLSNEnd:   ct.committedGLSNEnd,
		CommittedLLSNBegin: uncommittedLLSNBegin,
	}

	return c.commitDirectly(commitContext, true)

	/*
		batch, err := c.strg.NewCommitBatch(commitContext)
		if err != nil {
			return 0, err
		}
		defer func() {
			_ = batch.Close()
		}()

		iter := c.commitWaitQ.peekIterator()
		for i := 0; i < numCommits; i++ {
			glsn := ct.committedGLSNBegin + types.GLSN(i)
			cwt := iter.task()
			if uncommittedLLSNBegin+types.LLSN(i) != cwt.llsn {
				return 0, errors.New("llsn mismatch")
			}
			if cwt.twg != nil {
				cwt.twg.glsn = glsn
			}
			if err := batch.Put(cwt.llsn, glsn); err != nil {
				return 0, err
			}
			iter.next()
		}
		if err := batch.Apply(); err != nil {
			return 0, err
		}

		// NOTE: Popping committed tasks should be happened before assigning a new
		// localHighWatermark.
		//
		// Seal RPC decides whether the LSE can be sealed or not by using the localHighWatermark.
		// If Seal RPC tries to make the LSE sealed, the committer should not pop anything from
		// commitWaitQ.
		committedTasks := make([]*commitWaitTask, 0, numCommits)
		for i := 0; i < numCommits; i++ {
			tb := c.commitWaitQ.pop()
			// NOTE: This tb should not be nil, because the size of commitWaitQ is inspected
			// above.
			committedTasks = append(committedTasks, tb)
		}

		// only the first commit changes local low watermark
		c.lsc.localGLSN.localLowWatermark.CompareAndSwap(types.InvalidGLSN, ct.committedGLSNBegin)
		c.lsc.localGLSN.localHighWatermark.Store(ct.committedGLSNEnd - 1)
		uncommittedLLSNBegin += types.LLSN(numCommits)
		c.decider.change(func() {
			c.lsc.storeReportCommitBase(ct.highWatermark, uncommittedLLSNBegin)
		})

		// NOTE: Notifying the completion of append should be happened after assigning a new
		// localHighWatermark.
		//
		// A client that receives the response of Append RPC should be able to read that log
		// immediately. If updating localHighWatermark occurs later, the client can receive
		// ErrUndecidable if ErrUndecidable is allowed.
		for _, cwt := range committedTasks {
			cwt.twg.done(nil)
			cwt.release()
		}

		return int64(numCommits), nil
	*/
}

func (c *committerImpl) stop() {
	c.running.mu.Lock()
	c.running.val = false
	c.running.mu.Unlock()

	c.dispatcher.cancel()
	c.dispatcher.runner.Stop()

	c.drainCommitTaskQ()
	c.drainCommitWaitQ(errors.WithStack(verrors.ErrClosed))
	c.resetBatch()
}

func (c *committerImpl) drainCommitWaitQ(err error) {
	for atomic.LoadInt64(&c.inflightCommitWaitTasks) > 0 {
		numPopped := int64(0)
		for c.commitWaitQ.size() > 0 {
			cwt := c.commitWaitQ.pop()
			numPopped++
			cwt.twg.done(err)
			cwt.release()
		}
		atomic.AddInt64(&c.inflightCommitWaitTasks, -numPopped)
	}
}

func (c *committerImpl) drainCommitTaskQ() {
	numPopped := int64(0)
	for c.commitTaskQ.size() > 0 {
		ctb := c.commitTaskQ.pop()
		numPopped++
		ctb.release()
	}
	atomic.AddInt64(&c.inflightCommitTasks.cnt, -numPopped)
}

func (c *committerImpl) waitForDrainageOfCommitTasks(ctx context.Context) error {
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
			c.inflightCommitTasks.cv.L.Lock()
			c.inflightCommitTasks.cv.Signal()
			c.inflightCommitTasks.cv.L.Unlock()
		case <-done:
		}
	}()

	c.inflightCommitTasks.cv.L.Lock()
	defer c.inflightCommitTasks.cv.L.Unlock()
	for atomic.LoadInt64(&c.inflightCommitTasks.cnt) > 0 && ctx.Err() == nil {
		c.inflightCommitTasks.cv.Wait()
	}
	if c.commitTaskQ.size() == 0 {
		return nil
	}
	return ctx.Err()
}

func (c *committerImpl) resetBatch() {
	for _, ctb := range c.commitTaskBatch {
		ctb.release()
	}
	c.commitTaskBatch = c.commitTaskBatch[0:0]
}

func (c *committerImpl) commitDirectly(commitContext storage.CommitContext, requireCommitWaitTasks bool) error {
	_, uncommittedLLSNBegin := c.lsc.reportCommitBase()
	numCommits := int(commitContext.CommittedGLSNEnd - commitContext.CommittedGLSNBegin)

	// NOTE: It seems to be similar to the above condition. The actual purpose of this
	// condition is to avoid an invalid commit situation that the number of commitWaitTasks is
	// less than numCommits.
	// `numUncommitted` might be greater than or equal to the `numCommits`, but the number of
	// commitWaitTasks can be not. Since uncommittedLLSNEnd of the log stream context is
	// increased whenever each log entry is written to the storage, it doesn't represent the
	// size of commitWaitQueue. For instance, a batch of log entries could be written to the
	// storage, however, it is failed to push them into the commitWaitQueue.
	// See [#VARLOG-444](VARLOG-444).
	if requireCommitWaitTasks && c.commitWaitQ.size() < numCommits {
		return nil
	}

	batch, err := c.strg.NewCommitBatch(commitContext)
	if err != nil {
		return err
	}
	defer func() {
		_ = batch.Close()
	}()

	iter := c.commitWaitQ.peekIterator()
	for i := 0; i < numCommits; i++ {
		llsn := commitContext.CommittedLLSNBegin + types.LLSN(i)
		glsn := commitContext.CommittedGLSNBegin + types.GLSN(i)

		if uncommittedLLSNBegin+types.LLSN(i) != llsn {
			return errors.New("llsn mismatch")
		}

		// If requireCommitWaitTasks is true, since the number of tasks in commitWaitQ is
		// inspected above, cwt must exist.
		// If cwt is null, it means that there is no task in commitWaitQ anymore. When this
		// method is executed by SyncReplicate, it is okay for cwt not to exist.
		cwt := iter.task()
		if cwt != nil && cwt.twg != nil {
			cwt.twg.glsn = glsn
		}

		if err := batch.Put(llsn, glsn); err != nil {
			return err
		}
		iter.next()
	}
	if err := batch.Apply(); err != nil {
		return err
	}

	// NOTE: Popping committed tasks should be happened before assigning a new
	// localHighWatermark.
	//
	// Seal RPC decides whether the LSE can be sealed or not by using the localHighWatermark.
	// If Seal RPC tries to make the LSE sealed, the committer should not pop anything from
	// commitWaitQ.

	popSize := mathutil.MinInt(numCommits, c.commitWaitQ.size())
	committedTasks := make([]*commitWaitTask, 0, popSize)
	for i := 0; i < popSize; i++ {
		// NOTE: This cwt should not be nil, because the size of commitWaitQ is inspected
		// above.
		cwt := c.commitWaitQ.pop()
		committedTasks = append(committedTasks, cwt)
	}

	// NOTE: localGLSN should be increased only when numCommits is greater than zero.
	if numCommits > 0 {
		// only the first commit changes local low watermark
		c.lsc.localGLSN.localLowWatermark.CompareAndSwap(types.InvalidGLSN, commitContext.CommittedGLSNBegin)
		c.lsc.localGLSN.localHighWatermark.Store(commitContext.CommittedGLSNEnd - 1)
	}
	uncommittedLLSNBegin += types.LLSN(numCommits)
	c.decider.change(func() {
		c.lsc.storeReportCommitBase(commitContext.HighWatermark, uncommittedLLSNBegin)
	})

	// NOTE: Notifying the completion of append should be happened after assigning a new
	// localHighWatermark.
	//
	// A client that receives the response of Append RPC should be able to read that log
	// immediately. If updating localHighWatermark occurs later, the client can receive
	// ErrUndecidable if ErrUndecidable is allowed.
	for _, cwt := range committedTasks {
		cwt.twg.done(nil)
		cwt.release()
	}

	atomic.AddInt64(&c.inflightCommitWaitTasks, -int64(popSize))

	return nil
}
