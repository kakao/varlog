package executor

//go:generate mockgen -self_package github.com/kakao/varlog/internal/storagenode/executor -package executor -source committer.go -destination committer_mock.go -mock_names committer=MockCommitter

import (
	"context"
	"sort"
	"sync"

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
	sendCommitWaitTask(ctx context.Context, tb *appendTask) error
	sendCommitTask(ctx context.Context, ctb *commitTask) error
	drainCommitWaitQ(err error)
	stop()
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

func (c *committerImpl) sendCommitWaitTask(_ context.Context, tb *appendTask) error {
	if tb == nil {
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

	return c.commitWaitQ.push(tb)
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

	return c.commitTaskQ.pushWithContext(ctx, ct)
}

func (c *committerImpl) commitLoop(ctx context.Context) {
	for ctx.Err() == nil {
		c.resetBatch()

		if err := c.commitLoopInternal(ctx); err != nil {
			c.state.setSealing()
		}
	}
}

func (c *committerImpl) commitLoopInternal(ctx context.Context) error {
	if err := c.ready(ctx); err != nil {
		// sealing
		return err
	}
	if err := c.commit(ctx); err != nil {
		// sealing
		return err
	}
	return nil
}

func (c *committerImpl) ready(ctx context.Context) error {
	ct, err := c.commitTaskQ.popWithContext(ctx)
	if err != nil {
		return err
	}
	c.commitTaskBatch = append(c.commitTaskBatch, ct)

	globalHighWatermark, _ := c.lsc.reportCommitBase()

	popSize := mathutil.MinInt(c.commitTaskBatchSize-1, c.commitTaskQ.size())
	for i := 0; i < popSize; i++ {
		ct := c.commitTaskQ.pop()
		if ct.highWatermark <= globalHighWatermark {
			ct.release()
			continue
		}

		c.commitTaskBatch = append(c.commitTaskBatch, ct)
	}
	return nil
}

func (c *committerImpl) commit(ctx context.Context) error {
	// NOTE: Sort is not needed, and it needs to be evaluated whether it is helpful or not.
	// - How many commit messages are processed at one time?
	// - What is the proper batch size?
	// - Maybe incoming commit messages are already sorted or partially sorted, is it helpful
	// sorting again here?
	sort.Slice(c.commitTaskBatch, func(i, j int) bool {
		return c.commitTaskBatch[i].highWatermark < c.commitTaskBatch[j].highWatermark
	})
	for _, ct := range c.commitTaskBatch {
		if err := c.commitInternal(ctx, ct); err != nil {
			return err
		}
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

	// NOTE: It is equal to the above condition.
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

	batch, err := c.strg.NewCommitBatch(commitContext)
	if err != nil {
		return err
	}
	defer func() {
		_ = batch.Close()
	}()

	// TODO: LLSN check
	iter := c.commitWaitQ.peekIterator()
	for i := 0; i < numCommits; i++ {
		glsn := ct.committedGLSNBegin + types.GLSN(i)
		cwt := iter.task()
		if uncommittedLLSNBegin+types.LLSN(i) != cwt.llsn {
			return errors.New("llsn mismatch")
		}
		cwt.glsn = glsn
		if err := batch.Put(cwt.llsn, cwt.glsn); err != nil {
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
	committedTasks := make([]*appendTask, 0, numCommits)
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
	for _, ct := range committedTasks {
		ct.wg.Done()
	}

	return nil
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
	for c.commitWaitQ.size() > 0 {
		tb := c.commitWaitQ.pop()
		tb.err = err
		tb.wg.Done()
	}
}

func (c *committerImpl) drainCommitTaskQ() {
	for c.commitTaskQ.size() > 0 {
		ctb := c.commitTaskQ.pop()
		ctb.release()
	}
}

func (c *committerImpl) resetBatch() {
	for _, ctb := range c.commitTaskBatch {
		ctb.release()
	}
	c.commitTaskBatch = c.commitTaskBatch[0:0]
}
