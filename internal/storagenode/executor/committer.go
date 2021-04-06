package executor

//go:generate mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/executor -package executor -source committer.go -destination committer_mock.go -mock_names committer=MockCommitter

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/mathutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/timeutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
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
	drainCommitQ(err error)
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

	stat struct {
		ctPassDur time.Duration
		ctPassMin time.Duration
		ctPassMax time.Duration
		ctPassCnt int
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

	c.stat.ctPassMin = timeutil.MaxDuration
	c.stat.ctPassMax = time.Duration(0)

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

		if err := c.ready(ctx); err != nil {
			// sealing
			c.state.setSealing()
			continue
		}
		if err := c.commit(ctx); err != nil {
			// sealing
			c.state.setSealing()
			continue
		}
	}
}

func (c *committerImpl) ready(ctx context.Context) error {
	ct, err := c.commitTaskQ.popWithContext(ctx)
	if err != nil {
		return err
	}
	c.commitTaskBatch = append(c.commitTaskBatch, ct)

	// -- debug / stat
	d := time.Since(ct.ctime)
	c.stat.ctPassDur += d
	if d < c.stat.ctPassMin {
		c.stat.ctPassMin = d
	}
	if d > c.stat.ctPassMax {
		c.stat.ctPassMax = d
	}
	c.stat.ctPassCnt++
	// -- debug / stat

	globalHighWatermark, _ := c.lsc.reportCommitBase()

	popSize := mathutil.MinInt(c.commitTaskBatchSize-1, c.commitTaskQ.size())
	for i := 0; i < popSize; i++ {
		ct := c.commitTaskQ.pop()
		if ct.highWatermark <= globalHighWatermark {
			ct.release()
			continue
		}

		// -- debug / stat
		d := time.Since(ct.ctime)
		c.stat.ctPassDur += d
		if d < c.stat.ctPassMin {
			c.stat.ctPassMin = d
		}
		if d > c.stat.ctPassMax {
			c.stat.ctPassMax = d
		}
		c.stat.ctPassCnt++
		// -- debug / stat

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

func (c *committerImpl) commitInternal(_ context.Context, ct *commitTask) (err error) {
	//defer ct.release()

	globalHighWatermark, uncommittedLLSNBegin := c.lsc.reportCommitBase()
	if globalHighWatermark != ct.prevHighWatermark {
		// skip this commit
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
		if err = batch.Put(cwt.llsn, cwt.glsn); err != nil {
			return err
		}
		iter.next()
	}
	if err := batch.Apply(); err != nil {
		return err
	}

	// only the first commit changes local low watermark
	c.lsc.localGLSN.localLowWatermark.CompareAndSwap(types.InvalidGLSN, ct.committedGLSNBegin)
	c.lsc.localGLSN.localHighWatermark.Store(ct.committedGLSNEnd - 1)
	uncommittedLLSNBegin += types.LLSN(numCommits)
	c.decider.change(func() {
		c.lsc.storeReportCommitBase(ct.highWatermark, uncommittedLLSNBegin)
	})

	for i := 0; i < numCommits; i++ {
		tb := c.commitWaitQ.pop()
		tb.wg.Done()
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
	c.drainCommitQ(errors.WithStack(verrors.ErrClosed))
	c.resetBatch()

	if c.stat.ctPassCnt > 0 {
		log.Printf("ctPass: mean=%v min=%+v max=%+v",
			c.stat.ctPassDur/time.Duration(c.stat.ctPassCnt),
			c.stat.ctPassMin,
			c.stat.ctPassMax,
		)
	}
}

func (c *committerImpl) drainCommitQ(err error) {
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
