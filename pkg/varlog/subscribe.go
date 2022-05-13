package varlog

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/logclient"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

type SubscribeCloser func()

func (v *logImpl) subscribe(ctx context.Context, topicID types.TopicID, begin, end types.GLSN, onNext OnNext, opts ...SubscribeOption) (closer SubscribeCloser, err error) {
	if begin >= end {
		return nil, verrors.ErrInvalid
	}

	subscribeOpts := defaultSubscribeOptions()
	for _, opt := range opts {
		opt.apply(&subscribeOpts)
	}

	subscribeRunner := runner.New("subscribe", v.logger.Named("subscribe").With(
		zap.Int32("tpid", int32(topicID)),
		zap.Uint64("begin", uint64(begin)),
		zap.Uint64("end", uint64(end)),
	))

	transmitCV := make(chan struct{}, 1)

	mctx, cancel := subscribeRunner.WithManagedCancel(context.Background())
	closer = func() {
		cancel()
		subscribeRunner.Stop()
	}

	sleq := newSubscribedLogEntiresQueue(begin, end, closer, v.logger)

	tlogger := v.logger.Named("transmitter")
	tsm := &transmitter{
		topicID:           topicID,
		subscribers:       make(map[types.LogStreamID]*subscriber),
		refresher:         v.refresher,
		replicasRetriever: v.replicasRetriever,
		logCLManager:      v.logCLManager,
		sleq:              sleq,
		wanted:            begin,
		end:               end,
		transmitQ:         &transmitQueue{pq: &PriorityQueue{}},
		transmitCV:        transmitCV,
		timeout:           subscribeOpts.timeout,
		runner:            runner.New("transmitter", tlogger),
		logger:            tlogger,
	}

	dis := &dispatcher{
		onNextFunc: onNext,
		sleq:       sleq,
		logger:     v.logger,
	}
	if err = subscribeRunner.RunC(mctx, tsm.transmit); err != nil {
		goto errOut
	}
	if err = subscribeRunner.RunC(mctx, dis.dispatch); err != nil {
		goto errOut
	}

	return closer, nil

errOut:
	closer()
	return nil, err
}

type PriorityQueueItem interface {
	Priority() uint64
}

type PriorityQueue []PriorityQueueItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority() < pq[j].Priority()
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(PriorityQueueItem)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type transmitResult struct {
	logStreamID   types.LogStreamID
	storageNodeID types.StorageNodeID
	result        logclient.SubscribeResult
}

func (t transmitResult) Priority() uint64 {
	return uint64(t.result.GLSN)
}

type transmitQueue struct {
	pq *PriorityQueue
	mu sync.Mutex
}

func (tq *transmitQueue) Push(r transmitResult) {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	heap.Push(tq.pq, r)
}

func (tq *transmitQueue) Pop() (transmitResult, bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if tq.pq.Len() == 0 {
		return transmitResult{
			result: logclient.InvalidSubscribeResult,
		}, false
	}

	return heap.Pop(tq.pq).(transmitResult), true
}

func (tq *transmitQueue) Front() (transmitResult, bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if tq.pq.Len() == 0 {
		return transmitResult{
			result: logclient.InvalidSubscribeResult,
		}, false
	}

	return (*tq.pq)[0].(transmitResult), true
}

type subscriber struct {
	topicID         types.TopicID
	logStreamID     types.LogStreamID
	storageNodeID   types.StorageNodeID
	logCL           *logclient.Client
	resultC         <-chan logclient.SubscribeResult
	cancelSubscribe context.CancelFunc

	transmitQ  *transmitQueue
	transmitCV chan struct{}

	done     chan struct{}
	closed   atomicutil.AtomicBool
	complete atomicutil.AtomicBool

	lastSubscribeAt atomic.Value

	logger *zap.Logger
}

func newSubscriber(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, storageNodeID types.StorageNodeID, logCL *logclient.Client, begin, end types.GLSN, transmitQ *transmitQueue, transmitCV chan struct{}, logger *zap.Logger) (*subscriber, error) {
	ctx, cancel := context.WithCancel(ctx)
	resultC, err := logCL.Subscribe(ctx, topicID, logStreamID, begin, end)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &subscriber{
		topicID:         topicID,
		logStreamID:     logStreamID,
		storageNodeID:   storageNodeID,
		logCL:           logCL,
		resultC:         resultC,
		cancelSubscribe: cancel,
		transmitQ:       transmitQ,
		transmitCV:      transmitCV,
		done:            make(chan struct{}),
		logger:          logger.Named("subscriber").With(zap.Int32("lsid", int32(logStreamID))),
	}
	s.lastSubscribeAt.Store(time.Now())
	s.closed.Store(false)
	s.complete.Store(false)
	return s, nil
}

func (s *subscriber) stop() {
	s.cancelSubscribe()
	close(s.done)
	s.closed.Store(true)
}

func (s *subscriber) subscribe(ctx context.Context) {
	defer func() {
		// s.logCL.Close()
		s.closed.Store(true)
	}()
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case res, ok := <-s.resultC:
			r := transmitResult{
				storageNodeID: s.storageNodeID,
				logStreamID:   s.logStreamID,
			}

			if ok {
				r.result = res
			} else {
				r.result = logclient.InvalidSubscribeResult
			}

			needExit := r.result.Error != nil

			if res.GLSN != types.InvalidGLSN {
				s.lastSubscribeAt.Store(time.Now())
			} else if res.Error == io.EOF || errors.Is(res.Error, verrors.ErrTrimmed) {
				s.complete.Store(true)
			}

			s.transmitQ.Push(r)
			select {
			case s.transmitCV <- struct{}{}:
			default:
			}

			if needExit {
				return
			}
		}
	}
}

func (s *subscriber) getLastSubscribeAt() time.Time {
	return s.lastSubscribeAt.Load().(time.Time)
}

type transmitter struct {
	topicID           types.TopicID
	subscribers       map[types.LogStreamID]*subscriber
	refresher         MetadataRefresher
	replicasRetriever ReplicasRetriever
	sleq              *subscribedLogEntriesQueue
	wanted            types.GLSN
	end               types.GLSN

	transmitQ  *transmitQueue
	transmitCV chan struct{}

	timeout time.Duration
	timer   *time.Timer

	logCLManager *logclient.Manager[*logclient.Client]
	runner       *runner.Runner
	logger       *zap.Logger
}

func (p *transmitter) transmit(ctx context.Context) {
	defer func() {
		p.sleq.close()
		p.runner.Stop()
	}()

	p.timer = time.NewTimer(p.timeout)
	defer p.timer.Stop()

	for {
		select {
		case <-ctx.Done():
			res := logclient.InvalidSubscribeResult
			res.Error = ctx.Err()
			p.logger.Debug("transmit error result", zap.Reflect("res", res))
			p.sleq.pushBack(res)
			return
		case <-p.transmitCV:
			if repeat := p.transmitLoop(ctx); !repeat {
				return
			}
		case <-p.timer.C:
			p.handleTimeout(ctx)
			p.timer.Reset(p.timeout)
		}
	}
}

func (p *transmitter) refreshSubscriber(ctx context.Context) error {
	p.refresher.Refresh(ctx)

	replicasMap := p.replicasRetriever.All(p.topicID)
	for logStreamID, replicas := range replicasMap {
		idx := 0
		if s, ok := p.subscribers[logStreamID]; ok {
			if !s.closed.Load() || s.complete.Load() {
				continue
			}
		SelectReplica:
			for i, r := range replicas {
				if r.StorageNodeID == s.storageNodeID {
					idx = (i + 1) % len(replicas)
					break SelectReplica
				}
			}
		}

		var s *subscriber
		var err error
	CONNECT:
		for i := 0; i < len(replicas); i++ {
			idx = (idx + i) % len(replicas)
			snid := replicas[idx].GetStorageNodeID()
			addr := replicas[idx].GetAddress()

			logCL, err := p.logCLManager.GetOrConnect(ctx, snid, addr)
			if err != nil {
				continue CONNECT
			}

			s, err = newSubscriber(ctx, p.topicID, logStreamID, snid, logCL, p.wanted, p.end, p.transmitQ, p.transmitCV, p.logger)
			if err != nil {
				// logCL.Close()
				continue CONNECT
			}

			break CONNECT
		}

		if s != nil {
			p.subscribers[logStreamID] = s

			if err = p.runner.RunC(ctx, s.subscribe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *transmitter) handleTimeout(ctx context.Context) {
	l := make([]*subscriber, 0, len(p.subscribers))
	for _, s := range p.subscribers {
		if !s.complete.Load() && !s.closed.Load() &&
			time.Since(s.getLastSubscribeAt()) >= p.timeout {
			l = append(l, s)
		}
	}

	if len(l) != len(p.subscribers) {
		for _, s := range l {
			s.stop()
		}
	}

	p.refreshSubscriber(ctx)
}

func (p *transmitter) handleError(r transmitResult) error {
	s, ok := p.subscribers[r.logStreamID]
	if !ok {
		return nil
	}

	if s.storageNodeID == r.storageNodeID && !s.closed.Load() {
		s.stop()
	}

	return r.result.Error
}

func (p *transmitter) handleResult(r transmitResult) error {
	var err error

	/* NOTE Ignore transmitResult with GLSN less than p.wanted.
	They can be delivered by subscribers that have not yet closed.
	*/
	if r.result.GLSN == types.InvalidGLSN {
		err = p.handleError(r)
		if errors.Is(err, verrors.ErrTrimmed) {
			p.sleq.pushBack(r.result)
		}
	} else if p.wanted == r.result.GLSN {
		p.sleq.pushBack(r.result)
		p.wanted++
		p.timer.Reset(p.timeout)
	}

	return err
}

func (p *transmitter) transmitLoop(ctx context.Context) bool {
	needRefresh := false

	for {
		res, ok := p.transmitQ.Front()
		if !ok {
			break
		}

		if res.result.GLSN <= p.wanted {
			res, _ := p.transmitQ.Pop()
			err := p.handleResult(res)
			if p.wanted == p.end ||
				errors.Is(err, verrors.ErrTrimmed) {
				return false
			}

			needRefresh = needRefresh || (err != nil && err != io.EOF)
		} else {
			break
		}
	}

	if needRefresh {
		p.refreshSubscriber(ctx)
	}

	return true
}

type subscribedLogEntriesQueue struct {
	c      chan logclient.SubscribeResult
	wanted types.GLSN
	end    types.GLSN
	closer SubscribeCloser
	logger *zap.Logger
}

func newSubscribedLogEntiresQueue(begin, end types.GLSN, closer SubscribeCloser, logger *zap.Logger) *subscribedLogEntriesQueue {
	q := &subscribedLogEntriesQueue{
		c:      make(chan logclient.SubscribeResult, end-begin),
		wanted: begin,
		end:    end,
		closer: closer,
		logger: logger.Named("subscribed_log_entries_queue"),
	}
	return q
}

func (q *subscribedLogEntriesQueue) pushBack(result logclient.SubscribeResult) {
	if !q.pushable(result) {
		q.logger.Panic("not pushable")
	}
	advance := result.Error == nil
	// NOTE: the sendC is not blocking since its size is enough to receive messages.
	q.sendC() <- result
	if advance {
		q.wanted++
	}
}

func (q *subscribedLogEntriesQueue) pushable(result logclient.SubscribeResult) bool {
	return result.LogEntry.GLSN == q.wanted || result.Error != nil
}

func (q *subscribedLogEntriesQueue) close() {
	close(q.c)
	// q.closer()
}

func (q *subscribedLogEntriesQueue) sendC() chan<- logclient.SubscribeResult {
	return q.c
}

func (q *subscribedLogEntriesQueue) recvC() <-chan logclient.SubscribeResult {
	return q.c
}

type dispatcher struct {
	onNextFunc OnNext
	sleq       *subscribedLogEntriesQueue
	logger     *zap.Logger
}

func (p *dispatcher) dispatch(_ context.Context) {
	sentErr := false
	for res := range p.sleq.recvC() {
		if sentErr {
			p.logger.Panic("multiple errors in dispatcher", zap.Any("res", res), zap.Error(res.Error))
		}
		p.onNextFunc(res.LogEntry, res.Error)
		sentErr = sentErr || res.Error != nil
	}
	if !sentErr {
		p.onNextFunc(varlogpb.InvalidLogEntry(), io.EOF)
	}
}

type Subscriber interface {
	Next() (varlogpb.LogEntry, error)
	io.Closer
}

type invalidSubscriber struct {
	err error
}

func (s invalidSubscriber) Next() (varlogpb.LogEntry, error) {
	return varlogpb.InvalidLogEntry(), s.err
}

func (s invalidSubscriber) Close() error {
	return nil
}

func (v *logImpl) subscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN, opts ...SubscribeOption) Subscriber {
	if begin >= end {
		return invalidSubscriber{err: verrors.ErrInvalid}
	}

	subscribeOpts := defaultSubscribeOptions()
	for _, opt := range opts {
		opt.apply(&subscribeOpts)
	}

	logStreamReplicas, ok := v.replicasRetriever.Retrieve(topicID, logStreamID)
	if !ok {
		return invalidSubscriber{err: errors.New("no such log stream")}
	}

	ctx, cancel := context.WithCancel(ctx)
	var (
		logCL   *logclient.Client
		resultC <-chan logclient.SubscribeResult
		err     error
	)
	for _, logStreamReplica := range logStreamReplicas {
		storageNodeID := logStreamReplica.StorageNodeID
		storageNodeAddr := logStreamReplica.Address
		var cerr error
		logCL, cerr = v.logCLManager.GetOrConnect(ctx, storageNodeID, storageNodeAddr)
		if cerr != nil {
			err = multierr.Append(err, cerr)
			// _ = logCL.Close()
			continue
		}

		resultC, cerr = logCL.SubscribeTo(ctx, topicID, logStreamID, begin, end)
		if cerr != nil {
			err = multierr.Append(err, cerr)
			// _ = logCL.Close()
			continue
		}

		err = nil
		break
	}
	if err != nil {
		cancel()
		return invalidSubscriber{err: err}
	}

	ch := make(chan struct{})
	return &logStreamSubscriber{
		ctx:     ctx,
		cancel:  cancel,
		closeC:  ch,
		closer:  func() { close(ch) },
		logCL:   logCL,
		resultC: resultC,
	}
}

type logStreamSubscriber struct {
	ctx    context.Context
	cancel context.CancelFunc

	closeC  <-chan struct{}
	logCL   *logclient.Client
	resultC <-chan logclient.SubscribeResult

	mu      sync.Mutex
	closer  func()
	err     error
	errOnce sync.Once
}

func (s *logStreamSubscriber) Next() (logEntry varlogpb.LogEntry, err error) {
	s.mu.Lock()
	err = s.err
	s.mu.Unlock()
	if err != nil {
		return
	}

	select {
	case <-s.ctx.Done():
		err = s.ctx.Err()
	case <-s.closeC:
		err = verrors.ErrClosed
	case sr, ok := <-s.resultC:
		if ok {
			logEntry, err = sr.LogEntry, sr.Error
		} else {
			err = errors.New("already stopped SubscribeTo RPC")
		}
	}
	if err != nil {
		s.mu.Lock()
		s.setErr(err)
		s.mu.Unlock()
	}
	return
}

func (s *logStreamSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closer == nil {
		return nil
	}
	s.setErr(verrors.ErrClosed)
	s.closer()
	s.closer = nil
	s.cancel()

	return nil
}

func (s *logStreamSubscriber) setErr(err error) {
	s.errOnce.Do(func() {
		s.err = err
	})
}
