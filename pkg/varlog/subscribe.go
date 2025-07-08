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

	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
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

	aggregationCV := make(chan struct{}, 1)

	mctx, cancel := subscribeRunner.WithManagedCancel(context.Background())
	closer = func() {
		cancel()
		subscribeRunner.Stop()
	}

	dispatchQueue := newDispatchQueue(begin, end, v.logger)

	aggregatorLogger := v.logger.Named("aggregator")
	// The maximum length of aggregation buffer is end-begin in the worst case.
	// Therefore, we can approximate half of it.
	// TODO: Use a better approximation.
	aggregationBufferSize := int((end - begin) / 2)
	tsm := &aggregator{
		topicID:           topicID,
		subscribers:       make(map[types.LogStreamID]*subscriber),
		replicasRetriever: v.replicasRetriever,
		logCLManager:      v.logCLManager,
		dispatchQueue:     dispatchQueue,
		wanted:            begin,
		end:               end,
		buffer:            &aggregationBuffer{pq: newPriorityQueue(aggregationBufferSize)},
		aggregationCV:     aggregationCV,
		timeout:           subscribeOpts.timeout,
		runner:            runner.New("aggregator", aggregatorLogger),
		logger:            aggregatorLogger,
	}

	dis := &dispatcher{
		onNextFunc: onNext,
		queue:      dispatchQueue,
		observer:   subscribeOpts.observer,
		logger:     v.logger,
	}
	if err = subscribeRunner.RunC(mctx, tsm.run); err != nil {
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

func newPriorityQueue(size int) *PriorityQueue {
	items := make([]PriorityQueueItem, 0, size)
	pq := PriorityQueue(items)
	return &pq
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority() < pq[j].Priority()
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(PriorityQueueItem)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// aggregationItem is an internal struct that wraps a log entry received from a
// subscriber, along with the necessary metadata for processing and statistics
// collection. It is the unit of work that flows through the aggregation and
// dispatch pipeline.
type aggregationItem struct {
	logStreamID   types.LogStreamID
	storageNodeID types.StorageNodeID
	result        client.SubscribeResult

	stats       SubscribeStats
	enqueueTime time.Time
}

func (t aggregationItem) Priority() uint64 {
	return uint64(t.result.GLSN)
}

// aggregationBuffer is a thread-safe priority queue that stores
// aggregationItems. It is used to reorder log entries fetched from different
// log streams into a single, sorted stream based on their GLSNs.
type aggregationBuffer struct {
	pq *PriorityQueue
	mu sync.Mutex
}

func (ab *aggregationBuffer) Push(r aggregationItem) {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	r.enqueueTime = time.Now()
	heap.Push(ab.pq, r)
	r.stats.AggregationEnqueueDuration = time.Since(r.enqueueTime)
}

func (ab *aggregationBuffer) Pop() (aggregationItem, bool) {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	if ab.pq.Len() == 0 {
		return aggregationItem{
			result: client.InvalidSubscribeResult,
		}, false
	}

	r := heap.Pop(ab.pq).(aggregationItem)
	r.stats.AggregationBufferWait = time.Since(r.enqueueTime) - r.stats.AggregationEnqueueDuration
	return r, true
}

func (ab *aggregationBuffer) Front() (aggregationItem, bool) {
	ab.mu.Lock()
	defer ab.mu.Unlock()

	if ab.pq.Len() == 0 {
		return aggregationItem{
			result: client.InvalidSubscribeResult,
		}, false
	}

	return (*ab.pq)[0].(aggregationItem), true
}

type subscriber struct {
	topicID         types.TopicID
	logStreamID     types.LogStreamID
	storageNodeID   types.StorageNodeID
	logCL           *client.LogClient
	resultC         <-chan client.SubscribeResult
	cancelSubscribe context.CancelFunc

	aggregationBuffer *aggregationBuffer
	aggregationCV     chan<- struct{}

	done     chan struct{}
	closed   atomic.Bool
	complete atomic.Bool

	lastSubscribeAt atomic.Int64

	logger *zap.Logger
}

func newSubscriber(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, storageNodeID types.StorageNodeID, logCL *client.LogClient, begin, end types.GLSN, aggregationBuffer *aggregationBuffer, aggregationCV chan<- struct{}, logger *zap.Logger) (*subscriber, error) {
	ctx, cancel := context.WithCancel(ctx)
	resultC, err := logCL.Subscribe(ctx, topicID, logStreamID, begin, end)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &subscriber{
		topicID:           topicID,
		logStreamID:       logStreamID,
		storageNodeID:     storageNodeID,
		logCL:             logCL,
		resultC:           resultC,
		cancelSubscribe:   cancel,
		aggregationBuffer: aggregationBuffer,
		aggregationCV:     aggregationCV,
		done:              make(chan struct{}),
		logger:            logger.Named("subscriber").With(zap.Int32("lsid", int32(logStreamID))),
	}
	s.lastSubscribeAt.Store(time.Now().UnixNano())
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
			r := aggregationItem{
				storageNodeID: s.storageNodeID,
				logStreamID:   s.logStreamID,
			}

			if ok {
				r.result = res
			} else {
				r.result = client.InvalidSubscribeResult
			}

			needExit := r.result.Error != nil

			if res.GLSN != types.InvalidGLSN {
				s.lastSubscribeAt.Store(time.Now().UnixNano())
			} else if res.Error == io.EOF || errors.Is(res.Error, verrors.ErrTrimmed) {
				s.complete.Store(true)
			}

			s.aggregationBuffer.Push(r)
			select {
			case s.aggregationCV <- struct{}{}:
			default:
			}

			if needExit {
				return
			}
		}
	}
}

func (s *subscriber) getLastSubscribeAt() time.Time {
	nsec := s.lastSubscribeAt.Load()
	return time.Unix(0, nsec)
}

// aggregator fetches log entries from multiple log streams through
// subscribers. It uses an internal priority queue (aggregationBuffer) to
// reorder the log entries into a globally sorted stream by GLSN. The ordered
// log entries are then passed to the dispatcher.
type aggregator struct {
	topicID           types.TopicID
	subscribers       map[types.LogStreamID]*subscriber
	replicasRetriever ReplicasRetriever
	dispatchQueue     *dispatchQueue
	wanted            types.GLSN
	end               types.GLSN

	buffer        *aggregationBuffer
	aggregationCV chan struct{}

	timeout time.Duration
	timer   *time.Timer

	logCLManager *client.Manager[*client.LogClient]
	runner       *runner.Runner
	logger       *zap.Logger
}

func (a *aggregator) run(ctx context.Context) {
	defer func() {
		a.dispatchQueue.close()
		a.runner.Stop()
	}()

	a.timer = time.NewTimer(a.timeout)
	defer a.timer.Stop()

	_ = a.refreshSubscribers(ctx)

	for {
		select {
		case <-ctx.Done():
			var item aggregationItem
			item.result = client.InvalidSubscribeResult
			item.result.Error = ctx.Err()
			a.dispatchQueue.pushBack(item)
			return
		case <-a.aggregationCV:
			if repeat := a.processBuffer(ctx); !repeat {
				return
			}
		case <-a.timer.C:
			a.handleTimeout(ctx)
			a.timer.Reset(a.timeout)
		}
	}
}

func (a *aggregator) refreshSubscribers(ctx context.Context) error {
	replicasMap := a.replicasRetriever.All(a.topicID)
	for logStreamID, replicas := range replicasMap {
		idx := 0
		if s, ok := a.subscribers[logStreamID]; ok {
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

			logCL, err := a.logCLManager.GetOrConnect(ctx, snid, addr)
			if err != nil {
				continue CONNECT
			}

			s, err = newSubscriber(ctx, a.topicID, logStreamID, snid, logCL, a.wanted, a.end, a.buffer, a.aggregationCV, a.logger)
			if err != nil {
				// logCL.Close()
				continue CONNECT
			}

			break CONNECT
		}

		if s != nil {
			a.subscribers[logStreamID] = s

			if err = a.runner.RunC(ctx, s.subscribe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *aggregator) handleTimeout(ctx context.Context) {
	for _, s := range a.subscribers {
		if !s.complete.Load() && !s.closed.Load() &&
			time.Since(s.getLastSubscribeAt()) >= a.timeout {
			s.stop()
		}
	}

	a.refreshSubscribers(ctx) //nolint:errcheck,revive // TODO: Handle an error returned.
}

func (a *aggregator) handleError(r aggregationItem) error {
	s, ok := a.subscribers[r.logStreamID]
	if !ok {
		return nil
	}

	if s.storageNodeID == r.storageNodeID && !s.closed.Load() {
		s.stop()
	}

	return r.result.Error
}

func (a *aggregator) processResult(r aggregationItem) error {
	var err error

	// NOTE: Ignore aggregationItem with GLSN less than p.wanted.
	// They can be delivered by subscribers that have not yet closed.
	if r.result.GLSN == types.InvalidGLSN {
		err = a.handleError(r)
		if errors.Is(err, verrors.ErrTrimmed) {
			a.dispatchQueue.pushBack(r)
		}
	} else if a.wanted == r.result.GLSN {
		a.dispatchQueue.pushBack(r)
		a.wanted++
		a.timer.Reset(a.timeout)
	}

	return err
}

func (a *aggregator) processBuffer(ctx context.Context) bool {
	needRefresh := false

	for {
		res, ok := a.buffer.Front()
		if !ok {
			break
		}

		if res.result.GLSN <= a.wanted {
			res, _ := a.buffer.Pop()
			err := a.processResult(res)
			if a.wanted == a.end ||
				errors.Is(err, verrors.ErrTrimmed) {
				return false
			}

			needRefresh = needRefresh || (err != nil && err != io.EOF)
		} else {
			break
		}
	}

	if needRefresh {
		a.refreshSubscribers(ctx) //nolint:errcheck,revive // TODO:: Handle an error returned.
	}

	return true
}

// dispatchQueue is a bounded, in-order queue that acts as a buffer between
// the aggregator and the dispatcher. It is implemented using a buffered channel
// to decouple the two components, ensuring that a slow user callback does not
// block the aggregation process.
type dispatchQueue struct {
	c      chan aggregationItem
	wanted types.GLSN
	logger *zap.Logger
}

func newDispatchQueue(begin, end types.GLSN, logger *zap.Logger) *dispatchQueue {
	q := &dispatchQueue{
		// BUG: If end-begin is too large, it can panic because of too large channel size.
		c:      make(chan aggregationItem, end-begin),
		wanted: begin,
		logger: logger.Named("dispatch_queue"),
	}
	return q
}

func (q *dispatchQueue) pushBack(item aggregationItem) {
	if !q.pushable(item) {
		q.logger.Panic("not pushable")
	}
	advance := item.result.Error == nil
	item.enqueueTime = time.Now()
	// NOTE: the sendC is not blocking since its size is enough to receive messages.
	q.sendC() <- item
	if advance {
		q.wanted++
	}
}

func (q *dispatchQueue) pushable(item aggregationItem) bool {
	return item.result.GLSN == q.wanted || item.result.Error != nil
}

func (q *dispatchQueue) close() {
	close(q.c)
}

func (q *dispatchQueue) sendC() chan<- aggregationItem {
	return q.c
}

func (q *dispatchQueue) recvC() <-chan aggregationItem {
	return q.c
}

// dispatcher is responsible for invoking the user-provided callback. It pulls
// ordered log entries from the dispatchQueue and executes the user's OnNext
// callback for each entry. This decouples the aggregation logic from the
// user's processing logic.
type dispatcher struct {
	onNextFunc OnNext
	queue      *dispatchQueue
	observer   SubscribeObserver
	logger     *zap.Logger
}

func (p *dispatcher) dispatch(ctx context.Context) {
	sentErr := false
	for item := range p.queue.recvC() {
		if sentErr {
			p.logger.Panic("multiple errors in dispatcher",
				zap.Uint64("glsn", uint64(item.result.GLSN)),
				zap.Uint64("llsn", uint64(item.result.LLSN)),
				zap.Error(item.result.Error),
			)
		}
		now := time.Now()
		p.onNextFunc(item.result.LogEntry, item.result.Error)
		item.stats.ProcessDuration = time.Since(now)
		item.stats.DispatchQueueWait = now.Sub(item.enqueueTime)
		sentErr = sentErr || item.result.Error != nil

		if p.observer != nil {
			p.observer.Observe(ctx, item.stats)
		}
	}
	if !sentErr {
		p.onNextFunc(varlogpb.LogEntry{}, io.EOF)
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
	return varlogpb.LogEntry{}, s.err
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
		logCL   *client.LogClient
		resultC <-chan client.SubscribeResult
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
	logCL   *client.LogClient
	resultC <-chan client.SubscribeResult

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
