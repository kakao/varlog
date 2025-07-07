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

	transmitCV := make(chan struct{}, 1)

	mctx, cancel := subscribeRunner.WithManagedCancel(context.Background())
	closer = func() {
		cancel()
		subscribeRunner.Stop()
	}

	dispatchQueue := newDispatchQueue(begin, end, v.logger)

	tlogger := v.logger.Named("transmitter")
	// The maximum length of transmitQ is end - begin in the worst case.
	// Therefore, we can approximate half of it.
	// TODO: Use a better approximation.
	approxTransmitQSize := int((end - begin) / 2)
	tsm := &transmitter{
		topicID:           topicID,
		subscribers:       make(map[types.LogStreamID]*subscriber),
		replicasRetriever: v.replicasRetriever,
		logCLManager:      v.logCLManager,
		dispatchQueue:     dispatchQueue,
		wanted:            begin,
		end:               end,
		transmitQ:         &transmitQueue{pq: newPriorityQueue(approxTransmitQSize)},
		transmitCV:        transmitCV,
		timeout:           subscribeOpts.timeout,
		runner:            runner.New("transmitter", tlogger),
		logger:            tlogger,
	}

	dis := &dispatcher{
		onNextFunc: onNext,
		queue:      dispatchQueue,
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
	result        client.SubscribeResult
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
			result: client.InvalidSubscribeResult,
		}, false
	}

	return heap.Pop(tq.pq).(transmitResult), true
}

func (tq *transmitQueue) Front() (transmitResult, bool) {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if tq.pq.Len() == 0 {
		return transmitResult{
			result: client.InvalidSubscribeResult,
		}, false
	}

	return (*tq.pq)[0].(transmitResult), true
}

type subscriber struct {
	topicID         types.TopicID
	logStreamID     types.LogStreamID
	storageNodeID   types.StorageNodeID
	logCL           *client.LogClient
	resultC         <-chan client.SubscribeResult
	cancelSubscribe context.CancelFunc

	transmitQ  *transmitQueue
	transmitCV chan struct{}

	done     chan struct{}
	closed   atomic.Bool
	complete atomic.Bool

	lastSubscribeAt atomic.Int64

	logger *zap.Logger
}

func newSubscriber(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, storageNodeID types.StorageNodeID, logCL *client.LogClient, begin, end types.GLSN, transmitQ *transmitQueue, transmitCV chan struct{}, logger *zap.Logger) (*subscriber, error) {
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
			r := transmitResult{
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
	nsec := s.lastSubscribeAt.Load()
	return time.Unix(0, nsec)
}

type transmitter struct {
	topicID           types.TopicID
	subscribers       map[types.LogStreamID]*subscriber
	replicasRetriever ReplicasRetriever
	dispatchQueue     *dispatchQueue
	wanted            types.GLSN
	end               types.GLSN

	transmitQ  *transmitQueue
	transmitCV chan struct{}

	timeout time.Duration
	timer   *time.Timer

	logCLManager *client.Manager[*client.LogClient]
	runner       *runner.Runner
	logger       *zap.Logger
}

func (p *transmitter) transmit(ctx context.Context) {
	defer func() {
		p.dispatchQueue.close()
		p.runner.Stop()
	}()

	p.timer = time.NewTimer(p.timeout)
	defer p.timer.Stop()

	_ = p.refreshSubscriber(ctx)

	for {
		select {
		case <-ctx.Done():
			var item transmitResult
			item.result = client.InvalidSubscribeResult
			item.result.Error = ctx.Err()
			p.dispatchQueue.pushBack(item)
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
	for _, s := range p.subscribers {
		if !s.complete.Load() && !s.closed.Load() &&
			time.Since(s.getLastSubscribeAt()) >= p.timeout {
			s.stop()
		}
	}

	p.refreshSubscriber(ctx) //nolint:errcheck,revive // TODO: Handle an error returned.
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
			p.dispatchQueue.pushBack(r)
		}
	} else if p.wanted == r.result.GLSN {
		p.dispatchQueue.pushBack(r)
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
		p.refreshSubscriber(ctx) //nolint:errcheck,revive // TODO:: Handle an error returned.
	}

	return true
}

// dispatchQueue buffers transmitResult items in order before invoking the user
// callback. It ensures results are delivered in sequence, starting from the
// GLSN specified by wanted.
type dispatchQueue struct {
	c      chan transmitResult
	wanted types.GLSN
	logger *zap.Logger
}

func newDispatchQueue(begin, end types.GLSN, logger *zap.Logger) *dispatchQueue {
	q := &dispatchQueue{
		// BUG: If end-begin is too large, it can panic because of too large channel size.
		c:      make(chan transmitResult, end-begin),
		wanted: begin,
		logger: logger.Named("dispatch_queue"),
	}
	return q
}

func (q *dispatchQueue) pushBack(item transmitResult) {
	if !q.pushable(item) {
		q.logger.Panic("not pushable")
	}
	advance := item.result.Error == nil
	// NOTE: the sendC is not blocking since its size is enough to receive messages.
	q.sendC() <- item
	if advance {
		q.wanted++
	}
}

func (q *dispatchQueue) pushable(item transmitResult) bool {
	return item.result.GLSN == q.wanted || item.result.Error != nil
}

func (q *dispatchQueue) close() {
	close(q.c)
}

func (q *dispatchQueue) sendC() chan<- transmitResult {
	return q.c
}

func (q *dispatchQueue) recvC() <-chan transmitResult {
	return q.c
}

type dispatcher struct {
	onNextFunc OnNext
	queue      *dispatchQueue
	logger     *zap.Logger
}

func (p *dispatcher) dispatch(_ context.Context) {
	sentErr := false
	for item := range p.queue.recvC() {
		if sentErr {
			p.logger.Panic("multiple errors in dispatcher",
				zap.Uint64("glsn", uint64(item.result.GLSN)),
				zap.Uint64("llsn", uint64(item.result.LLSN)),
				zap.Error(item.result.Error),
			)
		}
		p.onNextFunc(item.result.LogEntry, item.result.Error)
		sentErr = sentErr || item.result.Error != nil
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
