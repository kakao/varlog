package varlog

import (
	"container/list"
	"context"
	"errors"
	"io"
	"sync"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/verrors"
)

type SubscribeOption struct{}

type SubscribeCloser func()

func (v *varlog) subscribe(ctx context.Context, begin, end types.GLSN, onNext OnNext, opts SubscribeOption) (closer SubscribeCloser, err error) {
	if begin >= end {
		return nil, verrors.ErrInvalid
	}
	replicasMap := v.replicasRetriever.All()

	subscribers := make(map[types.LogStreamID]*subscriber, len(replicasMap))
	transmitCV := make(chan transmitSignal, end-begin)

	for logStreamID, replicas := range replicasMap {
		primarySNID := replicas[0].GetStorageNodeID()
		primaryAddr := replicas[0].GetAddress()
		primaryLogCL, err := v.logCLManager.GetOrConnect(ctx, primarySNID, primaryAddr)
		if err != nil {
			return nil, err
		}
		subscriber, err := newSubscriber(ctx, logStreamID, primaryLogCL, begin, end, transmitCV, v.logger)
		if err != nil {
			return nil, err
		}
		subscribers[logStreamID] = subscriber
	}

	mctx, cancel := v.runner.WithManagedCancel(context.Background())
	closer = func() {
		cancel()
	}

	sleq := newSubscribedLogEntiresQueue(begin, end, closer, v.logger)

	tsm := &transmitter{
		subscribers: subscribers,
		sleq:        sleq,
		wanted:      begin,
		end:         end,
		transmitCV:  transmitCV,
		logger:      v.logger.Named("transmitter"),
	}

	dis := &dispatcher{
		onNextFunc: onNext,
		sleq:       sleq,
		logger:     v.logger,
	}
	if err = v.runner.RunC(mctx, tsm.transmit); err != nil {
		goto errOut
	}
	if err = v.runner.RunC(mctx, dis.dispatch); err != nil {
		goto errOut
	}

	for _, subscriber := range subscribers {
		subscriber.transmitCV = transmitCV
		if err = v.runner.RunC(mctx, subscriber.subscribe); err != nil {
			goto errOut
		}
	}
	return closer, nil

errOut:
	closer()
	return nil, err
}

type transmitSignal struct {
	glsn   types.GLSN
	closed bool
}

type subscriber struct {
	logStreamID types.LogStreamID
	logCL       logc.LogIOClient
	resultC     <-chan logc.SubscribeResult

	queue   *list.List
	muQueue sync.RWMutex

	transmitCV chan<- transmitSignal
	closed     atomicutil.AtomicBool
	logger     *zap.Logger
}

func newSubscriber(ctx context.Context, logStreamID types.LogStreamID, logCL logc.LogIOClient, begin, end types.GLSN, transmitCV chan<- transmitSignal, logger *zap.Logger) (*subscriber, error) {
	resultC, err := logCL.Subscribe(ctx, logStreamID, begin, end)
	if err != nil {
		return nil, err
	}
	s := &subscriber{
		logStreamID: logStreamID,
		logCL:       logCL,
		resultC:     resultC,
		queue:       list.New(),
		transmitCV:  transmitCV,
		logger:      logger.Named("subscriber").With(zap.Uint32("lsid", uint32(logStreamID))),
	}
	s.closed.Store(false)
	return s, nil
}

func (s *subscriber) subscribe(ctx context.Context) {
	defer func() {
		s.closed.Store(true)
		s.transmitCV <- transmitSignal{glsn: types.InvalidGLSN, closed: true}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case res, ok := <-s.resultC:
			// NOTE: ErrUndecidable is ignored since it is neither sign of connection
			// failure nor a miss of logs.
			if !ok || res.Error == io.EOF || errors.Is(res.Error, verrors.ErrUndecidable) {
				return
			}
			glsn := res.GLSN
			s.muQueue.Lock()
			s.queue.PushBack(res)
			s.muQueue.Unlock()
			s.transmitCV <- transmitSignal{glsn: glsn, closed: false}
		}
	}
}

func (s *subscriber) front() (logc.SubscribeResult, bool) {
	s.muQueue.RLock()
	defer s.muQueue.RUnlock()
	front := s.queue.Front()
	if front == nil {
		return logc.InvalidSubscribeResult, false
	}
	return front.Value.(logc.SubscribeResult), true
}

func (s *subscriber) popFront() logc.SubscribeResult {
	s.muQueue.Lock()
	defer s.muQueue.Unlock()
	front := s.queue.Front()
	if front == nil {
		s.logger.Panic("empty queue")
	}
	return s.queue.Remove(front).(logc.SubscribeResult)
}

type transmitter struct {
	subscribers map[types.LogStreamID]*subscriber
	sleq        *subscribedLogEntriesQueue
	wanted      types.GLSN
	end         types.GLSN
	transmitCV  <-chan transmitSignal
	logger      *zap.Logger
}

func (p *transmitter) transmit(ctx context.Context) {
	defer p.sleq.close()

	for {
		select {
		case <-ctx.Done():
			res := logc.InvalidSubscribeResult
			res.Error = ctx.Err()
			p.logger.Debug("transmit error result", zap.Reflect("res", res))
			p.sleq.pushBack(res)
			return
		case signal := <-p.transmitCV:
			if !signal.closed && signal.glsn != p.wanted {
				continue
			}
			if repeat := p.transmitLoop(); !repeat {
				return
			}
		}
	}
}

func (p *transmitter) transmitLoop() bool {
	for {
		exist, res, closedAll := p.checkTransmittable()
		if !exist {
			if closedAll {
				p.logger.Debug("transmit completed: all subscribers are closed")
				return false
			}
			p.logger.Debug("there is no transmittable")
			return true
		}

		ok := res.Error == nil
		p.sleq.pushBack(res)
		if ok {
			p.wanted++
		}
		if !ok || p.wanted == p.end || closedAll {
			p.logger.Debug("transmit completed", zap.Bool("ok", ok), zap.Any("wanted", p.wanted), zap.Any("end", p.end), zap.Bool("closedAll", closedAll))
			return false
		}
	}
}

func (p *transmitter) checkTransmittable() (pushable bool, result logc.SubscribeResult, closedAll bool) {
	maybeHole := true
	closedAll = true
	for logStreamID, subscriber := range p.subscribers {
		closedAll = closedAll && subscriber.closed.Load()
		res, exist := subscriber.front()
		if !exist {
			p.logger.Debug("check subscriber: empty", zap.Uint32("lsid", uint32(logStreamID)))
			maybeHole = false
			continue
		}
		if p.sleq.pushable(res) {
			p.logger.Debug("check subscriber: transmittable", zap.Uint32("lsid", uint32(logStreamID)), zap.Any("res", res))
			res = subscriber.popFront()
			return true, res, false
		}
	}
	if maybeHole {
		res := logc.InvalidSubscribeResult
		res.Error = errors.New("missing log entry")
		p.logger.Debug("check subscriber: transmittable (hole)", zap.Any("res", res))
		return true, res, closedAll
	}
	return false, logc.InvalidSubscribeResult, closedAll
}

type subscribedLogEntriesQueue struct {
	c      chan logc.SubscribeResult
	wanted types.GLSN
	end    types.GLSN
	closer SubscribeCloser
	logger *zap.Logger
}

func newSubscribedLogEntiresQueue(begin, end types.GLSN, closer SubscribeCloser, logger *zap.Logger) *subscribedLogEntriesQueue {
	q := &subscribedLogEntriesQueue{
		c:      make(chan logc.SubscribeResult, end-begin),
		wanted: begin,
		end:    end,
		closer: closer,
		logger: logger.Named("subscribed_log_entries_queue"),
	}
	return q
}

func (q *subscribedLogEntriesQueue) pushBack(result logc.SubscribeResult) {
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

func (q *subscribedLogEntriesQueue) pushable(result logc.SubscribeResult) bool {
	return result.LogEntry.GLSN == q.wanted || result.Error != nil
}

func (q *subscribedLogEntriesQueue) close() {
	close(q.c)
	q.closer()
}

func (q *subscribedLogEntriesQueue) sendC() chan<- logc.SubscribeResult {
	return q.c
}

func (q *subscribedLogEntriesQueue) recvC() <-chan logc.SubscribeResult {
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
		p.onNextFunc(types.InvalidLogEntry, io.EOF)
	}
}
