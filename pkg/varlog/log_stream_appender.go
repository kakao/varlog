package varlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v2"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

// LogStreamAppender is a client only to be able to append to a particular log
// stream.
type LogStreamAppender interface {
	// AppendBatch appends dataBatch to the given log stream asynchronously.
	// Users can call this method without being blocked until the pipeline of
	// the LogStreamAppender is full. If the pipeline of the LogStreamAppender
	// is already full, it may become blocked. However, the process will
	// continue once a response is received from the storage node. A long block
	// duration with a configured WithCallTimeout can cause ErrCallTimeout to
	// occur.
	//
	// On completion of AppendBatch, the argument callback provided by users
	// will be invoked. All callback functions registered to the same
	// LogStreamAppender will be called by the same goroutine sequentially.
	// Therefore, the callback should be lightweight. If heavy work is
	// necessary for the callback, it would be better to use separate worker
	// goroutines.
	// Once the stream in the LogStreamAppender is either done or broken, the
	// AppendBatch returns an error. It returns an ErrClosed when the
	// LogStreamAppender is closed and an ErrCallTimeout when the call timeout
	// expires.
	//
	// It is safe to have multiple goroutines calling AppendBatch
	// simultaneously, but the order between them is not guaranteed.
	AppendBatch(dataBatch [][]byte, callback BatchCallback) error

	// Close closes the LogStreamAppender client. Once the client is closed,
	// calling AppendBatch will fail immediately. If AppendBatch still waits
	// for room of pipeline, Close will be blocked. It also waits for all
	// pending callbacks to be called.
	Close()
}

// BatchCallback is a callback function to notify the result of
// AppendBatch.
type BatchCallback func([]varlogpb.LogEntryMeta, error)

type cbQueueEntry struct {
	cb         BatchCallback
	data       [][]byte
	err        error
	meta       []varlogpb.LogEntryMeta
	expireTime time.Time
}

func newCallbackQueueEntry() *cbQueueEntry {
	return callbackQueueEntryPool.Get().(*cbQueueEntry)
}

func (cqe *cbQueueEntry) Release() {
	*cqe = cbQueueEntry{}
	callbackQueueEntryPool.Put(cqe)
}

var callbackQueueEntryPool = sync.Pool{
	New: func() any {
		return &cbQueueEntry{}
	},
}

type logStreamAppender struct {
	logStreamAppenderConfig
	stream     snpb.LogIO_AppendClient
	cancelFunc context.CancelCauseFunc
	causeFunc  func() error
	sema       chan struct{}
	sq         chan *cbQueueEntry
	rq         chan *cbQueueEntry
	cq         chan *cbQueueEntry
	wg         sync.WaitGroup
	closed     struct {
		xsync.RBMutex
		value bool
	}
}

var _ LogStreamAppender = (*logStreamAppender)(nil)

func (v *logImpl) newLogStreamAppender(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, opts ...LogStreamAppenderOption) (LogStreamAppender, error) {
	replicas, ok := v.replicasRetriever.Retrieve(tpid, lsid)
	if !ok {
		return nil, fmt.Errorf("client: log stream %d of topic %d does not exist", lsid, tpid)
	}

	snid := replicas[0].StorageNodeID
	addr := replicas[0].Address
	cl, err := v.logCLManager.GetOrConnect(ctx, snid, addr)
	if err != nil {
		v.allowlist.Deny(tpid, lsid)
		return nil, fmt.Errorf("client: %w", err)
	}

	ctx, cancelFunc := context.WithCancelCause(ctx)
	stream, err := cl.AppendStream(ctx)
	if err != nil {
		cancelFunc(err)
		return nil, fmt.Errorf("client: %w", err)
	}

	cfg := newLogStreamAppenderConfig(opts)
	cfg.tpid = tpid
	cfg.lsid = lsid
	lsa := &logStreamAppender{
		logStreamAppenderConfig: cfg,
		stream:                  stream,
		sema:                    make(chan struct{}, cfg.pipelineSize),
		sq:                      make(chan *cbQueueEntry, cfg.pipelineSize),
		rq:                      make(chan *cbQueueEntry, cfg.pipelineSize),
		cq:                      make(chan *cbQueueEntry, cfg.pipelineSize),
		cancelFunc:              cancelFunc,
		causeFunc: func() error {
			return context.Cause(ctx)
		},
	}
	_, err = v.runner.Run(func(context.Context) {
		lsa.callbackLoop()
	})
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	lsa.wg.Add(2)
	go lsa.sendLoop()
	go lsa.recvLoop()
	return lsa, nil
}

func (lsa *logStreamAppender) AppendBatch(dataBatch [][]byte, callback BatchCallback) error {
	rt := lsa.closed.RLock()
	defer lsa.closed.RUnlock(rt)
	if lsa.closed.value {
		return ErrClosed
	}

	if err := lsa.causeFunc(); err != nil {
		return err
	}

	now := time.Now()
	if lsa.callTimeout > 0 {
		timer := time.NewTimer(lsa.callTimeout)
		defer timer.Stop()
		select {
		case lsa.sema <- struct{}{}:
		case <-timer.C:
			lsa.cancelFunc(ErrCallTimeout)
			return ErrCallTimeout
		}
	} else {
		lsa.sema <- struct{}{}
	}

	qe := newCallbackQueueEntry()
	qe.data = dataBatch
	qe.cb = callback
	qe.expireTime = now.Add(lsa.callTimeout)
	lsa.sq <- qe
	return nil
}

func (lsa *logStreamAppender) Close() {
	lsa.cancelFunc(ErrClosed)

	lsa.closed.Lock()
	defer lsa.closed.Unlock()
	if lsa.closed.value {
		return
	}
	lsa.closed.value = true

	close(lsa.sq)
	lsa.wg.Wait()
}

func (lsa *logStreamAppender) sendLoop() {
	defer func() {
		close(lsa.rq)
		lsa.wg.Done()
	}()

	var sendErr error
	req := &snpb.AppendRequest{
		TopicID:     lsa.tpid,
		LogStreamID: lsa.lsid,
	}
	for qe := range lsa.sq {
		if sendErr == nil {
			req.Payload = qe.data

			var wg sync.WaitGroup
			var watchdog *time.Timer
			if lsa.callTimeout > 0 {
				wg.Add(1)
				watchdog = time.AfterFunc(time.Until(qe.expireTime), func() {
					defer wg.Done()
					lsa.cancelFunc(ErrCallTimeout)
				})
			}
			sendErr = lsa.stream.Send(req)
			if watchdog != nil && !watchdog.Stop() {
				wg.Wait()
			}
			if sendErr != nil {
				if cause := lsa.causeFunc(); cause != nil {
					sendErr = cause
				}
				_ = lsa.stream.CloseSend()
			}
		}
		if sendErr != nil {
			qe.err = sendErr
		}
		lsa.rq <- qe
	}
}

func (lsa *logStreamAppender) recvLoop() {
	defer func() {
		close(lsa.cq)
		lsa.wg.Done()
	}()

	var recvErr error
	rsp := &snpb.AppendResponse{}
	for qe := range lsa.rq {
		if recvErr == nil {
			var wg sync.WaitGroup
			var watchdog *time.Timer
			var meta []varlogpb.LogEntryMeta

			recvErr = qe.err
			if recvErr != nil {
				goto Call
			}

			if lsa.callTimeout > 0 {
				wg.Add(1)
				watchdog = time.AfterFunc(time.Until(qe.expireTime), func() {
					defer wg.Done()
					lsa.cancelFunc(ErrCallTimeout)
				})
			}

			rsp.Reset()
			recvErr = lsa.stream.RecvMsg(rsp)
			if watchdog != nil && !watchdog.Stop() {
				wg.Wait()
			}
			if recvErr != nil {
				goto Call
			}

			meta = make([]varlogpb.LogEntryMeta, len(rsp.Results))
			for idx, res := range rsp.Results {
				if len(res.Error) == 0 {
					meta[idx] = res.Meta
					continue
				}
				recvErr = errors.New(res.Error)
				lsa.cancelFunc(recvErr)
				break
			}
			qe.meta = meta
		}

	Call:
		if recvErr != nil {
			if cause := lsa.causeFunc(); cause != nil {
				recvErr = cause
			}
			qe.err = recvErr
		}
		lsa.cq <- qe
	}
}

func (lsa *logStreamAppender) callbackLoop() {
	for qe := range lsa.cq {
		var cb BatchCallback
		if qe.cb != nil {
			cb = qe.cb
		} else {
			cb = lsa.defaultBatchCallback
		}
		if cb != nil {
			cb(qe.meta, qe.err)
		}
		<-lsa.sema
	}
}
