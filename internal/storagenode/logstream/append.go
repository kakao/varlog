package logstream

import (
	"context"
	"sync"
	"time"

	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

var appendTaskPool = sync.Pool{
	New: func() any {
		return &AppendTask{}
	},
}

type AppendTask struct {
	lse          *Executor
	deferredFunc func(*AppendTask)
	err          error
	start        time.Time
	apc          appendContext
	dataBatchLen int

	TopicID      types.TopicID
	LogStreamID  types.LogStreamID
	RPCStartTime time.Time
}

func NewAppendTask() *AppendTask {
	at := appendTaskPool.Get().(*AppendTask)
	return at
}

func (at *AppendTask) SetError(err error) {
	at.err = err
}

func (at *AppendTask) Release() {
	if at.deferredFunc != nil {
		at.deferredFunc(at)
	}
	*at = AppendTask{}
	appendTaskPool.Put(at)
}

func (at *AppendTask) ReleaseWriteWaitGroup() {
	at.apc.wwg.release()
}

func (at *AppendTask) WaitForCompletion(ctx context.Context) ([]snpb.AppendResult, error) {
	if at.err != nil {
		return nil, at.err
	}

	// Append batch requests will eventually only succeed or fail atomically.
	// This is a work in progress to resolve #843.
	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	res := make([]snpb.AppendResult, at.dataBatchLen)
	done := make(chan struct{})
	go func() {
		defer close(done)

		err = at.apc.awg.wait(cctx)
		if err != nil {
			// Unable to determine if awg can be safely released. Not releasing
			// awg here is not a problem, since it is expected to be garbage
			// collected.
			//
			// If the append operation failed, the operation might still be
			// handled by another component, such as the committer. In this
			// case, releasing awg here would prevent the committer from
			// checking whether the append operation has completed, since awg
			// would already be cleared. This could even introduce a data race
			// that is not detected by the race detector. More severely, when
			// this happens, the WaitGroup counter of awg can become negative,
			// resulting in a panic.
			//
			// TODO(ijsong): Refactor waiting logic to clearly distinguish
			// between user cancellation, storage node shutdown, and append
			// operation failure.
			return
		}

		for i := range at.apc.awg.batchLen {
			res[i].Meta.TopicID = at.lse.tpid
			res[i].Meta.LogStreamID = at.lse.lsid
			res[i].Meta.GLSN = at.apc.awg.beginLSN.GLSN + types.GLSN(i)
			res[i].Meta.LLSN = at.apc.awg.beginLSN.LLSN + types.LLSN(i)
		}
		at.apc.awg.release()
	}()

	select {
	case <-ctx.Done():
		// NOTE: If the request is timed out or canceled by the peer, we cannot
		// guarantee either success or failure.
		cancel()
		<-done
		return nil, ctx.Err()
	case <-done:
		if err != nil {
			return nil, err
		}
		return res, nil
	}
}

func appendTaskDeferredFunc(at *AppendTask) {
	at.lse.inflight.Add(-1)
	at.lse.inflightAppend.Add(-1)
}

type appendContext struct {
	st  *sequenceTask
	wwg *writeWaitGroup
	awg *appendWaitGroup
}

func (lse *Executor) AppendAsync(ctx context.Context, dataBatch [][]byte, appendTask *AppendTask) error {
	lse.inflight.Add(1)
	lse.inflightAppend.Add(1)

	startTime := time.Now()
	dataBatchLen := len(dataBatch)

	appendTask.start = startTime
	appendTask.lse = lse
	appendTask.dataBatchLen = dataBatchLen
	appendTask.deferredFunc = appendTaskDeferredFunc

	switch lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		return verrors.ErrSealed
	case executorStateClosed:
		return verrors.ErrClosed
	}
	if !lse.isPrimary() {
		return snerrors.ErrNotPrimary
	}

	var totalBytes int64
	var preparationDuration time.Duration
	defer func() {
		if lse.lsm != nil {
			lse.lsm.AppendPreparationDuration.Record(context.Background(), preparationDuration.Microseconds())
			lse.lsm.LogRPCServerBatchSize.Record(context.Background(), totalBytes)
			lse.lsm.LogRPCServerLogEntriesPerBatch.Record(context.Background(), int64(dataBatchLen))
		}
	}()

	totalBytes = lse.prepareAppendContext(dataBatch, &appendTask.apc)
	preparationDuration = time.Since(startTime)
	lse.sendSequenceTask(ctx, appendTask.apc.st)
	return nil
}

func (lse *Executor) prepareAppendContext(dataBatch [][]byte, apc *appendContext) int64 {
	numBackups := len(lse.primaryBackups) - 1

	st := newSequenceTask()
	apc.st = st

	// data batch
	st.dataBatch = dataBatch

	// replicate tasks
	st.rts = newReplicateTaskSlice()
	for i := 0; i < numBackups; i++ {
		rt := newReplicateTask()
		rt.tpid = lse.tpid
		rt.lsid = lse.lsid
		rt.dataList = dataBatch
		st.rts.tasks = append(st.rts.tasks, rt)
	}

	// write wait group
	st.wwg = newWriteWaitGroup()
	apc.wwg = st.wwg

	var totalBytes int64
	st.wb = lse.stg.NewWriteBatch()
	for i := 0; i < len(dataBatch); i++ {
		logEntrySize := int64(len(dataBatch[i]))
		totalBytes += logEntrySize
		if lse.lsm != nil {
			lse.lsm.LogRPCServerLogEntrySize.Record(context.Background(), logEntrySize)
		}
	}
	awg := newAppendWaitGroup(st.wwg, len(dataBatch))
	apc.awg = awg
	st.cwt = newCommitWaitTask(awg, len(dataBatch))
	st.awg = awg
	return totalBytes
}

func (lse *Executor) sendSequenceTask(ctx context.Context, st *sequenceTask) {
	if err := lse.sq.send(ctx, st); err != nil {
		st.wwg.done(err)
		_ = st.wb.Close()
		st.cwt.release()
		releaseReplicateTasks(st.rts.tasks)
		releaseReplicateTaskSlice(st.rts)
		st.release()
	}
}
