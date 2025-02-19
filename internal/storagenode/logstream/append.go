package logstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
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

func (at *AppendTask) WaitForCompletion(ctx context.Context) (res []snpb.AppendResult, err error) {
	if at.err != nil {
		return nil, at.err
	}

	// Append batch requests can succeed partially. In case of partial failure,
	// failed log entries must be sequential. That is, suffixes of the batch
	// can be failed to append, whose length can be from zero to the full size.
	hasNonContextErr := false
	res = make([]snpb.AppendResult, at.dataBatchLen)
	for i := range at.apc.awgs {
		cerr := at.apc.awgs[i].wait(ctx)
		if cerr != nil {
			if !hasNonContextErr {
				hasNonContextErr = !errors.Is(cerr, context.Canceled) && !errors.Is(cerr, context.DeadlineExceeded)
			}

			res[i].Error = cerr.Error()
			if err == nil {
				err = cerr
			}
			continue
		}

		// It has not failed yet.
		if err == nil {
			res[i].Meta.TopicID = at.lse.tpid
			res[i].Meta.LogStreamID = at.lse.lsid
			res[i].Meta.GLSN = at.apc.awgs[i].glsn
			res[i].Meta.LLSN = at.apc.awgs[i].llsn
			at.apc.awgs[i].release()
			continue
		}

		// It panics when the batch's success and failure are interleaved.
		// However, context errors caused by clients can be ignored since
		// the batch can succeed in the storage node, although clients
		// canceled it.
		// Once the codebase stabilizes, it is planned to be removed.
		if hasNonContextErr {
			at.lse.logger.Panic("Results of batch requests of Append RPC must not be interleaved with success and failure", zap.Error(err))
		}
		res[i].Error = err.Error()
		at.apc.awgs[i].release()
	}
	if res[0].Meta.GLSN.Invalid() {
		return nil, err
	}
	return res, nil
}

func appendTaskDeferredFunc(at *AppendTask) {
	at.lse.inflight.Add(-1)
	at.lse.inflightAppend.Add(-1)
	if at.lse.lsm != nil {
		at.lse.lsm.AppendDuration.Add(time.Since(at.start).Microseconds())
	}
}

type appendContext struct {
	st  *sequenceTask
	wwg *writeWaitGroup
	// NOTE(jun): awgs represents a collection of wait groups corresponding to
	// each log entry in the batch. While storage typically writes log entries
	// in a batch simultaneously, the commit operation, although expected to
	// handle all entries in a batch, is not strictly enforced to do so.
	// Therefore, we should maintain awgs until we can guarantee batch-level
	// atomic commits.
	awgs       []*appendWaitGroup
	totalBytes int64
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

	appendTask.apc = appendContext{
		awgs: make([]*appendWaitGroup, 0, dataBatchLen),
	}

	var preparationDuration time.Duration
	defer func() {
		if lse.lsm != nil {
			lse.lsm.AppendLogs.Add(int64(dataBatchLen))
			lse.lsm.AppendBytes.Add(appendTask.apc.totalBytes)
			lse.lsm.AppendOperations.Add(1)
			lse.lsm.AppendPreparationMicro.Add(preparationDuration.Microseconds())

			lse.lsm.LogRPCServerBatchSize.Record(context.Background(), telemetry.RPCKindAppend, codes.OK, appendTask.apc.totalBytes)
			lse.lsm.LogRPCServerLogEntriesPerBatch.Record(context.Background(), telemetry.RPCKindAppend, codes.OK, int64(dataBatchLen))
		}
	}()

	lse.prepareAppendContext(dataBatch, &appendTask.apc)
	preparationDuration = time.Since(startTime)
	lse.sendSequenceTask(ctx, appendTask.apc.st)
	return nil
}

func (lse *Executor) prepareAppendContext(dataBatch [][]byte, apc *appendContext) {
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

	st.wb = lse.stg.NewWriteBatch()
	st.cwts = newListQueue()
	for i := 0; i < len(dataBatch); i++ {
		logEntrySize := int64(len(dataBatch[i]))
		apc.totalBytes += logEntrySize
		if lse.lsm != nil {
			// TODO: Set the correct status code.
			lse.lsm.LogRPCServerLogEntrySize.Record(context.Background(), telemetry.RPCKindAppend, codes.OK, logEntrySize)
		}
		awg := newAppendWaitGroup(st.wwg)
		st.cwts.PushFront(newCommitWaitTask(awg))
		apc.awgs = append(apc.awgs, awg)
	}
	st.awgs = apc.awgs
}

func (lse *Executor) sendSequenceTask(ctx context.Context, st *sequenceTask) {
	if err := lse.sq.send(ctx, st); err != nil {
		st.wwg.done(err)
		_ = st.wb.Close()
		releaseCommitWaitTaskList(st.cwts)
		releaseReplicateTasks(st.rts.tasks)
		releaseReplicateTaskSlice(st.rts)
		st.release()
	}
}
