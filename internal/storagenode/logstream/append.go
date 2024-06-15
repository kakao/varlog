package logstream

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/internal/batchlet"
	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
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

	RPCStartTime time.Time

	Request snpb.AppendRequest
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
	req := at.Request
	req.ResetReuse()
	*at = AppendTask{}
	at.Request = req
	appendTaskPool.Put(at)
}

func (at *AppendTask) ReleaseWriteWaitGroups() {
	for i := range at.apc.wwgs {
		at.apc.wwgs[i].release()
	}
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
	sts        []*sequenceTask
	wwgs       []*writeWaitGroup
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

	_, batchletLen := batchlet.SelectLengthClass(dataBatchLen)
	batchletCount := dataBatchLen / batchletLen
	if dataBatchLen%batchletLen > 0 {
		batchletCount++
	}

	appendTask.apc = appendContext{
		sts:  make([]*sequenceTask, 0, batchletCount),
		wwgs: make([]*writeWaitGroup, 0, batchletCount),
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
	lse.sendSequenceTasks(ctx, appendTask.apc.sts)
	return nil
}

// Append appends a batch of logs to the log stream.
func (lse *Executor) Append(ctx context.Context, dataBatch [][]byte) ([]snpb.AppendResult, error) {
	lse.inflight.Add(1)
	lse.inflightAppend.Add(1)

	defer func() {
		lse.inflightAppend.Add(-1)
		lse.inflight.Add(-1)
	}()

	switch lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		return nil, verrors.ErrSealed
	case executorStateClosed:
		return nil, verrors.ErrClosed
	}

	if !lse.isPrimary() {
		return nil, snerrors.ErrNotPrimary
	}

	startTime := time.Now()
	var preparationDuration time.Duration
	dataBatchLen := len(dataBatch)

	_, batchletLen := batchlet.SelectLengthClass(dataBatchLen)
	batchletCount := dataBatchLen / batchletLen
	if dataBatchLen%batchletLen > 0 {
		batchletCount++
	}

	apc := appendContext{
		sts:  make([]*sequenceTask, 0, batchletCount),
		wwgs: make([]*writeWaitGroup, 0, batchletCount),
		awgs: make([]*appendWaitGroup, 0, dataBatchLen),
	}

	defer func() {
		if lse.lsm == nil {
			return
		}
		lse.lsm.AppendLogs.Add(int64(dataBatchLen))
		lse.lsm.AppendBytes.Add(apc.totalBytes)
		lse.lsm.AppendDuration.Add(time.Since(startTime).Microseconds())
		lse.lsm.AppendOperations.Add(1)
		lse.lsm.AppendPreparationMicro.Add(preparationDuration.Microseconds())

		// TODO: Set a correct error code.
		lse.lsm.LogRPCServerBatchSize.Record(context.Background(), telemetry.RPCKindAppend, codes.OK, apc.totalBytes)
		lse.lsm.LogRPCServerLogEntriesPerBatch.Record(context.Background(), telemetry.RPCKindAppend, codes.OK, int64(dataBatchLen))
	}()

	lse.prepareAppendContext(dataBatch, &apc)
	preparationDuration = time.Since(startTime)
	lse.sendSequenceTasks(ctx, apc.sts)
	res, err := lse.waitForCompletionOfAppends(ctx, dataBatchLen, apc.awgs)
	if err == nil {
		for i := range apc.wwgs {
			apc.wwgs[i].release()
		}
	}
	return res, err
}

func (lse *Executor) prepareAppendContext(dataBatch [][]byte, apc *appendContext) {
	begin, end := 0, len(dataBatch)
	for begin < end {
		batchletClassIdx, batchletLen := batchlet.SelectLengthClass(end - begin)
		batchletEndIdx := begin + batchletLen
		if batchletEndIdx > end {
			batchletEndIdx = end
		}

		lse.prepareAppendContextInternal(dataBatch, begin, batchletEndIdx, batchletClassIdx, apc)
		begin = batchletEndIdx
	}
}

func (lse *Executor) prepareAppendContextInternal(dataBatch [][]byte, begin, end, batchletClassIdx int, apc *appendContext) {
	numBackups := len(lse.primaryBackups) - 1
	batchletData := dataBatch[begin:end]

	st := newSequenceTask()
	apc.sts = append(apc.sts, st)

	// data batch
	st.dataBatch = batchletData

	// replicate tasks
	st.rts = newReplicateTaskSlice()
	for i := 0; i < numBackups; i++ {
		rt := newReplicateTask(batchletClassIdx)
		rt.tpid = lse.tpid
		rt.lsid = lse.lsid
		rt.dataList = batchletData
		st.rts.tasks = append(st.rts.tasks, rt)
	}

	// write wait group
	st.wwg = newWriteWaitGroup()
	apc.wwgs = append(apc.wwgs, st.wwg)

	// st.dwb = lse.stg.NewWriteBatch().Deferred(batchletClassIdx)
	st.wb = lse.stg.NewWriteBatch()
	st.cwts = newListQueue()
	for i := 0; i < len(batchletData); i++ {
		// st.dwb.PutData(batchletData[i])
		logEntrySize := int64(len(batchletData[i]))
		apc.totalBytes += logEntrySize
		if lse.lsm != nil {
			// TODO: Set the correct status code.
			lse.lsm.LogRPCServerLogEntrySize.Record(context.Background(), telemetry.RPCKindAppend, codes.OK, logEntrySize)
		}
		awg := newAppendWaitGroup(st.wwg)
		st.cwts.PushFront(newCommitWaitTask(awg))
		apc.awgs = append(apc.awgs, awg)
	}
	st.awgs = apc.awgs[begin:end]
}

func (lse *Executor) sendSequenceTasks(ctx context.Context, sts []*sequenceTask) {
	var err error
	sendIdx := 0
	for sendIdx < len(sts) {
		err = lse.sq.send(ctx, sts[sendIdx])
		if err != nil {
			break
		}
		sendIdx++
	}
	for stIdx := sendIdx; stIdx < len(sts); stIdx++ {
		st := sts[stIdx]
		st.wwg.done(err)
		// _ = st.dwb.Close()
		_ = st.wb.Close()
		releaseCommitWaitTaskList(st.cwts)
		releaseReplicateTasks(st.rts.tasks)
		releaseReplicateTaskSlice(st.rts)
		st.release()
	}
}

func (lse *Executor) waitForCompletionOfAppends(ctx context.Context, dataBatchLen int, awgs []*appendWaitGroup) ([]snpb.AppendResult, error) {
	var err error
	result := make([]snpb.AppendResult, dataBatchLen)
	for i := range awgs {
		cerr := awgs[i].wait(ctx)
		if cerr != nil {
			result[i].Error = cerr.Error()
			if err == nil {
				err = cerr
			}
			continue
		}
		if err != nil {
			lse.logger.Panic("Results of batch requests of Append RPC must not be interleaved with success and failure", zap.Error(err))
		}
		result[i].Meta.TopicID = lse.tpid
		result[i].Meta.LogStreamID = lse.lsid
		result[i].Meta.GLSN = awgs[i].glsn
		result[i].Meta.LLSN = awgs[i].llsn
		awgs[i].release()
	}
	if result[0].Meta.GLSN.Invalid() {
		return nil, err
	}
	return result, nil
}
