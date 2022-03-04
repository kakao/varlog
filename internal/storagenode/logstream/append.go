package logstream

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/kakao/varlog/internal/batchlet"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

type appendContext struct {
	sts        []*sequenceTask
	wwgs       []*writeWaitGroup
	awgs       []*appendWaitGroup
	totalBytes int64
}

// Append appends a batch of logs to the log stream.
func (lse *Executor) Append(ctx context.Context, dataBatch [][]byte) ([]snpb.AppendResult, error) {
	atomic.AddInt64(&lse.inflight, 1)
	defer atomic.AddInt64(&lse.inflight, -1)

	switch lse.esm.load() {
	case executorStateSealing, executorStateSealed, executorStateLearning:
		return nil, verrors.ErrSealed
	case executorStateClosed:
		return nil, verrors.ErrClosed
	}

	if !lse.isPrimary() {
		return nil, errors.New("log stream: not primary")
	}

	startTime := time.Now()
	var preparationDuration time.Duration
	dataBatchLen := len(dataBatch)
	apc := appendContext{
		sts:  make([]*sequenceTask, 0, dataBatchLen/batchlet.LengthClasses[0]),
		wwgs: make([]*writeWaitGroup, 0, dataBatchLen/batchlet.LengthClasses[0]),
		awgs: make([]*appendWaitGroup, 0, dataBatchLen),
	}

	defer func() {
		if lse.lsm == nil {
			return
		}
		atomic.AddInt64(&lse.lsm.AppendLogs, int64(dataBatchLen))
		atomic.AddInt64(&lse.lsm.AppendBytes, apc.totalBytes)
		atomic.AddInt64(&lse.lsm.AppendDuration, time.Since(startTime).Milliseconds())
		atomic.AddInt64(&lse.lsm.AppendOperations, 1)
		atomic.AddInt64(&lse.lsm.AppendPreparationMicro, preparationDuration.Microseconds())
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
			batchletLen = batchletEndIdx - begin
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
		st.rts = append(st.rts, rt)
	}

	// write wait group
	st.wwg = newWriteWaitGroup()
	apc.wwgs = append(apc.wwgs, st.wwg)

	// st.dwb = lse.stg.NewWriteBatch().Deferred(batchletClassIdx)
	st.wb = lse.stg.NewWriteBatch()
	st.cwts = newListQueue()
	for i := 0; i < len(batchletData); i++ {
		// st.dwb.PutData(batchletData[i])
		apc.totalBytes += int64(len(batchletData[i]))
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
		releaseReplicateTasks(st.rts)
		releaseReplicateTaskSlice(st.rts)
		st.release()
	}
}

func (lse *Executor) waitForCompletionOfAppends(ctx context.Context, dataBatchLen int, awgs []*appendWaitGroup) ([]snpb.AppendResult, error) {
	var err error
	result := make([]snpb.AppendResult, dataBatchLen)
	for i := range awgs {
		cerr := awgs[i].wait(ctx)
		if err == nil && cerr != nil {
			err = cerr
		}
		if cerr == nil {
			result[i].Meta.TopicID = lse.tpid
			result[i].Meta.LogStreamID = lse.lsid
			result[i].Meta.GLSN = awgs[i].glsn
			result[i].Meta.LLSN = awgs[i].llsn
			awgs[i].release()
		}
	}
	return result, err
}
