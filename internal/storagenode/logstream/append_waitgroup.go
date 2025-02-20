package logstream

import (
	"context"
	"sync"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

var writeWaitGroupPool = sync.Pool{
	New: func() interface{} {
		return &writeWaitGroup{}
	},
}

type writeWaitGroup struct {
	ch  chan struct{}
	err error
}

func newWriteWaitGroup() *writeWaitGroup {
	wwg := writeWaitGroupPool.Get().(*writeWaitGroup)
	wwg.ch = make(chan struct{})
	return wwg
}

func (wwg *writeWaitGroup) release() {
	wwg.ch = nil
	wwg.err = nil
	writeWaitGroupPool.Put(wwg)
}

func (wwg *writeWaitGroup) done(err error) {
	wwg.err = err
	close(wwg.ch)
}

func (wwg *writeWaitGroup) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wwg.ch:
		return wwg.err
	}
}

var appendWaitGroupPool = &sync.Pool{
	New: func() interface{} {
		return &appendWaitGroup{}
	},
}

type appendWaitGroup struct {
	// beginLSN is the first log sequence number of the batch.
	beginLSN varlogpb.LogSequenceNumber
	// batchLen is the number of log entries in the batch.
	batchLen  int
	wwg       *writeWaitGroup
	cwg       sync.WaitGroup
	commitErr error
}

func newAppendWaitGroup(wwg *writeWaitGroup, batchLen int) *appendWaitGroup {
	awg := appendWaitGroupPool.Get().(*appendWaitGroup)
	awg.batchLen = batchLen
	awg.wwg = wwg
	awg.cwg.Add(1)
	return awg
}

func (awg *appendWaitGroup) release() {
	awg.beginLSN.GLSN = types.InvalidGLSN
	awg.beginLSN.LLSN = types.InvalidLLSN
	awg.batchLen = 0
	awg.wwg = nil
	awg.cwg = sync.WaitGroup{}
	appendWaitGroupPool.Put(awg)
}

func (awg *appendWaitGroup) setBeginGLSN(glsn types.GLSN) {
	if awg != nil {
		awg.beginLSN.GLSN = glsn
	}
}

func (awg *appendWaitGroup) setBeginLLSN(llsn types.LLSN) {
	if awg != nil {
		awg.beginLSN.LLSN = llsn
	}
}

func (awg *appendWaitGroup) writeDone(err error) {
	// NOTE: backup replica has no awg.
	if awg != nil {
		awg.wwg.done(err)
	}
}

func (awg *appendWaitGroup) commitDone(err error) {
	// NOTE: backup replica has no awg.
	if awg != nil {
		awg.commitErr = err
		awg.cwg.Done()
	}
}

func (awg *appendWaitGroup) wait(ctx context.Context) error {
	// NOTE: backup replica has no awg.
	if awg == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := awg.wwg.wait(ctx)
	if err != nil {
		return err
	}
	awg.cwg.Wait()
	return awg.commitErr
}
