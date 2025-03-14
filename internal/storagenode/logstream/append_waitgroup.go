package logstream

import (
	"context"
	"sync"

	"github.com/kakao/varlog/pkg/types"
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
	glsn      types.GLSN
	llsn      types.LLSN
	wwg       *writeWaitGroup
	cwg       sync.WaitGroup
	commitErr error
}

func newAppendWaitGroup(wwg *writeWaitGroup) *appendWaitGroup {
	awg := appendWaitGroupPool.Get().(*appendWaitGroup)
	awg.wwg = wwg
	awg.cwg.Add(1)
	return awg
}

func (awg *appendWaitGroup) release() {
	awg.glsn = types.InvalidGLSN
	awg.llsn = types.InvalidLLSN
	awg.wwg = nil
	awg.cwg = sync.WaitGroup{}
	appendWaitGroupPool.Put(awg)
}

func (awg *appendWaitGroup) setGLSN(glsn types.GLSN) {
	if awg != nil {
		awg.glsn = glsn
	}
}

func (awg *appendWaitGroup) setLLSN(llsn types.LLSN) {
	if awg != nil {
		awg.llsn = llsn
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
