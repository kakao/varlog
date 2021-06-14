package executor

import (
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var taskWaitGroupPool = sync.Pool{
	New: func() interface{} {
		t := &taskWaitGroup{}
		return t
	},
}

type taskWaitGroup struct {
	glsn types.GLSN
	wg   sync.WaitGroup
	err  error
}

func newTaskWaitGroup() *taskWaitGroup {
	t := taskWaitGroupPool.Get().(*taskWaitGroup)
	t.wg.Add(1)
	return t
}

func (twg *taskWaitGroup) release() {
	twg.glsn = types.InvalidGLSN
	twg.err = nil
	taskWaitGroupPool.Put(twg)
}

func (twg *taskWaitGroup) done(err error) {
	// If twg is nil, this method does nothing. If twg is nil, this method does nothing. In case
	// of commitWaitTask of backup replica has nil of twg.
	if twg == nil {
		return
	}
	twg.err = err
	twg.wg.Done()
}
