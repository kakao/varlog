package executor

import (
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var taskWaitGroupPool = sync.Pool{
	New: func() interface{} {
		t := &taskWaitGroup{}
		return t
	},
}

// taskWaitGroup is to wait for the commit of the log in Append RPC.
type taskWaitGroup struct {
	glsn types.GLSN
	llsn types.LLSN
	wg   sync.WaitGroup
	err  error

	createdTime   time.Time
	committedTime time.Time
}

func newTaskWaitGroup() *taskWaitGroup {
	t := taskWaitGroupPool.Get().(*taskWaitGroup)
	t.wg.Add(1)
	t.createdTime = time.Now()
	return t
}

func (twg *taskWaitGroup) release() {
	twg.glsn = types.InvalidGLSN
	twg.llsn = types.InvalidLLSN
	twg.err = nil
	twg.createdTime = time.Time{}
	twg.committedTime = time.Time{}
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
