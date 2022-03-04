package replication

import (
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var callbackBlockPool = sync.Pool{
	New: func() interface{} {
		return &callbackBlock{}
	},
}

type callbackBlock struct {
	llsn           types.LLSN
	startTimeMicro int64
	callback       func(int64, error)
}

func newCallbackBlock(llsn types.LLSN, startTimeMicro int64, callback func(int64, error)) *callbackBlock {
	t := callbackBlockPool.Get().(*callbackBlock)
	t.llsn = llsn
	t.startTimeMicro = startTimeMicro
	t.callback = callback
	return t
}

func (t *callbackBlock) release() {
	t.llsn = types.InvalidLLSN
	t.callback = nil
	t.startTimeMicro = 0
	callbackBlockPool.Put(t)
}
