package replication

import (
	"sync"

	"github.com/kakao/varlog/pkg/types"
)

var callbackBlockPool = sync.Pool{
	New: func() interface{} {
		return &callbackBlock{}
	},
}

type callbackBlock struct {
	llsn     types.LLSN
	callback func(error)
}

func newCallbackBlock(llsn types.LLSN, callback func(error)) *callbackBlock {
	t := callbackBlockPool.Get().(*callbackBlock)
	t.llsn = llsn
	t.callback = callback
	return t
}

func (t *callbackBlock) release() {
	t.llsn = types.InvalidLLSN
	t.callback = nil
	callbackBlockPool.Put(t)
}
