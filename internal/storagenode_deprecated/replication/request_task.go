package replication

import (
	"sync"
	"time"

	"github.com/kakao/varlog/pkg/types"
)

var requestTaskPool = sync.Pool{
	New: func() interface{} {
		return &requestTask{}
	},
}

type requestTask struct {
	topicID          types.TopicID
	logStreamID      types.LogStreamID
	llsn             types.LLSN
	data             []byte
	createdTimeMicro int64
}

func newRequestTask(topicID types.TopicID, logStreamID types.LogStreamID, llsn types.LLSN, data []byte) *requestTask {
	rt := requestTaskPool.Get().(*requestTask)
	rt.topicID = topicID
	rt.logStreamID = logStreamID
	rt.llsn = llsn
	rt.data = data
	rt.createdTimeMicro = time.Now().UnixMicro()
	return rt
}

func (rt *requestTask) release() {
	rt.topicID = types.TopicID(0)
	rt.logStreamID = types.LogStreamID(0)
	rt.llsn = types.InvalidLLSN
	rt.data = nil
	rt.createdTimeMicro = 0
	requestTaskPool.Put(rt)
}
