package executorsmap

import "github.daumkakao.com/varlog/varlog/pkg/types"

const halfBits = 32

type logStreamTopicID int64

func packLogStreamTopicID(lsid types.LogStreamID, tpid types.TopicID) logStreamTopicID {
	return logStreamTopicID(int64(lsid)<<halfBits | int64(tpid))
}

func (tpls logStreamTopicID) unpack() (lsid types.LogStreamID, tpid types.TopicID) {
	const mask = 0xffffffff
	n := int64(tpls)
	lsid = types.LogStreamID((n >> halfBits) & mask)
	tpid = types.TopicID(n & mask)
	return lsid, tpid
}
