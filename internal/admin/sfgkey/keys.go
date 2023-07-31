package sfgkey

import (
	"github.com/kakao/varlog/pkg/types"
)

const (
	delimiter           = "_"
	keyGetStorageNode   = "getsn"
	keyListStorageNodes = "listsns"
	keyGetTopic         = "gettp"
	keyListTopics       = "listtps"
	keyGetLogStream     = "getls"
	keyListLogStreams   = "listlss"
	keyDescribeTopic    = "desctp"
)

func GetStorageNodeKey(snid types.StorageNodeID) string {
	return keyGetStorageNode + delimiter + snid.String()
}

func ListStorageNodesKey() string {
	return keyListStorageNodes
}

func GetTopicKey(tpid types.TopicID) string {
	return keyGetTopic + delimiter + tpid.String()
}

func ListTopicsKey() string {
	return keyListTopics
}

func GetLogStreamKey(tpid types.TopicID, lsid types.LogStreamID) string {
	return keyGetLogStream + delimiter + tpid.String() + delimiter + lsid.String()
}

func ListLogStreamsKey(tpid types.TopicID) string {
	return keyListLogStreams + delimiter + tpid.String()
}

func DescribeTopicKey(tpid types.TopicID) string {
	return keyDescribeTopic + delimiter + tpid.String()
}
