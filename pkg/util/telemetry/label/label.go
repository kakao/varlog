package label

import (
	"go.opentelemetry.io/otel/label"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type (
	Key      = label.Key
	Value    = label.Value
	KeyValue = label.KeyValue
)

const (
	storageNodeIDLabelKey       = Key("varlog.storagenode.id")
	logStreamIDLabelKey         = Key("varlog.logstream.id")
	metadataRepositoryNodeIDKey = Key("varlog.metadatarepository.id")
	rpcNameKey                  = Key("rpc")
)

func Int64(k string, v int64) KeyValue {
	return Key(k).Int64(v)
}

func Float64(k string, v float64) KeyValue {
	return Key(k).Float64(v)
}

func String(k string, v string) KeyValue {
	return Key(k).String(v)
}

func StorageNodeIDLabel(storageNodeID types.StorageNodeID) KeyValue {
	return storageNodeIDLabelKey.Uint32(uint32(storageNodeID))
}

func LogStreamIDLabel(logStreamID types.LogStreamID) KeyValue {
	return logStreamIDLabelKey.Uint32(uint32(logStreamID))
}

func MetadataRepositoryNodeID(nodeID types.NodeID) KeyValue {
	return metadataRepositoryNodeIDKey.Uint64(uint64(nodeID))
}

func RPCName(name string) KeyValue {
	return rpcNameKey.String(name)
}
