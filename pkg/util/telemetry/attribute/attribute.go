package attribute

import (
	"go.opentelemetry.io/otel/attribute"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type (
	Key      = attribute.Key
	Value    = attribute.Value
	KeyValue = attribute.KeyValue
)

const (
	storageNodeIDattributeKey   = Key("varlog.storagenode.id")
	logStreamIDattributeKey     = Key("varlog.logstream.id")
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

func StorageNodeID(storageNodeID types.StorageNodeID) KeyValue {
	return storageNodeIDattributeKey.String(storageNodeID.String())
}

func LogStreamID(logStreamID types.LogStreamID) KeyValue {
	return logStreamIDattributeKey.String(logStreamID.String())
}

func MetadataRepositoryNodeID(nodeID types.NodeID) KeyValue {
	return metadataRepositoryNodeIDKey.String(nodeID.String())
}

func RPCName(name string) KeyValue {
	return rpcNameKey.String(name)
}
