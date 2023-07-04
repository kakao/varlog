package telemetry

import (
	"go.opentelemetry.io/otel/attribute"

	"github.com/kakao/varlog/pkg/types"
)

const (
	ClusterIDKey   = attribute.Key("varlog.cluster.id")
	TopicIDKey     = attribute.Key("varlog.topic.id")
	LogStreamIDKey = attribute.Key("varlog.logstream.id")
)

func ClusterID(val types.ClusterID) attribute.KeyValue {
	return ClusterIDKey.Int(int(val))
}

func TopicID(val types.TopicID) attribute.KeyValue {
	return TopicIDKey.Int(int(val))
}

func LogStreamID(val types.LogStreamID) attribute.KeyValue {
	return LogStreamIDKey.Int(int(val))
}
