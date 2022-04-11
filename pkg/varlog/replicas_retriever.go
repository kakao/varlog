package varlog

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/varlog -package varlog -destination replicas_retriever_mock.go . ReplicasRetriever,RenewableReplicasRetriever

import (
	"errors"
	"sync/atomic"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

var (
	errNoLogIOClient = errors.New("no such log client")
)

// ReplicasRetriever is the interface that wraps the Retrieve method.
//
// Retrieve searches replicas belongs to the log stream.
type ReplicasRetriever interface {
	Retrieve(topicID types.TopicID, logStreamID types.LogStreamID) ([]varlogpb.LogStreamReplica, bool)
	All(topicID types.TopicID) map[types.LogStreamID][]varlogpb.LogStreamReplica
}

type RenewableReplicasRetriever interface {
	ReplicasRetriever
	Renewable
}

type renewableReplicasRetriever struct {
	topic atomic.Value // map[types.TopicID]map[types.LogStreamID][]varlogpb.LogStreamReplica
}

func (r *renewableReplicasRetriever) Retrieve(topicID types.TopicID, logStreamID types.LogStreamID) ([]varlogpb.LogStreamReplica, bool) {
	topicMapIf := r.topic.Load()
	if topicMapIf == nil {
		return nil, false
	}
	topicMap := topicMapIf.(map[types.TopicID]map[types.LogStreamID][]varlogpb.LogStreamReplica)
	if lsReplicasMap, ok := topicMap[topicID]; ok {
		if lsreplicas, ok := lsReplicasMap[logStreamID]; ok {
			return lsreplicas, true
		}
	}
	return nil, false
}

func (r *renewableReplicasRetriever) All(topicID types.TopicID) map[types.LogStreamID][]varlogpb.LogStreamReplica {
	topicMapIf := r.topic.Load()
	if topicMapIf == nil {
		return nil
	}
	topicMap := topicMapIf.(map[types.TopicID]map[types.LogStreamID][]varlogpb.LogStreamReplica)

	lsReplicasMap, ok := topicMap[topicID]
	if !ok {
		return nil
	}

	ret := make(map[types.LogStreamID][]varlogpb.LogStreamReplica)
	for lsID, replicas := range lsReplicasMap {
		ret[lsID] = replicas
	}
	return ret
}

func (r *renewableReplicasRetriever) Renew(metadata *varlogpb.MetadataDescriptor) {
	storageNodes := metadata.GetStorageNodes()
	snMap := make(map[types.StorageNodeID]string, len(storageNodes))
	for _, storageNode := range storageNodes {
		snMap[storageNode.GetStorageNodeID()] = storageNode.GetAddress()
	}

	newTopicMap := make(map[types.TopicID]map[types.LogStreamID][]varlogpb.LogStreamReplica)
	topicdescs := metadata.GetTopics()
	for _, topicdesc := range topicdescs {
		topicID := topicdesc.TopicID

		newLSReplicasMap := make(map[types.LogStreamID][]varlogpb.LogStreamReplica)
		for _, lsid := range topicdesc.LogStreams {
			lsdesc := metadata.GetLogStream(lsid)

			logStreamID := lsdesc.GetLogStreamID()
			replicas := lsdesc.GetReplicas()
			lsreplicas := make([]varlogpb.LogStreamReplica, len(replicas))
			for i, replica := range replicas {
				storageNodeID := replica.GetStorageNodeID()
				lsreplicas[i].StorageNodeID = storageNodeID
				lsreplicas[i].LogStreamID = logStreamID
				lsreplicas[i].Address = snMap[storageNodeID]
			}
			newLSReplicasMap[logStreamID] = lsreplicas
		}

		newTopicMap[topicID] = newLSReplicasMap
	}

	r.topic.Store(newTopicMap)
}
