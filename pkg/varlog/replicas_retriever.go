package varlog

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/pkg/varlog -package varlog -destination replicas_retriever_mock.go . ReplicasRetriever,RenewableReplicasRetriever

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

var (
	errNoLogIOClient = errors.New("no such log client")
)

// ReplicasRetriever is the interface that wraps the Retrieve method.
//
// Retrieve searches replicas belongs to the log stream.
type ReplicasRetriever interface {
	Retrieve(logStreamID types.LogStreamID) ([]varlogpb.LogStreamReplicaDescriptor, bool)
	All() map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor
}

type RenewableReplicasRetriever interface {
	ReplicasRetriever
	Renewable
}

type renewableReplicasRetriever struct {
	lsreplicas atomic.Value // *sync.Map // map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor
}

func (r *renewableReplicasRetriever) Retrieve(logStreamID types.LogStreamID) ([]varlogpb.LogStreamReplicaDescriptor, bool) {
	lsReplicasMapIf := r.lsreplicas.Load()
	if lsReplicasMapIf == nil {
		return nil, false
	}
	lsReplicasMap := lsReplicasMapIf.(*sync.Map)
	if lsreplicas, ok := lsReplicasMap.Load(logStreamID); ok {
		return lsreplicas.([]varlogpb.LogStreamReplicaDescriptor), true
	}
	return nil, false
}

func (r *renewableReplicasRetriever) All() map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor {
	lsReplicasMapIf := r.lsreplicas.Load()
	if lsReplicasMapIf == nil {
		return nil
	}
	lsReplicasMap := lsReplicasMapIf.(*sync.Map)
	ret := make(map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor)
	lsReplicasMap.Range(func(logStreamID interface{}, replicas interface{}) bool {
		ret[logStreamID.(types.LogStreamID)] = replicas.([]varlogpb.LogStreamReplicaDescriptor)
		return true
	})
	return ret
}

func (r *renewableReplicasRetriever) Renew(metadata *varlogpb.MetadataDescriptor) {
	newLSReplicasMap := new(sync.Map)

	storageNodes := metadata.GetStorageNodes()
	snMap := make(map[types.StorageNodeID]string, len(storageNodes))
	for _, storageNode := range storageNodes {
		snMap[storageNode.GetStorageNodeID()] = storageNode.GetAddress()
	}

	lsdescs := metadata.GetLogStreams()
	for _, lsdesc := range lsdescs {
		logStreamID := lsdesc.GetLogStreamID()
		replicas := lsdesc.GetReplicas()
		lsreplicas := make([]varlogpb.LogStreamReplicaDescriptor, len(replicas))
		for i, replica := range replicas {
			storageNodeID := replica.GetStorageNodeID()
			lsreplicas[i].StorageNodeID = storageNodeID
			lsreplicas[i].LogStreamID = logStreamID
			lsreplicas[i].Address = snMap[storageNodeID]
		}
		newLSReplicasMap.Store(logStreamID, lsreplicas)
	}

	r.lsreplicas.Store(newLSReplicasMap)
}
