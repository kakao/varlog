package vms

import (
	"context"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type StatRepository interface {
	Refresh()

	Report(*varlogpb.StorageNodeMetadataDescriptor)

	GetLogStream(types.LogStreamID) map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor

	GetStorageNode(types.StorageNodeID) *varlogpb.StorageNodeDescriptor

	GetAppliedIndex() uint64
}

type statRepository struct {
	cmView       ClusterMetadataView
	appliedIndex uint64
	logStreams   map[types.LogStreamID]map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor
	storageNodes map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor
	mu           sync.RWMutex
}

var _ StatRepository = (*statRepository)(nil)

func NewStatRepository(cmView ClusterMetadataView) StatRepository {
	s := &statRepository{
		cmView:       cmView,
		logStreams:   make(map[types.LogStreamID]map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor),
		storageNodes: make(map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor),
	}

	s.Refresh()
	return s
}

func (s *statRepository) Refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	meta, err := s.cmView.ClusterMetadata(ctx)
	if err != nil || meta == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.appliedIndex >= meta.AppliedIndex {
		return
	}

	logStreams := make(map[types.LogStreamID]map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor)
	storageNodes := make(map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor)

	for _, sn := range meta.GetStorageNodes() {
		if existsn, ok := s.storageNodes[sn.StorageNodeID]; ok {
			storageNodes[sn.StorageNodeID] = existsn
		} else {
			storageNodes[sn.StorageNodeID] = sn
		}
	}

	for _, ls := range meta.GetLogStreams() {
		replicas := make(map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor)
		if existls, ok := s.logStreams[ls.LogStreamID]; ok {
			for _, r := range ls.GetReplicas() {
				if existr, ok := existls[r.StorageNodeID]; ok {
					replicas[r.StorageNodeID] = existr
				} else {
					replicas[r.StorageNodeID] = varlogpb.LogStreamMetadataDescriptor{
						LogStreamID: ls.LogStreamID,
						Path:        r.Path,
					}
				}
			}
		} else {
			for _, r := range ls.GetReplicas() {
				replicas[r.StorageNodeID] = varlogpb.LogStreamMetadataDescriptor{
					LogStreamID: ls.LogStreamID,
					Path:        r.Path,
				}
			}
		}
		logStreams[ls.LogStreamID] = replicas
	}

	s.logStreams = logStreams
	s.storageNodes = storageNodes
	s.appliedIndex = meta.AppliedIndex
}

func (s *statRepository) Report(r *varlogpb.StorageNodeMetadataDescriptor) {
	s.Refresh()

	s.mu.Lock()
	defer s.mu.Unlock()

	sn := r.GetStorageNode()
	if sn == nil {
		return
	}

	snID := sn.StorageNodeID
	if _, ok := s.storageNodes[snID]; !ok {
		return
	}

	s.storageNodes[snID] = sn

	for _, ls := range r.GetLogStreams() {
		replicas, ok := s.logStreams[ls.LogStreamID]
		if !ok {
			continue
		}

		_, ok = replicas[snID]
		if !ok {
			continue
		}

		replicas[snID] = ls
		s.logStreams[ls.LogStreamID] = replicas
	}
}

func (s *statRepository) GetLogStream(lsID types.LogStreamID) map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ls, _ := s.logStreams[lsID]

	return ls
}

func (s *statRepository) GetStorageNode(snID types.StorageNodeID) *varlogpb.StorageNodeDescriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sn, _ := s.storageNodes[snID]

	return sn
}

func (s *statRepository) GetAppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.appliedIndex
}
