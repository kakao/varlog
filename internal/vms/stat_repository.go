package vms

import (
	"context"
	"sync"
	"time"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

type StatRepository interface {
	Refresh()

	Report(*varlogpb.StorageNodeMetadataDescriptor)

	GetLogStream(types.LogStreamID) LogStreamStat

	GetStorageNode(types.StorageNodeID) *varlogpb.StorageNodeDescriptor

	SetLogStreamStatus(types.LogStreamID, varlogpb.LogStreamStatus)

	GetAppliedIndex() uint64
}

type statRepository struct {
	cmView       ClusterMetadataView
	appliedIndex uint64
	logStreams   map[types.LogStreamID]LogStreamStat
	storageNodes map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor
	mu           sync.RWMutex
}

type LogStreamStat struct {
	Status   varlogpb.LogStreamStatus
	Replicas map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor
}

var _ StatRepository = (*statRepository)(nil)

func NewStatRepository(cmView ClusterMetadataView) StatRepository {
	s := &statRepository{
		cmView:       cmView,
		logStreams:   make(map[types.LogStreamID]LogStreamStat),
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

	logStreams := make(map[types.LogStreamID]LogStreamStat)
	storageNodes := make(map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor)

	for _, sn := range meta.GetStorageNodes() {
		if existsn, ok := s.storageNodes[sn.StorageNodeID]; ok {
			storageNodes[sn.StorageNodeID] = existsn
		} else {
			storageNodes[sn.StorageNodeID] = sn
		}
	}

	for _, ls := range meta.GetLogStreams() {
		lsStat := LogStreamStat{
			Replicas: make(map[types.StorageNodeID]varlogpb.LogStreamMetadataDescriptor),
		}
		if existls, ok := s.logStreams[ls.LogStreamID]; ok {
			lsStat.Status = existls.Status

			for _, r := range ls.GetReplicas() {
				if existr, ok := existls.Replicas[r.StorageNodeID]; ok {
					lsStat.Replicas[r.StorageNodeID] = existr
				} else {
					lsStat.Replicas[r.StorageNodeID] = varlogpb.LogStreamMetadataDescriptor{
						LogStreamID: ls.LogStreamID,
						Path:        r.Path,
					}
				}
			}
		} else {
			for _, r := range ls.GetReplicas() {
				lsStat.Replicas[r.StorageNodeID] = varlogpb.LogStreamMetadataDescriptor{
					LogStreamID: ls.LogStreamID,
					Path:        r.Path,
				}
			}
		}
		logStreams[ls.LogStreamID] = lsStat
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
		lsStat, ok := s.logStreams[ls.LogStreamID]
		if !ok {
			continue
		}

		_, ok = lsStat.Replicas[snID]
		if !ok {
			continue
		}

		lsStat.Replicas[snID] = ls
		s.logStreams[ls.LogStreamID] = lsStat
	}
}

func (s *statRepository) GetLogStream(lsID types.LogStreamID) LogStreamStat {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if ls, ok := s.logStreams[lsID]; ok {
		return ls
	}

	return LogStreamStat{
		Status:   varlogpb.LogStreamStatusDeleted,
		Replicas: nil,
	}
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

func (s *statRepository) SetLogStreamStatus(lsID types.LogStreamID, status varlogpb.LogStreamStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ls, ok := s.logStreams[lsID]; ok {
		ls.Status = status
		s.logStreams[lsID] = ls
	}
}
