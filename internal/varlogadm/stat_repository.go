package varlogadm

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type StatRepository interface {
	Refresh(context.Context)

	Report(context.Context, *snpb.StorageNodeMetadataDescriptor)

	GetLogStream(types.LogStreamID) *LogStreamStat

	GetStorageNode(types.StorageNodeID) *varlogpb.StorageNodeDescriptor

	SetLogStreamStatus(types.LogStreamID, varlogpb.LogStreamStatus)

	GetAppliedIndex() uint64
}

type statRepository struct {
	cmView       ClusterMetadataView
	meta         *varlogpb.MetadataDescriptor
	appliedIndex uint64
	logStreams   map[types.LogStreamID]*LogStreamStat
	storageNodes map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor
	mu           sync.RWMutex
}

type LogStreamStat struct {
	status   varlogpb.LogStreamStatus
	replicas map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor
	mu       sync.RWMutex
}

func (lss *LogStreamStat) Status() varlogpb.LogStreamStatus {
	lss.mu.RLock()
	defer lss.mu.RUnlock()
	return lss.status
}

func (lss *LogStreamStat) Replicas() map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor {
	lss.mu.RLock()
	defer lss.mu.RUnlock()
	return lss.replicasInternal()
}

func (lss *LogStreamStat) Replica(storageNodeID types.StorageNodeID) (snpb.LogStreamReplicaMetadataDescriptor, bool) {
	lss.mu.RLock()
	defer lss.mu.RUnlock()
	lsmd, ok := lss.replicas[storageNodeID]
	return lsmd, ok
}

func (lss *LogStreamStat) Copy() LogStreamStat {
	lss.mu.RLock()
	defer lss.mu.RUnlock()
	return LogStreamStat{
		status:   lss.status,
		replicas: lss.replicasInternal(),
	}
}

func (lss *LogStreamStat) replicasInternal() map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor {
	replicas := make(map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor, len(lss.replicas))
	for snid, lsmd := range lss.replicas {
		replicas[snid] = lsmd
	}
	return replicas
}

var _ StatRepository = (*statRepository)(nil)

func NewStatRepository(ctx context.Context, cmView ClusterMetadataView) StatRepository {
	s := &statRepository{
		cmView:       cmView,
		logStreams:   make(map[types.LogStreamID]*LogStreamStat),
		storageNodes: make(map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor),
	}

	s.Refresh(ctx)
	return s
}

func (s *statRepository) Refresh(ctx context.Context) {
	meta, err := s.cmView.ClusterMetadata(ctx)
	if err != nil || meta == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.meta.Equal(meta) {
		return
	}

	logStreams := make(map[types.LogStreamID]*LogStreamStat)
	storageNodes := make(map[types.StorageNodeID]*varlogpb.StorageNodeDescriptor)

	for _, sn := range meta.GetStorageNodes() {
		if existsn, ok := s.storageNodes[sn.StorageNodeID]; ok {
			storageNodes[sn.StorageNodeID] = existsn
		} else {
			storageNodes[sn.StorageNodeID] = sn
		}
	}

	for _, ls := range meta.GetLogStreams() {
		lsStat := &LogStreamStat{
			replicas: make(map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor),
		}
		lsStat.mu.Lock()
		if existls, ok := s.logStreams[ls.LogStreamID]; ok {
			lsStat.status = existls.Status()

			for _, r := range ls.GetReplicas() {
				if existr, ok := existls.Replica(r.StorageNodeID); ok {
					lsStat.replicas[r.StorageNodeID] = existr
				} else {
					lsStat.replicas[r.StorageNodeID] = snpb.LogStreamReplicaMetadataDescriptor{
						LogStreamReplica: varlogpb.LogStreamReplica{
							TopicLogStream: varlogpb.TopicLogStream{
								LogStreamID: ls.LogStreamID,
							},
						},
						Path: r.Path,
					}
					// To reset the status of the log stream, set it as LogStreamStatusRunning
					lsStat.status = varlogpb.LogStreamStatusRunning
				}
			}
		} else {
			for _, r := range ls.GetReplicas() {
				lsStat.replicas[r.StorageNodeID] = snpb.LogStreamReplicaMetadataDescriptor{
					LogStreamReplica: varlogpb.LogStreamReplica{
						TopicLogStream: varlogpb.TopicLogStream{
							LogStreamID: ls.LogStreamID,
						},
					},
					Path: r.Path,
				}
			}
		}
		logStreams[ls.LogStreamID] = lsStat
		lsStat.mu.Unlock()
	}

	s.logStreams = logStreams
	s.storageNodes = storageNodes
	s.appliedIndex = meta.AppliedIndex
	s.meta = proto.Clone(meta).(*varlogpb.MetadataDescriptor)
}

func (s *statRepository) Report(ctx context.Context, r *snpb.StorageNodeMetadataDescriptor) {
	s.Refresh(ctx)

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

	for _, ls := range r.GetLogStreamReplicas() {
		lsStat, ok := s.logStreams[ls.LogStreamID]
		if !ok {
			continue
		}

		lsStat.mu.Lock()
		if _, ok = lsStat.replicas[snID]; ok {
			lsStat.replicas[snID] = ls
			s.logStreams[ls.LogStreamID] = lsStat
		}
		lsStat.mu.Unlock()
	}
}

func (s *statRepository) GetLogStream(lsID types.LogStreamID) *LogStreamStat {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lsStat, ok := s.logStreams[lsID]; ok {
		return lsStat
	}

	return &LogStreamStat{
		status:   varlogpb.LogStreamStatusDeleted,
		replicas: nil,
	}
}

func (s *statRepository) GetStorageNode(snID types.StorageNodeID) *varlogpb.StorageNodeDescriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.storageNodes[snID]
}

func (s *statRepository) GetAppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.appliedIndex
}

func (s *statRepository) SetLogStreamStatus(lsID types.LogStreamID, status varlogpb.LogStreamStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lsStat, ok := s.logStreams[lsID]; ok {
		lsStat.mu.Lock()
		lsStat.status = status
		lsStat.mu.Unlock()
		s.logStreams[lsID] = lsStat
	}
}
