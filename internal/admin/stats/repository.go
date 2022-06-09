package stats

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/admin/stats -package stats -destination repository_mock.go . Repository

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"

	"github.daumkakao.com/varlog/varlog/internal/admin/mrmanager"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// Repository is a repository to maintain statistics of log streams to manage
// the cluster.
type Repository interface {
	// Report receives metadata of the storage node denoted by the argument
	// snmd and updates statistics using it.
	// If a log stream metadata not included in the cluster metadata is
	// reported, it is ignored.
	Report(ctx context.Context, snmd *snpb.StorageNodeMetadataDescriptor)

	// SetLogStreamStatus sets status of the log stream specified by the
	// argument lsid.
	SetLogStreamStatus(lsid types.LogStreamID, status varlogpb.LogStreamStatus)

	// GetLogStream returns statistics of the log stream specified by the
	// argument lsid.
	GetLogStream(lsid types.LogStreamID) *LogStreamStat
}

type repository struct {
	cmview mrmanager.ClusterMetadataView

	meta           *varlogpb.MetadataDescriptor
	logStreamStats map[types.LogStreamID]*LogStreamStat
	mu             sync.RWMutex
}

var _ Repository = (*repository)(nil)

func NewRepository(ctx context.Context, cmview mrmanager.ClusterMetadataView) Repository {
	s := &repository{
		cmview:         cmview,
		logStreamStats: make(map[types.LogStreamID]*LogStreamStat),
	}

	// TODO: Initializing stats repository only by using cluster metadata
	// is not sufficient to fill log stream stats completely.
	// Should we initialize this eagerly by using both cluster metadata and
	// fetching metadata of all storage nodes, otherwise lazily?
	s.refresh(ctx)
	return s
}

func (s *repository) Report(ctx context.Context, snmd *snpb.StorageNodeMetadataDescriptor) {
	s.refresh(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range snmd.LogStreamReplicas {
		snid := snmd.LogStreamReplicas[i].StorageNodeID
		lsid := snmd.LogStreamReplicas[i].LogStreamID

		lsd := s.meta.GetLogStream(lsid)
		if lsd == nil {
			// It may be zomebie log stream.
			continue
		}
		if !lsd.IsReplica(snid) {
			// It seems to be not registered yet or unregistered.
			continue
		}

		if lss, ok := s.logStreamStats[lsid]; ok {
			lss.setReplica(snid, snmd.LogStreamReplicas[i])
			s.logStreamStats[lsid] = lss
		}
	}
}

func (s *repository) GetLogStream(lsid types.LogStreamID) *LogStreamStat {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lsStat, ok := s.logStreamStats[lsid]; ok {
		return lsStat
	}

	return &LogStreamStat{
		status:   varlogpb.LogStreamStatusDeleted,
		replicas: nil,
	}
}

func (s *repository) SetLogStreamStatus(lsid types.LogStreamID, status varlogpb.LogStreamStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lss, ok := s.logStreamStats[lsid]; ok {
		lss.setStatus(status)
		s.logStreamStats[lsid] = lss
	}
}

func (s *repository) refresh(ctx context.Context) {
	meta, err := s.cmview.ClusterMetadata(ctx)
	if err != nil || meta == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.meta.Equal(meta) {
		return
	}

	logStreams := make(map[types.LogStreamID]*LogStreamStat, len(meta.LogStreams))

	for _, lsd := range meta.GetLogStreams() {
		newlss := &LogStreamStat{
			replicas: make(map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor, len(lsd.Replicas)),
		}
		logStreams[lsd.LogStreamID] = newlss

		oldlss, ok := s.logStreamStats[lsd.LogStreamID]
		if !ok {
			for _, rd := range lsd.GetReplicas() {
				newlss.replicas[rd.StorageNodeID] = snpb.LogStreamReplicaMetadataDescriptor{
					LogStreamReplica: varlogpb.LogStreamReplica{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: rd.StorageNodeID,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     lsd.TopicID,
							LogStreamID: lsd.LogStreamID,
						},
					},
					Path: rd.Path,
				}
			}
			continue
		}

		newlss.status = oldlss.Status()
		for _, rd := range lsd.GetReplicas() {
			if old, ok := oldlss.Replica(rd.StorageNodeID); ok {
				newlss.replicas[rd.StorageNodeID] = old
				continue
			}
			newlss.replicas[rd.StorageNodeID] = snpb.LogStreamReplicaMetadataDescriptor{
				LogStreamReplica: varlogpb.LogStreamReplica{
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     lsd.TopicID,
						LogStreamID: lsd.LogStreamID,
					},
				},
				Path: rd.Path,
			}
			// To reset the status of the log stream, set it as
			// LogStreamStatusRunning
			newlss.status = varlogpb.LogStreamStatusRunning
		}
	}

	s.logStreamStats = logStreams
	s.meta = proto.Clone(meta).(*varlogpb.MetadataDescriptor)
}

// LogStreamStat represents the aggregated information of the log stream that
// is collected by Repository.
type LogStreamStat struct {
	// status is the guessed status of the log stream.
	// It is updated under the two conditions:
	//  - It is updated through the method
	//  `(*repository).SetLogStreamStatus`.
	//  - It is reset to `varlogpb.LogStreamStatusRunning` by
	//  `(*repository).refresh` since guessing the status is difficult -
	//  the log stream of status `varlogpb.LogStreamStatusRunning` will be
	//  updated by the storage node watcher.
	status   varlogpb.LogStreamStatus
	replicas map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor
	mu       sync.Mutex
}

func NewLogStreamStat(status varlogpb.LogStreamStatus, replicas map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor) *LogStreamStat {
	return &LogStreamStat{
		status:   status,
		replicas: replicas,
	}
}

// Status returns status of the log stream.
func (lss *LogStreamStat) Status() varlogpb.LogStreamStatus {
	lss.mu.Lock()
	defer lss.mu.Unlock()
	return lss.status
}

// setStatus sets new status to the log stream.
func (lss *LogStreamStat) setStatus(status varlogpb.LogStreamStatus) {
	lss.mu.Lock()
	defer lss.mu.Unlock()
	lss.status = status
}

// Replicas returns replicas of the log stream.
func (lss *LogStreamStat) Replicas() map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor {
	lss.mu.Lock()
	defer lss.mu.Unlock()
	return lss.copyReplicas()
}

// Replica returns the replica stored by the storage node that is identified by
// the argument snid.
func (lss *LogStreamStat) Replica(snid types.StorageNodeID) (snpb.LogStreamReplicaMetadataDescriptor, bool) {
	lss.mu.Lock()
	defer lss.mu.Unlock()
	lsmd, ok := lss.replicas[snid]
	return lsmd, ok
}

// setReplica sets the metadata, which is denoted by the argument lsrmd, of log
// stream stored by the storage node that is identified by the argument snid.
func (lss *LogStreamStat) setReplica(snid types.StorageNodeID, lsrmd snpb.LogStreamReplicaMetadataDescriptor) {
	lss.mu.Lock()
	defer lss.mu.Unlock()
	lss.replicas[snid] = lsrmd
}

// Copy returns clone of the log stream statistics.
func (lss *LogStreamStat) Copy() LogStreamStat {
	lss.mu.Lock()
	defer lss.mu.Unlock()
	return LogStreamStat{
		status:   lss.status,
		replicas: lss.copyReplicas(),
	}
}

func (lss *LogStreamStat) copyReplicas() map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor {
	replicas := make(map[types.StorageNodeID]snpb.LogStreamReplicaMetadataDescriptor, len(lss.replicas))
	for snid, lsrmd := range lss.replicas {
		replicas[snid] = lsrmd
	}
	return replicas
}
