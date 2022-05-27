package stats_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/internal/admin/mrmanager"
	"github.daumkakao.com/varlog/varlog/internal/admin/stats"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestStats_BadClusterMetadataView(t *testing.T) {
	const lsid = types.LogStreamID(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// It could not fetch cluster metadata.
	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error")).AnyTimes()

	// It could not get a log stream stat from the repository because of invalid cluster metadata.
	repos := stats.NewRepository(context.Background(), cmview)
	lss := repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusDeleted, lss.Status())

	repos.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusRunning)
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusDeleted, lss.Status())

	repos.Report(context.Background(), &snpb.StorageNodeMetadataDescriptor{
		LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
			{
				LogStreamReplica: varlogpb.LogStreamReplica{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: 1,
						Address:       "127.0.0.1:10000",
					},
				},
				Status: varlogpb.LogStreamStatusRunning,
			},
		},
	})
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusDeleted, lss.Status())
}

func TestStats_Report(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		snid = types.StorageNodeID(1)
		lsid = types.LogStreamID(1)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Status:      varlogpb.LogStreamStatusRunning,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: snid,
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil).AnyTimes()

	repos := stats.NewRepository(context.Background(), cmview)
	lss := repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lss.Status())

	// LogStreamStatusRunning -> LogStreamStatusSealed
	repos.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusSealed)
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss.Status())

	repos.Report(context.Background(), &snpb.StorageNodeMetadataDescriptor{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
			Address:       "127.0.0.1:10000",
		},
		LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
			{
				LogStreamReplica: varlogpb.LogStreamReplica{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
						Address:       "127.0.0.1:10000",
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
				Status: varlogpb.LogStreamStatusRunning,
			},
		},
	})
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss.Status())
	lsrmd, ok := lss.Replica(snid)
	assert.True(t, ok)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lsrmd.Status)
}

func TestStats_UpdatedClusterMetadata(t *testing.T) {
	const (
		tpid = types.TopicID(1)
		snid = types.StorageNodeID(1)
		lsid = types.LogStreamID(1)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := mrmanager.NewMockClusterMetadataView(ctrl)

	// empty cluster metadata
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{}, nil)

	repos := stats.NewRepository(context.Background(), cmview)

	lss := repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusDeleted, lss.Status())

	// updated cluster metadata
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Status:      varlogpb.LogStreamStatusRunning,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: snid,
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil)
	repos.Report(context.Background(), &snpb.StorageNodeMetadataDescriptor{
		LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
			{
				LogStreamReplica: varlogpb.LogStreamReplica{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
						Address:       "127.0.0.1:10000",
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
				Status: varlogpb.LogStreamStatusRunning,
			},
		},
	})
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lss.Status())

	// Updated cluster metadata does not affect the status of LogStreamStat.
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Status:      varlogpb.LogStreamStatusSealed,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: snid,
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil)
	repos.Report(context.Background(), &snpb.StorageNodeMetadataDescriptor{
		LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
			{
				LogStreamReplica: varlogpb.LogStreamReplica{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
						Address:       "127.0.0.1:10000",
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
				Status: varlogpb.LogStreamStatusSealing,
			},
		},
	})
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lss.Status())
	lsrmd, ok := lss.Replica(snid)
	assert.True(t, ok)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)

	repos.SetLogStreamStatus(lsid, varlogpb.LogStreamStatusSealed)
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss.Status())

	// Updated cluster metadata, which is changes replica, resets replica
	// status to LogStreamStatusRunning.
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     tpid,
				LogStreamID: lsid,
				Status:      varlogpb.LogStreamStatusRunning,
				Replicas: []*varlogpb.ReplicaDescriptor{
					{
						StorageNodeID: snid + 1,
						Path:          "/tmp",
					},
				},
			},
		},
	}, nil)
	repos.Report(context.Background(), &snpb.StorageNodeMetadataDescriptor{
		LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
			{
				LogStreamReplica: varlogpb.LogStreamReplica{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid + 1,
						Address:       "127.0.0.1:10001",
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
				Status: varlogpb.LogStreamStatusRunning,
			},
		},
	})
	lss = repos.GetLogStream(lsid)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lss.Status())
	replicas := lss.Replicas()
	assert.Contains(t, replicas, snid+1)
	assert.NotContains(t, replicas, snid)
}
