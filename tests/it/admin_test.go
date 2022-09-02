package it

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestAdminUpdateLogStream(t *testing.T) {
	const (
		replicationFactor      = 3
		numInitialStorageNodes = 3
		numTopics              = 1
		numLogStreams          = 1
	)

	tcs := []struct {
		name  string
		testf func(*testing.T, *VarlogCluster, varlog.Admin, types.TopicID, types.LogStreamID, varlogpb.ReplicaDescriptor, varlogpb.ReplicaDescriptor)
	}{
		{
			name: "Succeed",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				_, err := adm.Seal(context.Background(), tpid, lsid)
				require.NoError(t, err)

				_, err = adm.UpdateLogStream(context.Background(), tpid, lsid, oldReplica, newReplica)
				require.NoError(t, err)
			},
		},
		{
			name: "AlreadyUpdated",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				_, err := adm.Seal(context.Background(), tpid, lsid)
				require.NoError(t, err)

				_, err = adm.UpdateLogStream(context.Background(), tpid, lsid, newReplica, oldReplica)
				require.NoError(t, err)
			},
		},
		{
			name: "MetadataRepositoryFailure",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				_, err := adm.Seal(context.Background(), tpid, lsid)
				require.NoError(t, err)

				clus.CloseMR(t, 0)
				time.Sleep(mrmanager.ReloadInterval * 2)

				_, err = adm.UpdateLogStream(context.Background(), tpid, lsid, oldReplica, newReplica)
				require.Error(t, err)
				require.Equal(t, codes.Unavailable, status.Convert(err).Code())
			},
		},
		{
			name: "LogStreamNotExist",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				_, err := adm.UpdateLogStream(context.Background(), tpid, lsid+1, oldReplica, newReplica)
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Convert(err).Code())
			},
		},
		{
			name: "LogStreamNotSealed",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				_, err := adm.UpdateLogStream(context.Background(), tpid, lsid, oldReplica, newReplica)
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Convert(err).Code())
			},
		},
		{
			name: "VictimReplicaNotExist",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				_, err := adm.Seal(context.Background(), tpid, lsid)
				require.NoError(t, err)

				oldReplica.StorageNodeID = clus.StorageNodeIDAtIndex(t, numInitialStorageNodes) + 1
				_, err = adm.UpdateLogStream(context.Background(), tpid, lsid, oldReplica, newReplica)
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Convert(err).Code())
			},
		},
		{
			name: "VictimAndNewReplicaAlreadyExist",
			testf: func(t *testing.T, clus *VarlogCluster, adm varlog.Admin, tpid types.TopicID, lsid types.LogStreamID, oldReplica, newReplica varlogpb.ReplicaDescriptor) {
				lsd, err := adm.GetLogStream(context.Background(), tpid, lsid)
				require.NoError(t, err)
				oldReplica.StorageNodeID = lsd.Replicas[0].StorageNodeID
				oldReplica.Path = lsd.Replicas[0].Path
				newReplica.StorageNodeID = lsd.Replicas[1].StorageNodeID
				newReplica.Path = lsd.Replicas[1].Path

				_, err = adm.Seal(context.Background(), tpid, lsid)
				require.NoError(t, err)

				_, err = adm.UpdateLogStream(context.Background(), tpid, lsid, oldReplica, newReplica)
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Convert(err).Code())
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			clus := NewVarlogCluster(t,
				WithReplicationFactor(replicationFactor),
				WithNumberOfStorageNodes(numInitialStorageNodes),
				WithNumberOfTopics(numTopics),
				WithNumberOfLogStreams(numLogStreams),
			)
			defer clus.Close(t)

			tpid := clus.TopicIDs()[0]
			lsid := clus.LogStreamID(t, tpid, 0)
			adm := clus.GetVMSClient(t)

			lsd, err := adm.GetLogStream(context.Background(), tpid, lsid)
			require.NoError(t, err)
			oldReplica := varlogpb.ReplicaDescriptor{
				StorageNodeID: lsd.Replicas[replicationFactor-1].StorageNodeID,
				Path:          lsd.Replicas[replicationFactor-1].Path,
			}

			newsnid := clus.AddSN(t)
			snm, err := adm.GetStorageNode(context.Background(), newsnid)
			require.NoError(t, err)
			newReplica := varlogpb.ReplicaDescriptor{
				StorageNodeID: newsnid,
				Path:          snm.Storages[0].Path,
			}

			tc.testf(t, clus, adm, tpid, lsid, oldReplica, newReplica)
		})
	}
}
