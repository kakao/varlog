package it

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
)

func TestVarlogRegisterStorageNode(t *testing.T) {
	env := NewVarlogCluster(t,
		WithNumberOfStorageNodes(1),
		WithNumberOfLogStreams(1),
	)
	defer env.Close(t)

	// TODO (jun): use VMS to get metadata
	mr := env.GetMR(t)
	meta, err := mr.GetMetadata(context.TODO())
	require.NoError(t, err)

	info, err := mr.GetClusterInfo(context.TODO(), env.ClusterID())
	require.NoError(t, err)
	require.Contains(t, info.GetMembers(), env.MetadataRepositoryIDAt(t, 0))

	mcl, err := mrc.NewMetadataRepositoryManagementClient(context.Background(), env.MRRPCEndpointAtIndex(t, 0))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, mcl.Close())
	}()
	rsp, err := mcl.GetClusterInfo(context.Background(), env.ClusterID())
	require.NoError(t, err)
	require.Equal(t, rsp.GetClusterInfo().GetLeader(), env.MetadataRepositoryIDAt(t, 0))
	require.Contains(t, rsp.GetClusterInfo().GetMembers(), env.MetadataRepositoryIDAt(t, 0))

	require.Len(t, meta.GetStorageNodes(), 1)
	require.Len(t, meta.GetLogStreams(), 1)
	require.Eventually(t, func() bool {
		return mr.GetReportCount() > 0
	}, time.Second, 10*time.Millisecond)
}
