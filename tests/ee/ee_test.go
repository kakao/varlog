//go:build e2e

package ee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/tests/ee/cluster"
)

func TestVarlogAppend(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer func() {
		_ = logger.Sync()
	}()

	tc := NewCluster(t,
		cluster.NumStorageNodes(3),
		cluster.WithLogger(logger),
	)
	defer tc.Close(context.Background(), t)

	tc.Setup(context.Background(), t)

	adminAddr := tc.AdminServerAddress(context.Background(), t)
	adm, err := varlog.NewAdmin(context.Background(), adminAddr)
	require.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	td, err := adm.AddTopic(context.Background())
	require.NoError(t, err)

	lsd, err := adm.AddLogStream(context.Background(), td.TopicID, nil)
	require.NoError(t, err)

	mrAddr := tc.MetadataRepositoryAddress(context.Background(), t)

	vlg, err := varlog.Open(
		context.Background(),
		testClusterID,
		[]string{mrAddr},
		varlog.WithDenyTTL(5*time.Second),
		varlog.WithMetadataRefreshInterval(time.Second),
	)
	assert.NoError(t, err)
	defer func() {
		err := vlg.Close()
		assert.NoError(t, err)
	}()

	appendRes := vlg.Append(context.Background(), td.TopicID, [][]byte{[]byte("foo")})
	assert.NoError(t, appendRes.Err)
	glsn := appendRes.Metadata[0].GLSN

	sealRsp, err := adm.Seal(context.Background(), td.TopicID, lsd.LogStreamID)
	assert.NoError(t, err)
	assert.Equal(t, glsn, sealRsp.SealedGLSN)
	assert.Condition(t, func() bool {
		for _, lsrmd := range sealRsp.LogStreams {
			if !lsrmd.Status.Sealed() {
				return false
			}
		}
		return true
	})

	appendRes = vlg.Append(context.Background(), td.TopicID, [][]byte{[]byte("foo")})
	assert.Error(t, appendRes.Err)

	_, err = adm.Unseal(context.Background(), td.TopicID, lsd.LogStreamID)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		appendRes = vlg.Append(context.Background(), td.TopicID, [][]byte{[]byte("foo")})
		return appendRes.Err == nil && appendRes.Metadata[0].GLSN > glsn
	}, time.Minute, 10*time.Second)
}

func TestVarlogFailoverMR(t *testing.T) {
	const numMRs = 3

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer func() {
		_ = logger.Sync()
	}()

	tc := NewCluster(t,
		cluster.NumStorageNodes(3),
		cluster.NumMetadataRepositoryNodes(numMRs),
		cluster.WithLogger(logger),
	)
	defer tc.Close(context.Background(), t)

	tc.Setup(context.Background(), t)

	adminAddr := tc.AdminServerAddress(context.Background(), t)
	adm, err := varlog.NewAdmin(context.Background(), adminAddr)
	require.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	td, err := adm.AddTopic(context.Background())
	require.NoError(t, err)

	_, err = adm.AddLogStream(context.Background(), td.TopicID, nil)
	require.NoError(t, err)

	mrAddr := tc.MetadataRepositoryAddress(context.Background(), t)

	vlg, err := varlog.Open(
		context.Background(),
		testClusterID,
		[]string{mrAddr},
		varlog.WithDenyTTL(5*time.Second),
		varlog.WithMetadataRefreshInterval(time.Second),
	)
	require.NoError(t, err)
	defer func() {
		err := vlg.Close()
		assert.NoError(t, err)
	}()

	tc.SetNumMetadataRepositories(context.Background(), t, numMRs-1)

	for i := 0; i < 10; i++ {
		res := vlg.Append(context.Background(), td.TopicID, [][]byte{[]byte("foo")})
		assert.NoError(t, res.Err)
	}

	tc.SetNumMetadataRepositories(context.Background(), t, numMRs)

	for i := 0; i < 10; i++ {
		res := vlg.Append(context.Background(), td.TopicID, [][]byte{[]byte("foo")})
		assert.NoError(t, res.Err)
	}
}

func TestVarlogFailoverSN(t *testing.T) {
	const numMRs = 3

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer func() {
		_ = logger.Sync()
	}()

	tc := NewCluster(t,
		cluster.NumStorageNodes(3),
		cluster.NumMetadataRepositoryNodes(numMRs),
		cluster.WithLogger(logger),
	)
	defer tc.Close(context.Background(), t)

	tc.Setup(context.Background(), t)

	adminAddr := tc.AdminServerAddress(context.Background(), t)
	adm, err := varlog.NewAdmin(context.Background(), adminAddr)
	require.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	td, err := adm.AddTopic(context.Background())
	require.NoError(t, err)

	lsd, err := adm.AddLogStream(context.Background(), td.TopicID, nil)
	require.NoError(t, err)

	mrAddr := tc.MetadataRepositoryAddress(context.Background(), t)

	vlg, err := varlog.Open(
		context.Background(),
		testClusterID,
		[]string{mrAddr},
		varlog.WithDenyTTL(5*time.Second),
		varlog.WithMetadataRefreshInterval(time.Second),
	)
	require.NoError(t, err)
	defer func() {
		err := vlg.Close()
		assert.NoError(t, err)
	}()

	victim := lsd.Replicas[len(lsd.Replicas)-1].StorageNodeID
	nodeName := tc.StopStorageNode(context.Background(), t, victim)

	require.Eventually(t, func() bool {
		lsd, err = adm.GetLogStream(context.Background(), td.TopicID, lsd.LogStreamID)
		assert.NoError(t, err)
		return lsd.Status.Sealed()
	}, time.Minute, 10*time.Second)

	tc.StartStorageNode(context.Background(), t, nodeName)

	assert.Eventually(t, func() bool {
		_, err := adm.Unseal(context.Background(), td.TopicID, lsd.LogStreamID)
		if err != nil {
			return false
		}

		res := vlg.Append(context.Background(), td.TopicID, [][]byte{[]byte("foo")})
		return res.Err == nil
	}, time.Minute, 10*time.Second)
}

func TestVarlogEnduranceExample(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer func() {
		_ = logger.Sync()
	}()

	tc := NewCluster(t,
		cluster.WithLogger(logger),
		cluster.NumStorageNodes(6),
	)
	defer tc.Close(context.Background(), t)

	tc.Setup(context.Background(), t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		victim         types.StorageNodeID
		victimNodeName string
	)

	snFail := NewConfChanger(
		WithChangeFunc(AnyBackupSNFail(tc, &victim, &victimNodeName)),
		WithCheckFunc(WaitStorageNodeFail(tc, func() string { return victimNodeName })),
		WithConfChangeInterval(10*time.Second),
		WithLogger(logger),
	)

	recoverSN := NewConfChanger(
		WithChangeFunc(StartStorageNode(tc, func() string { return victimNodeName })),
		WithLogger(logger),
	)

	mrseed := tc.MetadataRepositoryAddress(ctx, t)
	action := NewAction(t,
		WithTitle("backup sn fail"),
		WithClusterID(testClusterID),
		WithMRAddr(mrseed),
		WithPreHook(InitLogStream(ctx, tc, 3)),
		WithConfChange(snFail),
		WithConfChange(recoverSN),
		WithNumClient(1),
		WithNumSubscriber(0),
		WithLogger(logger),
	)

	action.Do(ctx, t)
}
