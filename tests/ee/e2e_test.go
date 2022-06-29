//go:build e2e

package ee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/tests/ee/k8s/client"
	"github.daumkakao.com/varlog/varlog/tests/ee/k8s/cluster"
	"github.daumkakao.com/varlog/varlog/tests/ee/k8s/vault"
)

func TestVarlogAppend(t *testing.T) {
	connInfo := vault.GetClusterConnectionInfo(t)
	kc, err := client.NewClient(
		client.WithClusterConnectionInfo(connInfo),
	)
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	tc := cluster.NewTestCluster(t,
		cluster.WithClient(kc),
		cluster.NumStorageNodes(3),
		cluster.WithLogger(logger),
	)
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

	connInfo := vault.GetClusterConnectionInfo(t)
	kc, err := client.NewClient(
		client.WithClusterConnectionInfo(connInfo),
	)
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	tc := cluster.NewTestCluster(t,
		cluster.WithClient(kc),
		cluster.NumStorageNodes(3),
		cluster.NumMetadataRepositoryNodes(numMRs),
		cluster.WithLogger(logger),
	)
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

	connInfo := vault.GetClusterConnectionInfo(t)
	kc, err := client.NewClient(
		client.WithClusterConnectionInfo(connInfo),
	)
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	tc := cluster.NewTestCluster(t,
		cluster.WithClient(kc),
		cluster.NumStorageNodes(3),
		cluster.NumMetadataRepositoryNodes(numMRs),
		cluster.WithLogger(logger),
	)
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

	lsd, err = adm.GetLogStream(context.Background(), td.TopicID, lsd.LogStreamID)
	assert.NoError(t, err)
	assert.True(t, lsd.Status.Sealed())

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
	connInfo := vault.GetClusterConnectionInfo(t)
	kc, err := client.NewClient(
		client.WithClusterConnectionInfo(connInfo),
	)
	require.NoError(t, err)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync()

	tc := cluster.NewTestCluster(t,
		cluster.WithClient(kc),
		cluster.WithLogger(logger),
		cluster.NumStorageNodes(6),
	)
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

/*
func TestK8sVarlogEnduranceExample(t *testing.T) {
	t.Skip()

	opts := getK8sVarlogClusterOpts(t)
	opts.Reset = true
	opts.NrSN = 5
	opts.NrLS = 5
	opts.timeout = 20 * time.Second
	Convey("Given Varlog Cluster", t, withTestCluster(t, opts, func(k8s *K8sVarlogCluster) {
		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		snFail := NewConfChanger(
			WithChangeFunc(AnyBackupSNFail(k8s)),
			WithCheckFunc(WaitSNFail(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		updateLS := NewConfChanger(
			WithChangeFunc(ReconfigureSealedLogStreams(k8s)),
			WithCheckFunc(WaitSealed(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		recoverSN := NewConfChanger(
			WithChangeFunc(RecoverSN(k8s)),
			WithCheckFunc(RecoverSNCheck(k8s)),
		)

		Action := NewAction(WithTitle("backup sn fail"),
			WithClusterID(types.ClusterID(1)),
			WithMRAddr(mrseed),
			WithPreHook(InitLogStream(k8s)),
			WithConfChange(snFail),
			WithConfChange(updateLS),
			WithConfChange(recoverSN),
			WithNumClient(1),
			WithNumSubscriber(0),
		)

		err = Action.Do(context.TODO())
		if err != nil {
			t.Logf("failed: %+v", err)
		}
		So(err, ShouldBeNil)
	}))
}

func TestK8sVarlogEnduranceFollowerMRFail(t *testing.T) {
	t.Skip()

	opts := getK8sVarlogClusterOpts(t)
	opts.NrMR = 3
	opts.NrSN = 3
	opts.NrLS = 1
	Convey("Given Varlog Cluster", t, withTestCluster(t, opts, func(k8s *K8sVarlogCluster) {
		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		mrFail := NewConfChanger(
			WithChangeFunc(FollowerMRFail(t, k8s)),
			WithCheckFunc(WaitMRFail(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		recoverMR := NewConfChanger(
			WithChangeFunc(RecoverMR(k8s)),
			WithCheckFunc(RecoverMRCheck(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		Action := NewAction(WithTitle("follower mr fail"),
			WithClusterID(types.ClusterID(1)),
			WithMRAddr(mrseed),
			WithConfChange(mrFail),
			WithConfChange(recoverMR),
			WithNumRepeats(3),
			WithPreHook(InitLogStream(k8s)),
			WithNumClient(1),
			WithNumSubscriber(0),
		)

		err = Action.Do(context.TODO())
		if err != nil {
			t.Logf("failed: %+v", err)
		}
		So(err, ShouldBeNil)
	}))
}

func TestK8sCreateCluster(t *testing.T) {
	opts := getK8sVarlogClusterOpts(t)
	opts.NrMR = 3
	opts.NrSN = 6
	opts.NrLS = 2
	opts.RepFactor = 3
	opts.Reset = true

	Convey("Given Varlog Cluster", t, withTestCluster(t, opts, func(k8s *K8sVarlogCluster) {
		vmsAddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		ctx, cancel := k8s.TimeoutContext()
		defer cancel()

		vmsCL, err := varlog.NewAdmin(ctx, vmsAddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(vmsCL.Close(), ShouldBeNil)
		})

		var topicID types.TopicID

		k8s.WithTimeoutContext(func(ctx context.Context) {
			rsp, err := vmsCL.AddTopic(ctx)
			So(err, ShouldBeNil)
			topicID = rsp.TopicID
		})

		logStreamIDs := make([]types.LogStreamID, 0, opts.NrLS)
		for i := 0; i < opts.NrLS; i++ {
			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					var logStreamDesc *varlogpb.LogStreamDescriptor
					logStreamDesc, err = vmsCL.AddLogStream(ctx, topicID, nil)
					So(err, ShouldBeNil)
					logStreamID := logStreamDesc.GetLogStreamID()
					logStreamIDs = append(logStreamIDs, logStreamID)
					log.Printf("AddLogStream (%d)", logStreamID)
				})
				return err == nil
			})
		}

		for _, logStreamID := range logStreamIDs {
			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					_, err = vmsCL.Seal(ctx, topicID, logStreamID)
					So(err, ShouldBeNil)
				})
				log.Printf("Seal (%d)", logStreamID)
				return err == nil
			})
		}

		for _, logStreamID := range logStreamIDs {
			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					_, err = vmsCL.Unseal(ctx, topicID, logStreamID)
					So(err, ShouldBeNil)
				})
				log.Printf("Unseal (%d)", logStreamID)
				return err == nil
			})
		}
	}))
}
*/
