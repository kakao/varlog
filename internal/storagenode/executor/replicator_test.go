package executor

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/replication"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestReplicationProcessorFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	state := NewMockStateProvider(ctrl)

	// queue size
	_, err := newReplicator(replicatorConfig{
		queueSize: 0,
		state:     state,
		metrics:   telemetry.NewMetrics(),
		connectorOpts: []replication.ConnectorOption{
			replication.WithClientOptions(
				replication.WithMetrics(telemetry.NewMetrics()),
			),
		},
	})
	require.Error(t, err)

	// no state
	_, err = newReplicator(replicatorConfig{
		queueSize: 1,
		state:     nil,
		metrics:   telemetry.NewMetrics(),
		connectorOpts: []replication.ConnectorOption{
			replication.WithClientOptions(
				replication.WithMetrics(telemetry.NewMetrics()),
			),
		},
	})
	require.Error(t, err)
}

func TestReplicationProcessorNoClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	const queueSize = 10

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// state provider
	var called atomicutil.AtomicBool
	called.Store(false)
	state := NewMockStateProvider(ctrl)
	state.EXPECT().mutableWithBarrier().Return(nil)
	state.EXPECT().releaseBarrier().Return()
	state.EXPECT().setSealingWithReason(gomock.Any()).DoAndReturn(
		func(_ error) {
			called.Store(true)
		},
	).MaxTimes(2)

	rp, err := newReplicator(replicatorConfig{
		queueSize: queueSize,
		state:     state,
		metrics:   telemetry.NewMetrics(),
		connectorOpts: []replication.ConnectorOption{
			replication.WithClientOptions(
				replication.WithMetrics(telemetry.NewMetrics()),
			),
		},
	})
	require.NoError(t, err)
	defer rp.stop()

	rtb := newReplicateTask()
	rtb.llsn = types.LLSN(1)
	rtb.replicas = []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
				Address:       "localhost:12345",
			},
			LogStreamID: 1,
		},
	}

	err = rp.send(context.TODO(), rtb)
	require.NoError(t, err)
	require.Eventually(t, called.Load, 2*time.Second, 10*time.Millisecond)
}

func TestReplicationProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 10
		numLogs   = 100
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var testCases []*struct {
		rp             *replicatorImpl
		expectedSealed bool
		actualSealed   atomicutil.AtomicBool
	}

	for _, cbErr := range []error{nil, errors.New("fake")} {
		cbErr := cbErr

		tc := &struct {
			rp             *replicatorImpl
			expectedSealed bool
			actualSealed   atomicutil.AtomicBool
		}{}

		client := replication.NewMockClient(ctrl)
		client.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ types.LLSN, _ []byte, _ int64, f func(int64, error)) {
				f(0, cbErr)
			},
		).AnyTimes()

		tc.actualSealed.Store(false)
		state := NewMockStateProvider(ctrl)
		state.EXPECT().mutableWithBarrier().DoAndReturn(func() error {
			if tc.actualSealed.Load() {
				return verrors.ErrSealed
			}
			return nil
		}).AnyTimes()
		state.EXPECT().releaseBarrier().Return().AnyTimes()
		state.EXPECT().setSealingWithReason(gomock.Any()).DoAndReturn(func(_ error) {
			tc.actualSealed.Store(true)
			t.Logf("setSealing")
		}).AnyTimes()

		tc.expectedSealed = cbErr != nil

		rp, err := newReplicator(replicatorConfig{
			queueSize: queueSize,
			state:     state,
			metrics:   telemetry.NewMetrics(),
			connectorOpts: []replication.ConnectorOption{
				replication.WithClientOptions(
					replication.WithMetrics(telemetry.NewMetrics()),
				),
			},
		})
		require.NoError(t, err)

		// replace with mock connector
		require.NoError(t, rp.connector.Close())
		connector := replication.NewMockConnector(ctrl)
		connector.EXPECT().Get(gomock.Any(), gomock.Any()).Return(client, nil).AnyTimes()
		connector.EXPECT().Close().Return(nil).AnyTimes()
		rp.connector = connector

		tc.rp = rp

		testCases = append(testCases, tc)
	}

	for i := range testCases {
		tc := testCases[i]
		name := fmt.Sprintf("sealed=%+v", tc.expectedSealed)
		t.Run(name, func(t *testing.T) {
			for i := 1; i <= numLogs; i++ {
				rt := newReplicateTask()
				rt.llsn = types.LLSN(i)
				rt.replicas = []varlogpb.Replica{
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: 1,
							Address:       "localhost:12345",
						},
						LogStreamID: 1,
					},
				}
				if err := tc.rp.send(context.TODO(), rt); err != nil {
					rt.release()
					break
				}
			}

			// TODO: check status
			require.Eventually(t, func() bool {
				t.Logf("expected=%v, actual=%v", tc.expectedSealed, tc.actualSealed.Load())
				return tc.expectedSealed == tc.actualSealed.Load()
			}, time.Second, 10*time.Millisecond)

			tc.rp.stop()
		})
	}
}

func TestReplicatorResetConnector(t *testing.T) {
	const (
		logStreamID = types.LogStreamID(1)
		backupSNID  = types.StorageNodeID(1)
		queueSize   = 100

		maxReplicatedLLSN = types.LLSN(10)
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Backup replica
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	addrs, err := netutil.GetListenerAddrs(lis.Addr())
	require.NoError(t, err)

	// mock replicator: backup's executor
	replicator := replication.NewMockReplicator(ctrl)
	replicatorGetter := replication.NewMockGetter(ctrl)
	replicatorGetter.EXPECT().Replicator(gomock.Any(), gomock.Any()).DoAndReturn(
		func(types.TopicID, types.LogStreamID) (replication.Replicator, bool) {
			return replicator, true
		},
	).AnyTimes()

	var (
		mu             sync.Mutex
		replicatedLogs []types.LLSN
		blockedLogs    []types.LLSN
	)

	replicator.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, llsn types.LLSN, data []byte) error {
			mu.Lock()
			if llsn <= maxReplicatedLLSN {
				replicatedLogs = append(replicatedLogs, llsn)
				mu.Unlock()
				return nil
			}
			blockedLogs = append(blockedLogs, llsn)
			mu.Unlock()
			<-ctx.Done()
			return ctx.Err()
		},
	).AnyTimes()

	snidGetter := id.NewMockStorageNodeIDGetter(ctrl)
	snidGetter.EXPECT().StorageNodeID().Return(backupSNID).AnyTimes()
	server := replication.NewServer(
		replication.WithStorageNodeIDGetter(snidGetter),
		replication.WithLogReplicatorGetter(replicatorGetter),
		replication.WithMetrics(telemetry.NewMetrics()),
	)

	grpcServer := grpc.NewServer()
	server.Register(grpcServer)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, grpcServer.Serve(lis))
	}()

	// primary replica
	state := NewMockStateProvider(ctrl)
	state.EXPECT().mutableWithBarrier().Return(nil).AnyTimes()
	state.EXPECT().releaseBarrier().Return().AnyTimes()
	state.EXPECT().setSealingWithReason(gomock.Any()).Return().AnyTimes()

	rp, err := newReplicator(replicatorConfig{
		queueSize: queueSize,
		state:     state,
		metrics:   telemetry.NewMetrics(),
		connectorOpts: []replication.ConnectorOption{
			replication.WithClientOptions(
				replication.WithMetrics(telemetry.NewMetrics()),
			),
		},
	})
	require.NoError(t, err)
	defer rp.stop()

	for llsn := types.MinLLSN; llsn <= maxReplicatedLLSN+1; llsn++ {
		rt := newReplicateTask()
		rt.llsn = llsn
		rt.replicas = []varlogpb.Replica{{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: backupSNID,
				Address:       addrs[0],
			},
			LogStreamID: logStreamID,
		}}
		require.NoError(t, rp.send(context.Background(), rt))
	}

	// wait for that all logs are transferred to backup replica
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		t.Logf("replicated=%d, blocked=%d", len(replicatedLogs), len(blockedLogs))
		return len(replicatedLogs) == int(maxReplicatedLLSN) &&
			len(blockedLogs) == 1
	}, time.Second, 10*time.Millisecond)

	// (replicatedLogs) logs were replicated, but (maxTestLLSN-maxReplicatedLLSN) logs are still
	// waiting for replicated completely.
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&rp.inflight) == 1
	}, time.Second, 10*time.Millisecond)

	// Resetting connector cancels inflight replications.
	require.NoError(t, rp.resetConnector())
	require.Eventually(t, func() bool {
		return assert.Zero(t, atomic.LoadInt64(&rp.inflight))
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, server.Close())
	grpcServer.GracefulStop()
	wg.Wait()
}
