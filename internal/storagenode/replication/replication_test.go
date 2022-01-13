package replication

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/multierr"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/netutil"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestReplicationBadConnectorClient(t *testing.T) {
	var (
		err       error
		connector Connector
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metrics := telemetry.NewMetrics()

	// no address
	connector, err = NewConnector(
		WithClientOptions(
			WithMetrics(metrics),
		),
	)
	require.NoError(t, err)
	_, err = connector.Get(context.TODO(), varlogpb.Replica{})
	require.Error(t, err)
	require.NoError(t, connector.Close())

	// bad requestQueueSize
	connector, err = NewConnector(
		WithClientOptions(
			WithRequestQueueSize(-1),
			WithMetrics(metrics),
		))
	require.NoError(t, err)
	_, err = connector.Get(context.TODO(),
		varlogpb.Replica{
			StorageNode: varlogpb.StorageNode{
				Address: "localhost:12345",
			},
		},
	)
	require.Error(t, err)
	require.NoError(t, connector.Close())

	// bad address
	connector, err = NewConnector(WithClientOptions(
		WithRequestQueueSize(1),
		WithMetrics(metrics),
	))
	require.NoError(t, err)
	_, err = connector.Get(context.TODO(),
		varlogpb.Replica{
			StorageNode: varlogpb.StorageNode{
				Address: "bad-address",
			},
		},
	)
	require.Error(t, err)
	require.NoError(t, connector.Close())
}

func TestReplicationClosedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// logReplicator mock
	replicator := NewMockReplicator(ctrl)
	replicatorGetter := NewMockGetter(ctrl)
	replicatorGetter.EXPECT().Replicator(gomock.Any(), gomock.Any()).Return(replicator, true).AnyTimes()
	replicator.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// replicator server
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	addrs, err := netutil.GetListenerAddrs(lis.Addr())
	require.NoError(t, err)

	metrics := telemetry.NewMetrics()

	snidGetter := id.NewMockStorageNodeIDGetter(ctrl)
	snidGetter.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
	server := NewServer(
		WithStorageNodeIDGetter(snidGetter),
		WithLogReplicatorGetter(replicatorGetter),
		WithMetrics(metrics),
	)

	grpcServer := grpc.NewServer()
	server.Register(grpcServer)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(lis)
		require.NoError(t, err)
	}()

	// connector
	connector, err := NewConnector(
		WithClientOptions(
			WithMetrics(metrics),
		),
	)
	require.NoError(t, err)

	// replicator client
	replica := varlogpb.Replica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: 1,
			Address:       addrs[0],
		},
		LogStreamID: 1,
	}

	client, err := connector.Get(context.TODO(), replica)
	require.NoError(t, err)
	require.NoError(t, client.Close())

	// closed client
	cliWg := sync.WaitGroup{}
	cliWg.Add(1)
	client.Replicate(context.TODO(), 1, nil, 0, func(_ int64, err error) {
		defer cliWg.Done()
		require.Error(t, err)
	})
	cliWg.Wait()

	// closed connector
	require.NoError(t, connector.Close())
	_, err = connector.Get(context.TODO(), replica)
	require.Error(t, err)

	require.NoError(t, server.Close())
	grpcServer.GracefulStop()
	wg.Wait()
}

func TestReplicationServer(t *testing.T) {
	// bad pipeline size
	require.Panics(t, func() {
		NewServer(WithPipelineQueueSize(-1))
	})

	// no logReplicatorGetter
	require.Panics(t, func() {
		NewServer()
	})
}

func TestReplication(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		storageNodeID = types.StorageNodeID(1)
		logStreamID   = types.LogStreamID(1)

		beginLLSN = types.LLSN(1)
		midLLSN   = types.LLSN(1001)
		endLLSN   = types.LLSN(2001)
		badLLSN   = types.LLSN(1501)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// replicator mock
	replicator := NewMockReplicator(ctrl)
	replicatorGetter := NewMockGetter(ctrl)
	replicatorGetter.EXPECT().Replicator(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ types.TopicID, lsid types.LogStreamID) (Replicator, bool) {
			switch lsid {
			case logStreamID:
				return replicator, true
			default:
				return nil, false
			}
		},
	).AnyTimes()

	var replicatedLLSNs []types.LLSN
	expectedLLSN := beginLLSN
	replicator.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, llsn types.LLSN, data []byte) error {
			if llsn == badLLSN {
				return errors.Errorf("bad llsn: %d", llsn)
			}
			if llsn != expectedLLSN {
				return errors.Errorf("unexpected llsn: %d", llsn)
			}
			expectedLLSN++
			replicatedLLSNs = append(replicatedLLSNs, llsn)
			return nil
		},
	).AnyTimes()

	// replicator server
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	addrs, err := netutil.GetListenerAddrs(lis.Addr())
	require.NoError(t, err)

	metrics := telemetry.NewMetrics()

	snidGetter := id.NewMockStorageNodeIDGetter(ctrl)
	snidGetter.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
	server := NewServer(
		WithStorageNodeIDGetter(snidGetter),
		WithLogReplicatorGetter(replicatorGetter),
		WithMetrics(metrics),
	)

	grpcServer := grpc.NewServer()
	server.Register(grpcServer)

	clientServerWg := sync.WaitGroup{}
	clientServerWg.Add(1)
	go func() {
		defer clientServerWg.Done()
		err := grpcServer.Serve(lis)
		require.NoError(t, err)
	}()

	// connector
	connector, err := NewConnector(
		WithClientOptions(
			WithMetrics(metrics),
		),
	)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, connector.Close())
	}()

	// replicator client
	replica := varlogpb.Replica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: storageNodeID,
			Address:       addrs[0],
		},
		LogStreamID: logStreamID,
	}

	client, err := connector.Get(context.TODO(), replica)
	require.NoError(t, err)

	doneCnt := int64(0)
	wg := sync.WaitGroup{}
	for llsn := beginLLSN; llsn < midLLSN; llsn++ {
		llsn := llsn
		wg.Add(1)
		client.Replicate(context.TODO(), llsn, []byte("foo"), 0, func(_ int64, err error) {
			atomic.AddInt64(&doneCnt, 1)
			defer wg.Done()
			require.NoError(t, err)
		})
	}

	// get again
	client, err = connector.Get(context.TODO(), replica)
	require.NoError(t, err)
	for llsn := midLLSN; llsn < endLLSN; llsn++ {
		llsn := llsn
		wg.Add(1)
		client.Replicate(context.TODO(), llsn, []byte("foo"), 0, func(_ int64, err error) {
			atomic.AddInt64(&doneCnt, 1)
			defer wg.Done()
			if llsn < badLLSN {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
	wg.Wait()

	require.Equal(t, int(badLLSN-beginLLSN), len(replicatedLLSNs))
	for llsn := beginLLSN; llsn < badLLSN; llsn++ {
		i := int(llsn - beginLLSN)
		require.Equal(t, llsn, replicatedLLSNs[i])
	}

	func() { // Either server or client can stop first.
		errC := make(chan error, 2)

		clientServerWg.Add(2)
		go func() {
			defer clientServerWg.Done()
			err := server.Close()
			grpcServer.GracefulStop()
			errC <- err
		}()
		go func() {
			defer clientServerWg.Done()
			errC <- client.Close()
		}()
		clientServerWg.Wait()

		var err error
		for i := 0; i < 2; i++ {
			err = multierr.Append(err, <-errC)
		}
		require.NoError(t, err)
	}()
}
