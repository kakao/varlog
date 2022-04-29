package logstream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb/mock"
)

func TestReplicateClient_InvalidConfig(t *testing.T) {
	lis, connect := rpc.TestNewConn(t, context.Background(), 1)
	defer func() {
		assert.NoError(t, lis.Close())
	}()
	rpcConn := connect()
	defer func() {
		assert.NoError(t, rpcConn.Close())
	}()

	_, err := newReplicateClient(context.Background(), replicateClientConfig{
		queueCapacity: minQueueCapacity - 1,
		rpcConn:       rpcConn,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newReplicateClient(context.Background(), replicateClientConfig{
		queueCapacity: maxQueueCapacity + 1,
		rpcConn:       rpcConn,
		lse:           &Executor{},
		logger:        zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newReplicateClient(context.Background(), replicateClientConfig{
		rpcConn: rpcConn,
		logger:  zap.NewNop(),
	})
	assert.Error(t, err)

	_, err = newReplicateClient(context.Background(), replicateClientConfig{
		lse:     &Executor{},
		rpcConn: rpcConn,
	})
	assert.Error(t, err)

	_, err = newReplicateClient(context.Background(), replicateClientConfig{
		lse:    &Executor{},
		logger: zap.NewNop(),
	})
	assert.Error(t, err)
}

func TestReplicateClient_ShouldNotAcceptTasksWhileNotAppendable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	rc := &replicateClient{}
	rc.lse = lse
	rc.logger = zap.NewNop()

	mockStreamClient := mock.NewMockReplicator_ReplicateClient(ctrl)
	mockStreamClient.EXPECT().Context().Return(context.Background()).AnyTimes()
	rc.streamClient = mockStreamClient

	lse.esm.store(executorStateSealing)
	err := rc.send(context.Background(), &replicateTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateSealed)
	err = rc.send(context.Background(), &replicateTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateClosed)
	err = rc.send(context.Background(), &replicateTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = rc.send(canceledCtx, &replicateTask{})
	assert.Error(t, err)

	lse.esm.store(executorStateAppendable)
	mockStreamClient = mock.NewMockReplicator_ReplicateClient(ctrl)
	mockStreamClient.EXPECT().Context().Return(canceledCtx).AnyTimes()
	rc.streamClient = mockStreamClient
	err = rc.send(context.Background(), &replicateTask{})
	assert.Error(t, err)
}

func TestReplicateClientRPCError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	lse.tpid = 1
	lse.lsid = 2

	mockStreamClient := mock.NewMockReplicator_ReplicateClient(ctrl)
	mockStreamClient.EXPECT().Send(gomock.Any()).Return(errors.New("fake"))
	mockStreamClient.EXPECT().Context().Return(context.Background()).AnyTimes()
	mockStreamClient.EXPECT().CloseSend().Return(nil)

	rc := &replicateClient{}
	rc.lse = lse
	rc.logger = zap.NewNop()
	rc.queue = make(chan *replicateTask, 1)
	rc.streamClient = mockStreamClient

	rt := newReplicateTask(0)
	rt.tpid = lse.tpid
	rt.lsid = lse.lsid
	rt.llsnList = append(rt.llsnList, 1)
	rt.dataList = [][]byte{nil}

	err := rc.send(context.Background(), rt)
	assert.NoError(t, err)
	rc.sendLoop(context.Background())

	assert.Eventually(t, func() bool {
		return rc.lse.esm.load() == executorStateSealing
	}, time.Second, 10*time.Millisecond)
}

func TestReplicateClientDrain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const numTasks = 10

	mockStreamClient := mock.NewMockReplicator_ReplicateClient(ctrl)
	mockStreamClient.EXPECT().Context().Return(context.Background()).AnyTimes()
	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	rc := &replicateClient{}
	rc.lse = lse
	rc.streamClient = mockStreamClient
	rc.queue = make(chan *replicateTask, numTasks)

	for i := 0; i < numTasks; i++ {
		rt := newReplicateTask(0)
		err := rc.send(context.Background(), rt)
		assert.NoError(t, err)
	}

	assert.EqualValues(t, numTasks, atomic.LoadInt64(&rc.inflight))
	assert.Len(t, rc.queue, numTasks)

	rc.waitForDrainage()

	assert.Zero(t, atomic.LoadInt64(&rc.inflight))
	assert.Empty(t, rc.queue)
}

type testReplicateRequest struct {
	req *snpb.ReplicateRequest
	err error
}

func TestReplicateClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan testReplicateRequest, 1)

	serverMock := mock.NewMockReplicatorServer(ctrl)
	serverMock.EXPECT().Replicate(gomock.Any()).DoAndReturn(
		func(stream snpb.Replicator_ReplicateServer) error {
			req, err := stream.Recv()
			ch <- testReplicateRequest{req: req, err: err}
			return err
		},
	)
	_, rpcConn, closer := TestNewReplicateServer(t, serverMock)
	defer closer()

	lse := &Executor{
		esm: newExecutorStateManager(executorStateAppendable),
	}
	lse.tpid = 1
	lse.lsid = 2
	rc, err := newReplicateClient(context.Background(), replicateClientConfig{
		queueCapacity: 1,
		rpcConn:       rpcConn,
		lse:           lse,
		logger:        zap.NewNop(),
	})
	assert.NoError(t, err)
	defer rc.stop()

	rt := newReplicateTask(0)
	rt.tpid = lse.tpid
	rt.lsid = lse.lsid
	rt.llsnList = append(rt.llsnList, 1)
	rt.dataList = [][]byte{nil}
	err = rc.send(context.Background(), rt)
	assert.NoError(t, err)

	rr := <-ch
	assert.NoError(t, rr.err)
	assert.Equal(t, lse.tpid, rr.req.TopicID)
	assert.Equal(t, lse.lsid, rr.req.LogStreamID)
	assert.Equal(t, []types.LLSN{1}, rr.req.LLSN)
	assert.Len(t, rr.req.Data, 1)
	assert.Empty(t, rr.req.Data[0])
}
