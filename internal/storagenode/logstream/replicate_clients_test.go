package logstream

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/snpb/mock"
)

func TestReplicateClients(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	serverMock := mock.NewMockReplicatorServer(ctrl)
	serverMock.EXPECT().ReplicateNew(gomock.Any()).DoAndReturn(
		func(stream snpb.Replicator_ReplicateNewServer) error {
			return stream.SendAndClose(&snpb.ReplicateResponse{})
		},
	).MaxTimes(1)

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

	rcs := newReplicateClients()
	rcs.add(rc)
	rcs.close()
}
