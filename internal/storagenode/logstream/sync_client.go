package logstream

import (
	"context"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type syncClient struct {
	syncClientConfig
	srcReplica varlogpb.Replica
	rpcClient  snpb.ReplicatorClient
}

func newSyncClient(cfg syncClientConfig) *syncClient {
	sc := &syncClient{syncClientConfig: cfg}
	sc.srcReplica = varlogpb.Replica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: sc.lse.snid,
			Address:       sc.lse.advertiseAddress,
		},
		TopicID:     sc.lse.tpid,
		LogStreamID: sc.lse.lsid,
	}
	sc.rpcClient = snpb.NewReplicatorClient(sc.rpcConn.Conn)
	return sc
}

func (sc *syncClient) syncInit(ctx context.Context, srcRange snpb.SyncRange) (syncRange snpb.SyncRange, err error) {
	rsp, err := sc.rpcClient.SyncInit(ctx, &snpb.SyncInitRequest{
		ClusterID:   sc.lse.cid,
		Source:      sc.srcReplica,
		Destination: sc.dstReplica,
		Range:       srcRange,
	})
	return rsp.GetRange(), err
}

func (sc *syncClient) syncReplicate(ctx context.Context, payload snpb.SyncPayload) error {
	_, err := sc.rpcClient.SyncReplicate(ctx, &snpb.SyncReplicateRequest{
		ClusterID:   sc.lse.cid,
		Source:      sc.srcReplica,
		Destination: sc.dstReplica,
		Payload:     payload,
	})
	return err
}

func (sc *syncClient) close() error {
	return sc.rpcConn.Close()
}

type syncClientConfig struct {
	dstReplica varlogpb.Replica
	rpcConn    *rpc.Conn

	lse    *Executor
	logger *zap.Logger
}
