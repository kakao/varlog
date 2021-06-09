package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

func (app *VMCApp) removeStorageNode(storageNodeID types.StorageNodeID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.UnregisterStorageNode(ctx, storageNodeID)
		},
	)
}

func (app *VMCApp) removeLogStream(logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.UnregisterLogStream(ctx, logStreamID)
			// TODO (jun): according to options, it can remove log stream replicas of
			// the log stream.
		},
	)
}

func (app *VMCApp) removeMRPeer(raftURL string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			app.logger.Info("remove MR Peer",
				zap.Any("raft-url", raftURL),
			)
			return cli.RemoveMRPeer(ctx, raftURL)
		},
	)
}
