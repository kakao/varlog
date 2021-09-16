package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

func (app *VMCApp) removeStorageNode(storageNodeID types.StorageNodeID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.UnregisterStorageNode(ctx, storageNodeID)
		},
	)
}

func (app *VMCApp) removeLogStream(topicID types.TopicID, logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.UnregisterLogStream(ctx, topicID, logStreamID)
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
