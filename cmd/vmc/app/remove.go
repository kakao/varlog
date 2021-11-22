package app

import (
	"context"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

func (app *VMCApp) removeStorageNode(storageNodeID types.StorageNodeID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			return nil, cli.UnregisterStorageNode(ctx, storageNodeID)
		},
	)
}

func (app *VMCApp) removeLogStream(topicID types.TopicID, logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			return nil, cli.UnregisterLogStream(ctx, topicID, logStreamID)
			// TODO (jun): according to options, it can remove log stream replicas of
			// the log stream.
		},
	)
}

func (app *VMCApp) removeMRPeer(raftURL string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			app.logger.Info("remove MR Peer",
				zap.Any("raft-url", raftURL),
			)
			return nil, cli.RemoveMRPeer(ctx, raftURL)
		},
	)
}
