package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
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
