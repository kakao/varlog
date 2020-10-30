package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
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
