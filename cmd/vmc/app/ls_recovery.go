package app

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func (app *VMCApp) updateLogStream(logStreamID types.LogStreamID, popReplica, pushReplica *varlogpb.ReplicaDescriptor) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			// update
			panic("not implemented")
		},
	)
}

func (app *VMCApp) sealLogStream(topicID types.TopicID, logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			return cli.Seal(ctx, topicID, logStreamID)
		},
	)
}

func (app *VMCApp) unsealLogStream(topicID types.TopicID, logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			return cli.Unseal(ctx, topicID, logStreamID)
		},
	)
}

func (app *VMCApp) syncLogStream(topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			return cli.Sync(ctx, topicID, logStreamID, srcStorageNodeID, dstStorageNodeID)
		},
	)
}
