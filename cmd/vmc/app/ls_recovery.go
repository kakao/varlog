package app

import (
	"context"

	"github.com/gogo/protobuf/proto"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func (app *VMCApp) updateLogStream(logStreamID types.LogStreamID, popReplica, pushReplica *varlogpb.ReplicaDescriptor) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			// update
			panic("not implemented")
		},
	)
}

func (app *VMCApp) sealLogStream(logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.Seal(ctx, logStreamID)
		},
	)
}

func (app *VMCApp) unsealLogStream(logStreamID types.LogStreamID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.Unseal(ctx, logStreamID)
		},
	)
}

func (app *VMCApp) syncLogStream(logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.Sync(ctx, logStreamID, srcStorageNodeID, dstStorageNodeID)
		},
	)
}
