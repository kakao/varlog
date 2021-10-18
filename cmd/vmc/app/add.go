package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/admin"
	"github.com/kakao/varlog/pkg/types"
)

func (app *VMCApp) addStorageNode(snAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli admin.Client) (proto.Message, error) {
			app.logger.Info("add storagenode", zap.String("snaddr", snAddr))
			return cli.AddStorageNode(ctx, snAddr)
		},
	)
}

func (app *VMCApp) addTopic() {
	app.withExecutionContext(
		func(ctx context.Context, cli admin.Client) (proto.Message, error) {
			return cli.AddTopic(ctx)
		},
	)
}

func (app *VMCApp) addLogStream(topicID types.TopicID) {
	app.withExecutionContext(
		func(ctx context.Context, cli admin.Client) (proto.Message, error) {
			return cli.AddLogStream(ctx, topicID, nil)
		},
	)
}

func (app *VMCApp) addMRPeer(raftURL, rpcAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli admin.Client) (proto.Message, error) {
			app.logger.Info("add MR Peer",
				zap.String("raftURL", raftURL),
				zap.String("rpcAddr", rpcAddr),
			)
			return cli.AddMRPeer(ctx, raftURL, rpcAddr)
		},
	)
}
