package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

func (app *VMCApp) addStorageNode(snAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			app.logger.Info("add storagenode", zap.String("snaddr", snAddr))
			return cli.AddStorageNode(ctx, snAddr)
		},
	)
}

func (app *VMCApp) addLogStream() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			return cli.AddLogStream(ctx, nil)
		},
	)
}

func (app *VMCApp) addMRPeer(raftURL, rpcAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			app.logger.Info("add MR Peer",
				zap.String("raftURL", raftURL),
				zap.String("rpcAddr", rpcAddr),
			)
			return cli.AddMRPeer(ctx, raftURL, rpcAddr)
		},
	)
}
