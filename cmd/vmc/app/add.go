package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"go.uber.org/zap"
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
