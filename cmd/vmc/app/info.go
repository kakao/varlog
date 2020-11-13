package app

import (
	"context"

	"github.com/gogo/protobuf/proto"

	"github.com/kakao/varlog/pkg/varlog"
)

func (app *VMCApp) infoMRMembers() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			app.logger.Info("info MR Members")
			return cli.GetMRMembers(ctx)
		},
	)
}

func (app *VMCApp) infoStoragenodes() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			app.logger.Info("info storagenode")
			return cli.GetStorageNodes(ctx)
		},
	)
}
