package app

import (
	"context"

	"github.com/gogo/protobuf/proto"

	"github.daumkakao.com/varlog/varlog/pkg/admin"
)

func (app *VMCApp) infoMRMembers() {
	app.withExecutionContext(
		func(ctx context.Context, cli admin.Client) (proto.Message, error) {
			app.logger.Info("info MR Members")
			return cli.GetMRMembers(ctx)
		},
	)
}

func (app *VMCApp) infoStoragenodes() {
	app.withExecutionContext(
		func(ctx context.Context, cli admin.Client) (proto.Message, error) {
			app.logger.Info("info storagenode")
			return cli.GetStorageNodes(ctx)
		},
	)
}
