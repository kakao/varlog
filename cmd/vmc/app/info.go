package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

func (app *VMCApp) infoMRMembers() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.ClusterManagerClient) (proto.Message, error) {
			app.logger.Info("info MR Members")
			return cli.GetMRMembers(ctx)
		},
	)
}
