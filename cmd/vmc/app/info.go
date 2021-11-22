package app

import (
	"context"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/vmspb"
)

func (app *VMCApp) infoMRMembers() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			app.logger.Info("info MR Members")
			return cli.GetMRMembers(ctx)
		},
	)
}

func (app *VMCApp) infoStoragenodes() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			app.logger.Info("info storagenode")
			rsp := &vmspb.GetStorageNodesResponse{}
			snMap, err := cli.GetStorageNodes(ctx)
			rsp.StorageNodes = snMap
			if len(rsp.StorageNodes) == 0 {
				rsp.StorageNodes = make(map[types.StorageNodeID]string)
			}
			return rsp, err
		},
	)
}
