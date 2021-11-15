package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	pbtypes "github.com/gogo/protobuf/types"

	"github.com/kakao/varlog/pkg/varlog"
)

func (app *VMCApp) infoMRMembers() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (proto.Message, error) {
			app.logger.Info("info MR Members")
			return cli.GetMRMembers(ctx)
		},
	)
}

func (app *VMCApp) infoStoragenodes() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (proto.Message, error) {
			app.logger.Info("info storagenode")
			snMap, err := cli.GetStorageNodes(ctx)
			if err != nil {
				return nil, err
			}
			// FIXME: Fix awkward implementation.
			ret := &pbtypes.Struct{}
			for snID, addr := range snMap {
				ret.Fields[snID.String()] = &pbtypes.Value{
					Kind: &pbtypes.Value_StringValue{StringValue: addr},
				}
			}
			return ret, nil
		},
	)
}
