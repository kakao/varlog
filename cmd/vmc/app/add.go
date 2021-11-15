package app

import (
	"context"

	"github.com/gogo/protobuf/proto"
	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/varlog"

	"github.com/kakao/varlog/pkg/types"
)

func (app *VMCApp) addStorageNode(snAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (proto.Message, error) {
			app.logger.Info("add storagenode", zap.String("snaddr", snAddr))
			return cli.AddStorageNode(ctx, snAddr)
		},
	)
}

func (app *VMCApp) addTopic() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (proto.Message, error) {
			topic, err := cli.AddTopic(ctx)
			if err != nil {
				return nil, err
			}
			return &topic, nil
		},
	)
}

func (app *VMCApp) addLogStream(topicID types.TopicID) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (proto.Message, error) {
			return cli.AddLogStream(ctx, topicID, nil)
		},
	)
}

func (app *VMCApp) addMRPeer(raftURL, rpcAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (proto.Message, error) {
			app.logger.Info("add MR Peer",
				zap.String("raftURL", raftURL),
				zap.String("rpcAddr", rpcAddr),
			)
			nodeID, err := cli.AddMRPeer(ctx, raftURL, rpcAddr)
			return &pbtypes.UInt64Value{Value: uint64(nodeID)}, err
		},
	)
}
