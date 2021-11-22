package app

import (
	"context"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

func (app *VMCApp) addStorageNode(snAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			app.logger.Info("add storagenode", zap.String("snaddr", snAddr))
			return cli.AddStorageNode(ctx, snAddr)
		},
	)
}

func (app *VMCApp) addTopic() {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
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
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			return cli.AddLogStream(ctx, topicID, nil)
		},
	)
}

func (app *VMCApp) addMRPeer(raftURL, rpcAddr string) {
	app.withExecutionContext(
		func(ctx context.Context, cli varlog.Admin) (interface{}, error) {
			app.logger.Info("add MR Peer",
				zap.String("raftURL", raftURL),
				zap.String("rpcAddr", rpcAddr),
			)
			rsp := &vmspb.AddMRPeerResponse{}
			nodeID, err := cli.AddMRPeer(ctx, raftURL, rpcAddr)
			rsp.NodeID = nodeID
			return rsp, err
		},
	)
}
