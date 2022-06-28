package metarepos

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

func Describe(raftURL ...string) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		if len(raftURL) > 0 {
			nid := types.NewNodeIDFromURL(raftURL[0])
			return adm.GetMetadataRepositoryNode(ctx, nid)
		}
		return adm.ListMetadataRepositoryNodes(ctx)
	}
}

func Add(raftURL, rpcAddr string) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.AddMetadataRepositoryNode(ctx, raftURL, rpcAddr)
	}
}

func Remove(raftURL string) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		nid := types.NewNodeIDFromURL(raftURL)
		return &empty.Empty{}, adm.DeleteMetadataRepositoryNode(ctx, nid)
	}
}
