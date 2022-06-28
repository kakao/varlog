package metarepos

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
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
