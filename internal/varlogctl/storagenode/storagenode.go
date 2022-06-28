package storagenode

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

func Describe(snid ...types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		if len(snid) > 0 {
			return adm.GetStorageNode(ctx, snid[0])
		}
		return adm.ListStorageNodes(ctx)
	}
}

func Add(addr string, snid types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.AddStorageNode(ctx, snid, addr)
	}
}

func Remove(addr string, snid types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		err := adm.UnregisterStorageNode(ctx, snid)
		return empty.Empty{}, err
	}
}

// TODO: Unregister log stream replica
