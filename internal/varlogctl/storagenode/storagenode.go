package storagenode

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/admpb"
	"github.com/kakao/varlog/proto/snpb"
)

func Describe(snid ...types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		if len(snid) > 0 {
			snm, err := adm.GetStorageNode(ctx, snid[0])
			if err != nil {
				return nil, err
			}
			ensureEmptyFields(snm)
			return snm, nil

		}
		snms, err := adm.ListStorageNodes(ctx)
		if err != nil {
			return nil, err
		}
		for idx := range snms {
			ensureEmptyFields(&snms[idx])
		}
		return snms, nil
	}
}

func ensureEmptyFields(snm *admpb.StorageNodeMetadata) {
	if len(snm.LogStreamReplicas) == 0 {
		snm.LogStreamReplicas = []snpb.LogStreamReplicaMetadataDescriptor{}
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
