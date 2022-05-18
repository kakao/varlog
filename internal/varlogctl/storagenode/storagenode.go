package storagenode

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/internal/varlogctl/result"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

const resourceType = "storage node"

func Add(addr string, snid types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		if snmd, err := adm.AddStorageNode(ctx, snid, addr); err != nil {
			res.AddErrors(err)
		} else {
			res.AddDataItems(snmd)
		}
		return res
	}
}

func Remove(addr string, id types.StorageNodeID) varlogctl.ExecuteFunc {
	// TODO: Unregister storage node
	panic("not implemented")
}

func Describe(id ...types.StorageNodeID) varlogctl.ExecuteFunc {
	// FIXME: return type
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		snMap, err := adm.GetStorageNodes(ctx)
		if err != nil {
			res.AddErrors(err)
			return res
		}
		for snID, snmd := range snMap {
			if len(id) > 0 && id[0] != snID {
				continue
			}
			res.AddDataItems(map[string]interface{}{
				"storageNodeId": snID,
				"address":       snmd.StorageNode.Address,
				"metadata":      snmd,
			})
			if len(id) > 0 {
				break
			}
		}
		if len(id) > 0 && res.NumberOfDataItem() == 0 {
			res.AddErrors(errors.Errorf("no such storage node %d", id[0]))
		}
		return res
	}
}

// TODO: Unregister log stream replica
