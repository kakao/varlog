package storagenode

import (
	"context"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/result"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

const resourceType = "storage node"

func Add(addr string, _ types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		if snmd, err := adm.AddStorageNode(ctx, addr); err != nil {
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
		for snID, snAddr := range snMap {
			if len(id) > 0 && id[0] != snID {
				continue
			}
			res.AddDataItems(map[string]interface{}{
				"storageNodeId": snID,
				"address":       snAddr,
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