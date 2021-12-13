package metarepos

import (
	"context"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/result"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

const resourceType = "metadata repository"

func Add(raftURL, rpcAddr string) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		if nodeID, err := adm.AddMRPeer(ctx, raftURL, rpcAddr); err != nil {
			res.AddErrors(err)
		} else {
			res.AddDataItems(map[string]interface{}{
				"nodeId": nodeID,
			})
		}
		return res
	}
}

func Remove(raftURL string) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		if err := adm.RemoveMRPeer(ctx, raftURL); err != nil {
			res.AddErrors(err)
			return res
		}
		return res
	}
}

func Describe(raftURL ...string) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) *result.Result {
		res := result.New(resourceType)
		rsp, err := adm.GetMRMembers(ctx)
		if err != nil {
			res.AddErrors(err)
			return res
		}
		for id, url := range rsp.Members {
			if len(raftURL) > 0 && raftURL[0] != url {
				continue
			}
			res.AddDataItems(map[string]interface{}{
				"nodeId":            id,
				"raftURL":           url,
				"leader":            rsp.Leader,
				"replicationFactor": rsp.ReplicationFactor,
			})
			if len(raftURL) > 0 {
				break
			}
		}
		if len(raftURL) > 0 && res.NumberOfDataItem() == 0 {
			res.AddErrors(errors.Errorf("no such metadata repository %s", raftURL))
		}
		return res
	}
}
