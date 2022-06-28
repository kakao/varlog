package logstream

import (
	"context"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
)

func Describe(tpid types.TopicID, lsid ...types.LogStreamID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		if len(lsid) > 0 {
			return adm.GetLogStream(ctx, tpid, lsid[0])
		}
		return adm.ListLogStreams(ctx, tpid)
	}
}

func Add(tpid types.TopicID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.AddLogStream(ctx, tpid, nil)
	}
}

func Seal(tpid types.TopicID, lsid types.LogStreamID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.Seal(ctx, tpid, lsid)
	}
}

func Unseal(tpid types.TopicID, lsid types.LogStreamID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.Unseal(ctx, tpid, lsid)
	}
}

func Sync(tpid types.TopicID, lsid types.LogStreamID, src, dst types.StorageNodeID) varlogctl.ExecuteFunc {
	return func(ctx context.Context, adm varlog.Admin) (any, error) {
		return adm.Sync(ctx, tpid, lsid, src, dst)
	}
}
