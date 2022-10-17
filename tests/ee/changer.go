package ee

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/tests/ee/cluster"
)

func FailStorageNode(ctx context.Context, t *testing.T, tc cluster.Cluster, snid types.StorageNodeID) func() {
	return func() {
		tc.StopStorageNode(ctx, t, snid)
	}
}

func AnyBackupSNFail(tc cluster.Cluster, snid *types.StorageNodeID, nodeName *string) func(context.Context, *testing.T) bool {
	return func(ctx context.Context, t *testing.T) bool {
		return anySNFail(ctx, t, tc, false, snid, nodeName)
	}
}

func StartStorageNode(tc cluster.Cluster, nodeNameGetter func() string) func(context.Context, *testing.T) bool {
	return func(ctx context.Context, t *testing.T) bool {
		t.Helper()
		return tc.StartStorageNode(ctx, t, nodeNameGetter())
	}
}

func anySNFail(ctx context.Context, t *testing.T, tc cluster.Cluster, primary bool, snid *types.StorageNodeID, nodeName *string) bool {
	t.Helper()

	adminAddr := tc.AdminServerAddress(ctx, t)
	adm, err := varlog.NewAdmin(ctx, adminAddr)
	if !assert.NoError(t, err) {
		return false
	}

	tds, err := adm.ListTopics(ctx)
	if !assert.NoError(t, err) {
		return false
	}
	td := tds[rand.Intn(len(tds))]

	lsds, err := adm.ListLogStreams(ctx, td.TopicID)
	if !assert.NoError(t, err) {
		return false
	}
	lsd := lsds[rand.Intn(len(lsds))]

	if primary {
		*snid = lsd.Replicas[0].StorageNodeID
	} else {
		if !assert.Greater(t, len(lsd.Replicas), 1) {
			return false
		}
		idx := rand.Intn(len(lsd.Replicas)-1) + 1
		*snid = lsd.Replicas[idx].StorageNodeID
	}
	*nodeName = tc.StopStorageNode(ctx, t, *snid)
	return true
}
