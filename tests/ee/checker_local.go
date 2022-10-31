//go:build !k8s

package ee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/tests/ee/cluster"
	"github.com/kakao/varlog/tests/ee/cluster/local"
)

func WaitStorageNodeFail(c cluster.Cluster, nodeNameGetter func() string) func(context.Context, *testing.T) bool {
	return func(_ context.Context, t *testing.T) bool {
		tc, ok := c.(*local.Cluster)
		require.True(t, ok)
		return assert.Eventually(t, func() bool {
			node := tc.GetStorageNodeByNodeName(t, nodeNameGetter())
			return !node.Running()
		}, 10*time.Minute, 10*time.Second)
	}
}
