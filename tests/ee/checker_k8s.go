//go:build k8s

package ee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/tests/ee/cluster"
	"github.com/kakao/varlog/tests/ee/cluster/k8s"
)

func WaitStorageNodeFail(c cluster.Cluster, nodeNameGetter func() string) func(context.Context, *testing.T) bool {
	return func(ctx context.Context, t *testing.T) bool {
		tc, ok := c.(*k8s.Cluster)
		require.True(t, ok)
		return assert.Eventually(t, func() bool {
			pods := tc.ListStorageNodePods(ctx, t)
			for _, pod := range pods {
				if pod.Spec.NodeName == nodeNameGetter() {
					return false
				}
			}
			return true
		}, 10*time.Minute, 10*time.Second)
	}
}
