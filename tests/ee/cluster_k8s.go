//go:build k8s

package ee

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/tests/ee/cluster/k8s"

	"github.com/kakao/varlog/tests/ee/cluster"
)

func NewCluster(t *testing.T, opts ...cluster.Option) cluster.Cluster {
	cfg, err := cluster.NewConfig(t, opts...)
	require.NoError(t, err)
	return k8s.New(t, k8s.WithCommonConfig(cfg))
}
