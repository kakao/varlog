//go:build !k8s

package ee

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/tests/ee/cluster"
	"github.com/kakao/varlog/tests/ee/cluster/local"
)

func NewCluster(t *testing.T, opts ...cluster.Option) cluster.Cluster {
	cfg, err := cluster.NewConfig(t, opts...)
	require.NoError(t, err)
	return local.New(t, local.WithCommonConfig(cfg))
}
