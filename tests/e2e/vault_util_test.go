//go:build e2e
// +build e2e

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetVarlogK8sConnInfo(t *testing.T) {
	data, err := getVarlogK8sConnInfo()
	require.NoError(t, err)
	require.Contains(t, data, MasterURL)
	require.Contains(t, data, Cluster)
	require.Contains(t, data, Context)
	require.Contains(t, data, User)
	require.Contains(t, data, Token)
}
