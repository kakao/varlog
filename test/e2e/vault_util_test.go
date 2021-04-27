// +build e2e

package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetVarlogK8sConnInfo(t *testing.T) {
	data, err := getVarlogK8sConnInfo()
	require.NoError(t, err)
	require.Contains(t, data, E2E_MASTERURL)
	require.Contains(t, data, E2E_CLUSTER)
	require.Contains(t, data, E2E_CONTEXT)
	require.Contains(t, data, E2E_USER)
	require.Contains(t, data, E2E_TOKEN)
}
