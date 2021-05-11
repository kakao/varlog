package it

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVarlogRegisterStorageNode(t *testing.T) {
	env := NewVarlogCluster(t,
		WithNumberOfStorageNodes(1),
		WithNumberOfLogStreams(1),
	)
	defer env.Close(t)

	// TODO (jun): use VMS to get metadata
	mr := env.GetMR(t)
	meta, err := mr.GetMetadata(context.TODO())
	require.NoError(t, err)

	require.Len(t, meta.GetStorageNodes(), 1)
	require.Len(t, meta.GetLogStreams(), 1)
	require.Eventually(t, func() bool {
		return mr.GetReportCount() > 0
	}, time.Second, 10*time.Millisecond)
}
