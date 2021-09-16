package storagenode

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type testStorageNode struct {
	sn *StorageNode
	wg sync.WaitGroup
}

func newTestStorageNode(t *testing.T, snID types.StorageNodeID, numVolumes int) *testStorageNode {
	volumes := make([]string, 0, numVolumes)
	for i := 0; i < numVolumes; i++ {
		volumes = append(volumes, t.TempDir())
	}

	sn, err := New(context.Background(),
		WithListenAddress("localhost:0"),
		WithVolumes(volumes...),
		WithStorageNodeID(snID),
	)
	require.NoError(t, err)

	tsn := &testStorageNode{sn: sn}

	tsn.wg.Add(1)
	go func() {
		defer tsn.wg.Done()
		err := tsn.sn.Run()
		assert.NoError(t, err)
	}()

	// wait for listening
	assert.Eventually(t, func() bool {
		snmd, err := tsn.sn.GetMetadata(context.Background())
		if err != nil {
			return false
		}
		return len(snmd.GetStorageNode().GetAddress()) > 0
	}, time.Second, 10*time.Millisecond)

	return tsn
}

func (tsn *testStorageNode) close() {
	tsn.sn.Close()
	tsn.wg.Wait()
}
