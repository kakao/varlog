package varlogtest

import (
	"math/rand"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type VarlogTest struct {
	admin *testAdmin
	vlg   *testLog

	clusterID         types.ClusterID
	replicationFactor int

	rng *rand.Rand

	mu               sync.Mutex
	storageNodes     map[types.StorageNodeID]varlogpb.StorageNodeMetadataDescriptor
	logStreams       map[types.LogStreamID]varlogpb.LogStreamDescriptor
	topics           map[types.TopicID]varlogpb.TopicDescriptor
	globalLogEntries map[types.TopicID][]*varlogpb.LogEntry
	localLogEntries  map[types.LogStreamID][]*varlogpb.LogEntry

	nextTopicID       types.TopicID
	nextStorageNodeID types.StorageNodeID
	nextLogStreamID   types.LogStreamID

	adminClientClosed  bool
	varlogClientClosed bool
}

func New(clusterID types.ClusterID, replicationFactor int) *VarlogTest {
	vt := &VarlogTest{
		clusterID:         clusterID,
		replicationFactor: replicationFactor,
		rng:               rand.New(rand.NewSource(time.Now().UnixMilli())),
		storageNodes:      make(map[types.StorageNodeID]varlogpb.StorageNodeMetadataDescriptor),
		logStreams:        make(map[types.LogStreamID]varlogpb.LogStreamDescriptor),
		topics:            make(map[types.TopicID]varlogpb.TopicDescriptor),
		globalLogEntries:  make(map[types.TopicID][]*varlogpb.LogEntry),
		localLogEntries:   make(map[types.LogStreamID][]*varlogpb.LogEntry),
	}
	vt.admin = &testAdmin{vt: vt}
	vt.vlg = &testLog{vt: vt}
	return vt
}

func (vt *VarlogTest) Admin() varlog.Admin {
	return vt.admin
}

func (vt *VarlogTest) Log() varlog.Log {
	return vt.vlg
}

func (vt *VarlogTest) generateTopicID() types.TopicID {
	vt.nextTopicID++
	return vt.nextTopicID
}

func (vt *VarlogTest) generateStorageNodeID() types.StorageNodeID {
	vt.nextStorageNodeID++
	return vt.nextStorageNodeID
}

func (vt *VarlogTest) generateLogStreamID() types.LogStreamID {
	vt.nextLogStreamID++
	return vt.nextLogStreamID
}

func (vt *VarlogTest) storageNodeIDs() []types.StorageNodeID {
	snIDs := make([]types.StorageNodeID, 0, len(vt.storageNodes))
	for snID := range vt.storageNodes {
		snIDs = append(snIDs, snID)
	}
	return snIDs
}
