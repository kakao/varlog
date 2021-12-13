package varlogtest

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"

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
	cond             *sync.Cond
	storageNodes     map[types.StorageNodeID]varlogpb.StorageNodeMetadataDescriptor
	logStreams       map[types.LogStreamID]varlogpb.LogStreamDescriptor
	topics           map[types.TopicID]varlogpb.TopicDescriptor
	globalLogEntries map[types.TopicID][]*varlogpb.LogEntry
	localLogEntries  map[types.LogStreamID][]*varlogpb.LogEntry
	version          types.Version

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
	vt.cond = sync.NewCond(&vt.mu)
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

func (vt *VarlogTest) topicDescriptor(topicID types.TopicID) (varlogpb.TopicDescriptor, error) {
	topicDesc, ok := vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return varlogpb.TopicDescriptor{}, errors.New("no such topic")
	}
	if len(topicDesc.LogStreams) == 0 {
		return varlogpb.TopicDescriptor{}, errors.New("no log stream")
	}
	return topicDesc, nil
}

func (vt *VarlogTest) logStreamDescriptor(topicID types.TopicID, logStreamID types.LogStreamID) (varlogpb.LogStreamDescriptor, error) {
	topicDesc, err := vt.topicDescriptor(topicID)
	if err != nil {
		return varlogpb.LogStreamDescriptor{}, err
	}
	if !topicDesc.HasLogStream(logStreamID) {
		return varlogpb.LogStreamDescriptor{}, errors.New("no such log stream in the topic")
	}

	logStreamDesc, ok := vt.logStreams[logStreamID]
	if !ok {
		return varlogpb.LogStreamDescriptor{}, errors.New("no such log stream in the topic")
	}
	return logStreamDesc, nil
}

func (vt *VarlogTest) peek(topicID types.TopicID, logStreamID types.LogStreamID) (head varlogpb.LogEntryMeta, tail varlogpb.LogEntryMeta) {
	head.TopicID = topicID
	head.LogStreamID = logStreamID
	tail.TopicID = topicID
	tail.LogStreamID = logStreamID

	if len(vt.localLogEntries[logStreamID]) < 2 {
		return
	}

	head.GLSN = vt.localLogEntries[logStreamID][1].GLSN
	head.LLSN = vt.localLogEntries[logStreamID][1].LLSN
	lastIdx := len(vt.localLogEntries[logStreamID]) - 1
	tail.GLSN = vt.localLogEntries[logStreamID][lastIdx].GLSN
	tail.LLSN = vt.localLogEntries[logStreamID][lastIdx].LLSN
	return head, tail
}

func (vt *VarlogTest) globalHighWatermark(topicID types.TopicID) types.GLSN {
	lastIdx := len(vt.globalLogEntries[topicID]) - 1
	return vt.globalLogEntries[topicID][lastIdx].GLSN
}
