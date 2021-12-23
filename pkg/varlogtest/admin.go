package varlogtest

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/proto/vmspb"
)

type testAdmin struct {
	vt *VarlogTest
}

var _ varlog.Admin = (*testAdmin)(nil)

func (c *testAdmin) lock() error {
	c.vt.cond.L.Lock()
	if c.vt.adminClientClosed {
		c.vt.cond.L.Unlock()
		return verrors.ErrClosed
	}
	return nil
}

func (c testAdmin) unlock() {
	c.vt.cond.L.Unlock()
}

func (c *testAdmin) AddStorageNode(ctx context.Context, addr string) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	// NOTE: Use UTC rather than local to use gogoproto's non-nullable stdtime.
	now := time.Now().UTC()
	storageNodeID := c.vt.generateStorageNodeID()
	storageNodeMetaDesc := varlogpb.StorageNodeMetadataDescriptor{
		ClusterID: c.vt.clusterID,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: storageNodeID,
				Address:       addr,
			},
			Status: varlogpb.StorageNodeStatusRunning,
			Storages: []*varlogpb.StorageDescriptor{
				{Path: "/tmp"},
			},
		},
		LogStreams:  nil,
		CreatedTime: now,
		UpdatedTime: now,
	}
	c.vt.storageNodes[storageNodeID] = storageNodeMetaDesc

	return proto.Clone(&storageNodeMetaDesc).(*varlogpb.StorageNodeMetadataDescriptor), nil
}

func (c *testAdmin) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	panic("not implemented")
}

func (c *testAdmin) AddTopic(ctx context.Context) (varlogpb.TopicDescriptor, error) {
	if err := c.lock(); err != nil {
		return varlogpb.TopicDescriptor{}, err
	}
	defer c.unlock()

	topicID := c.vt.generateTopicID()
	topicDesc := varlogpb.TopicDescriptor{
		TopicID:    topicID,
		Status:     varlogpb.TopicStatusRunning,
		LogStreams: nil,
	}
	c.vt.topics[topicID] = topicDesc

	invalidLogEntry := varlogpb.InvalidLogEntry()
	c.vt.globalLogEntries[topicID] = []*varlogpb.LogEntry{&invalidLogEntry}

	return *proto.Clone(&topicDesc).(*varlogpb.TopicDescriptor), nil
}

func (c *testAdmin) Topics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	ret := make([]varlogpb.TopicDescriptor, 0, len(c.vt.topics))
	for topicID := range c.vt.topics {
		topicDesc := c.vt.topics[topicID]
		ret = append(ret, *proto.Clone(&topicDesc).(*varlogpb.TopicDescriptor))
	}
	return ret, nil
}

func (c *testAdmin) DescribeTopic(ctx context.Context, topicID types.TopicID) (*vmspb.DescribeTopicResponse, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return nil, errors.New("no such topic")
	}

	rsp := &vmspb.DescribeTopicResponse{
		Topic:      *proto.Clone(&topicDesc).(*varlogpb.TopicDescriptor),
		LogStreams: make([]varlogpb.LogStreamDescriptor, len(topicDesc.LogStreams)),
	}
	for i, lsID := range topicDesc.LogStreams {
		lsDesc, ok := c.vt.logStreams[lsID]
		if !ok {
			panic(errors.Errorf("inconsistency: no logstream %d in topic %d", lsID, topicID))
		}
		rsp.LogStreams[i] = *proto.Clone(&lsDesc).(*varlogpb.LogStreamDescriptor)
	}
	return rsp, nil
}

func (c *testAdmin) UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error) {
	panic("not implemented")
}

func (c *testAdmin) AddLogStream(ctx context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return nil, errors.New("no such topic")
	}

	if len(c.vt.storageNodes) < c.vt.replicationFactor {
		return nil, errors.New("not enough storage nodes")
	}

	logStreamID := c.vt.generateLogStreamID()
	logStreamDesc := varlogpb.LogStreamDescriptor{
		LogStreamID: logStreamID,
		TopicID:     topicID,
		Status:      varlogpb.LogStreamStatusRunning,
		Replicas:    make([]*varlogpb.ReplicaDescriptor, c.vt.replicationFactor),
	}

	snIDs := c.vt.storageNodeIDs()
	for i, j := range c.vt.rng.Perm(len(snIDs))[:c.vt.replicationFactor] {
		snID := snIDs[j]
		logStreamDesc.Replicas[i] = &varlogpb.ReplicaDescriptor{
			StorageNodeID: c.vt.storageNodes[snID].StorageNode.StorageNodeID,
			Path:          c.vt.storageNodes[snID].StorageNode.Storages[0].Path,
		}
	}
	c.vt.logStreams[logStreamID] = logStreamDesc

	invalidLogEntry := varlogpb.InvalidLogEntry()
	c.vt.localLogEntries[logStreamID] = []*varlogpb.LogEntry{&invalidLogEntry}

	topicDesc.LogStreams = append(topicDesc.LogStreams, logStreamID)
	c.vt.topics[topicID] = topicDesc

	return proto.Clone(&logStreamDesc).(*varlogpb.LogStreamDescriptor), nil
}

func (c *testAdmin) UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (c *testAdmin) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (c *testAdmin) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica *varlogpb.ReplicaDescriptor, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) Seal(_ context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*vmspb.SealResponse, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	logStreamDesc, err := c.vt.logStreamDescriptor(topicID, logStreamID)
	if err != nil {
		return nil, err
	}

	_, tail := c.vt.peek(topicID, logStreamID)
	sealedGLSN := tail.GLSN

	logStreamDesc.Status = varlogpb.LogStreamStatusSealed
	c.vt.logStreams[logStreamID] = logStreamDesc

	rsp := &vmspb.SealResponse{
		LogStreams: make([]varlogpb.LogStreamMetadataDescriptor, len(logStreamDesc.Replicas)),
		SealedGLSN: sealedGLSN,
	}
	for i, replica := range logStreamDesc.Replicas {
		rsp.LogStreams[i] = varlogpb.LogStreamMetadataDescriptor{
			StorageNodeID: replica.StorageNodeID,
			LogStreamID:   logStreamID,
			TopicID:       topicID,
			Status:        logStreamDesc.Status,
			Version:       c.vt.version,
			HighWatermark: c.vt.globalHighWatermark(topicID),
			Path:          replica.Path,
		}
	}

	return rsp, nil
}

func (c *testAdmin) Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	logStreamDesc, err := c.vt.logStreamDescriptor(topicID, logStreamID)
	if err != nil {
		return nil, err
	}

	logStreamDesc.Status = varlogpb.LogStreamStatusRunning
	c.vt.logStreams[logStreamID] = logStreamDesc

	return proto.Clone(&logStreamDesc).(*varlogpb.LogStreamDescriptor), nil
}

func (c *testAdmin) Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID) (*snpb.SyncStatus, error) {
	panic("not implemented")
}

func (c *testAdmin) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	panic("not implemented")
}

func (c *testAdmin) AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error) {
	panic("not implemented")
}

func (c *testAdmin) RemoveMRPeer(ctx context.Context, raftURL string) error {
	panic("not implemented")
}

func (c *testAdmin) GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]string, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	ret := make(map[types.StorageNodeID]string)
	for snID, snMetaDesc := range c.vt.storageNodes {
		ret[snID] = snMetaDesc.StorageNode.Address
	}
	return ret, nil
}

func (c *testAdmin) Close() error {
	c.vt.cond.L.Lock()
	defer c.vt.cond.L.Unlock()
	c.vt.adminClientClosed = true
	return nil
}
