package varlogtest

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
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

func (c *testAdmin) GetStorageNode(context.Context, types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) ListStorageNodes(ctx context.Context) (map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	ret := make(map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor)
	for snID := range c.vt.storageNodes {
		snmd := c.vt.storageNodes[snID]
		ret[snID] = &snmd
	}
	return ret, nil
}
func (c *testAdmin) GetStorageNodes(ctx context.Context) (map[types.StorageNodeID]*snpb.StorageNodeMetadataDescriptor, error) {
	return c.ListStorageNodes(ctx)
}

// FIXME: Argument snid
func (c *testAdmin) AddStorageNode(ctx context.Context, _ types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	// NOTE: Use UTC rather than local to use gogoproto's non-nullable stdtime.
	now := time.Now().UTC()
	storageNodeID := c.vt.generateStorageNodeID()
	storageNodeMetaDesc := snpb.StorageNodeMetadataDescriptor{
		ClusterID: c.vt.clusterID,
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: storageNodeID,
			Address:       addr,
		},
		Status: varlogpb.StorageNodeStatusRunning,
		Storages: []varlogpb.StorageDescriptor{
			{Path: "/tmp"},
		},
		LogStreamReplicas: nil,
		CreatedTime:       now,
		UpdatedTime:       now,
	}
	c.vt.storageNodes[storageNodeID] = storageNodeMetaDesc

	return proto.Clone(&storageNodeMetaDesc).(*snpb.StorageNodeMetadataDescriptor), nil
}

func (c *testAdmin) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	panic("not implemented")
}

func (c *testAdmin) GetTopic(ctx context.Context, tpid types.TopicID) (*varlogpb.TopicDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) ListTopics(ctx context.Context) ([]varlogpb.TopicDescriptor, error) {
	panic("not implemented")
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
	c.vt.trimGLSNs[topicID] = types.InvalidGLSN

	invalidLogEntry := varlogpb.InvalidLogEntry()
	c.vt.globalLogEntries[topicID] = []*varlogpb.LogEntry{&invalidLogEntry}

	return *proto.Clone(&topicDesc).(*varlogpb.TopicDescriptor), nil
}

func (c *testAdmin) UnregisterTopic(ctx context.Context, topicID types.TopicID) (*vmspb.UnregisterTopicResponse, error) {
	panic("not implemented")
}

func (c *testAdmin) GetLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) ListLogStreams(ctx context.Context, tpid types.TopicID) ([]*varlogpb.LogStreamDescriptor, error) {
	panic("not implemented")
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
			Path:          c.vt.storageNodes[snID].Storages[0].Path,
		}
	}
	c.vt.logStreams[logStreamID] = logStreamDesc

	invalidLogEntry := varlogpb.InvalidLogEntry()
	c.vt.localLogEntries[logStreamID] = []*varlogpb.LogEntry{&invalidLogEntry}

	topicDesc.LogStreams = append(topicDesc.LogStreams, logStreamID)
	c.vt.topics[topicID] = topicDesc

	return proto.Clone(&logStreamDesc).(*varlogpb.LogStreamDescriptor), nil
}

func (c *testAdmin) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica *varlogpb.ReplicaDescriptor, pushedReplica *varlogpb.ReplicaDescriptor) (*varlogpb.LogStreamDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (c *testAdmin) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error {
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
		LogStreams: make([]snpb.LogStreamReplicaMetadataDescriptor, len(logStreamDesc.Replicas)),
		SealedGLSN: sealedGLSN,
	}
	for i, replica := range logStreamDesc.Replicas {
		rsp.LogStreams[i] = snpb.LogStreamReplicaMetadataDescriptor{
			LogStreamReplica: varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: replica.StorageNodeID,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     topicID,
					LogStreamID: logStreamID,
				},
			},
			Status:  logStreamDesc.Status,
			Version: c.vt.version,
			LocalHighWatermark: varlogpb.LogSequenceNumber{
				GLSN: c.vt.globalHighWatermark(topicID),
			},
			Path: replica.Path,
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

func (c *testAdmin) Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]map[types.StorageNodeID]error, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	td, err := c.vt.topicDescriptor(topicID)
	if err != nil {
		return nil, err
	}

	ret := make(map[types.LogStreamID]map[types.StorageNodeID]error)
	for _, lsid := range td.LogStreams {
		lsd, err := c.vt.logStreamDescriptor(topicID, lsid)
		if err != nil {
			return nil, err
		}
		if _, ok := ret[lsid]; !ok {
			ret[lsid] = make(map[types.StorageNodeID]error)
		}
		for _, rd := range lsd.Replicas {
			snid := rd.StorageNodeID
			ret[lsid][snid] = nil
		}
	}
	c.vt.trimGLSNs[topicID] = lastGLSN
	return ret, nil
}

func (c *testAdmin) GetMetadataRepositoryNode(ctx context.Context, nid types.NodeID) (*varlogpb.MetadataRepositoryNode, error) {
	panic("not implemented")
}

func (c *testAdmin) ListMetadataRepositoryNodes(ctx context.Context) ([]*varlogpb.MetadataRepositoryNode, error) {
	panic("not implemented")
}

func (c *testAdmin) GetMRMembers(ctx context.Context) (*vmspb.GetMRMembersResponse, error) {
	panic("not implemented")
}

func (c *testAdmin) AddMetadataRepositoryNode(ctx context.Context, raftURL, rpcAddr string) (*varlogpb.MetadataRepositoryNode, error) {
	panic("not implemented")
}

func (c *testAdmin) AddMRPeer(ctx context.Context, raftURL, rpcAddr string) (types.NodeID, error) {
	panic("not implemented")
}

func (c *testAdmin) DeleteMetadataRepositoryNode(ctx context.Context, nid types.NodeID) error {
	panic("not implemented")
}

func (c *testAdmin) RemoveMRPeer(ctx context.Context, raftURL string) error {
	panic("not implemented")
}

func (c *testAdmin) Close() error {
	c.vt.cond.L.Lock()
	defer c.vt.cond.L.Unlock()
	c.vt.adminClientClosed = true
	return nil
}
