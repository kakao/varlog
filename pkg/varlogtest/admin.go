package varlogtest

import (
	"context"
	"path/filepath"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/internal/storagenode/volume"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/admpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
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

func (c *testAdmin) GetStorageNode(context.Context, types.StorageNodeID, ...varlog.AdminCallOption) (*admpb.StorageNodeMetadata, error) {
	panic("not implemented")
}

func (c *testAdmin) ListStorageNodes(ctx context.Context, opts ...varlog.AdminCallOption) ([]admpb.StorageNodeMetadata, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	ret := make([]admpb.StorageNodeMetadata, 0, len(c.vt.storageNodes))
	for snID := range c.vt.storageNodes {
		snmd := c.vt.storageNodes[snID]

		for _, lsd := range c.vt.logStreams {
			for _, rd := range lsd.Replicas {
				if rd.StorageNodeID == snID {
					snmd.LogStreamReplicas = append(snmd.LogStreamReplicas,
						snpb.LogStreamReplicaMetadataDescriptor{
							LogStreamReplica: varlogpb.LogStreamReplica{
								StorageNode: snmd.StorageNode,
								TopicLogStream: varlogpb.TopicLogStream{
									TopicID:     lsd.TopicID,
									LogStreamID: lsd.LogStreamID,
								},
							},
						},
					)
				}
			}
		}

		ret = append(ret, admpb.StorageNodeMetadata{
			StorageNodeMetadataDescriptor: snmd,
			LastHeartbeatTime:             time.Now().UTC(),
		})
	}

	return ret, nil
}
func (c *testAdmin) GetStorageNodes(ctx context.Context, opts ...varlog.AdminCallOption) (map[types.StorageNodeID]admpb.StorageNodeMetadata, error) {
	snms, err := c.ListStorageNodes(ctx)
	if err != nil {
		return nil, err
	}
	ret := make(map[types.StorageNodeID]admpb.StorageNodeMetadata, len(snms))
	for _, snm := range snms {
		ret[snm.StorageNode.StorageNodeID] = snm
	}
	return ret, nil
}

func (c *testAdmin) AddStorageNode(_ context.Context, snid types.StorageNodeID, addr string, _ ...varlog.AdminCallOption) (*admpb.StorageNodeMetadata, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	// NOTE: Use UTC rather than local to use gogoproto's non-nullable stdtime.
	now := time.Now().UTC()
	if snid.Invalid() {
		snid = c.vt.generateStorageNodeID()
	}

	snpath := filepath.Join("/tmp", volume.StorageNodeDirName(c.vt.clusterID, snid))
	storageNodeMetaDesc := snpb.StorageNodeMetadataDescriptor{
		ClusterID: c.vt.clusterID,
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
			Address:       addr,
		},
		Status: varlogpb.StorageNodeStatusRunning,
		Storages: []varlogpb.StorageDescriptor{
			{Path: snpath},
		},
		LogStreamReplicas: nil,
		StartTime:         now,
	}
	c.vt.storageNodes[snid] = storageNodeMetaDesc

	return &admpb.StorageNodeMetadata{
		StorageNodeMetadataDescriptor: *proto.Clone(&storageNodeMetaDesc).(*snpb.StorageNodeMetadataDescriptor),
		CreateTime:                    now,
		LastHeartbeatTime:             now,
	}, nil
}

func (c *testAdmin) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID, opts ...varlog.AdminCallOption) error {
	if err := c.lock(); err != nil {
		return err
	}
	defer c.unlock()

	storageNodeMetaDesc := c.vt.storageNodes[storageNodeID]
	if len(storageNodeMetaDesc.LogStreamReplicas) != 0 {
		return errors.New("not empty")
	}

	delete(c.vt.storageNodes, storageNodeID)

	return nil
}

func (c *testAdmin) GetTopic(ctx context.Context, tpid types.TopicID, opts ...varlog.AdminCallOption) (*varlogpb.TopicDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) ListTopics(ctx context.Context, opts ...varlog.AdminCallOption) ([]varlogpb.TopicDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) Topics(ctx context.Context, opts ...varlog.AdminCallOption) ([]varlogpb.TopicDescriptor, error) {
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

func (c *testAdmin) AddTopic(ctx context.Context, opts ...varlog.AdminCallOption) (*varlogpb.TopicDescriptor, error) {
	if err := c.lock(); err != nil {
		return nil, err
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

	return proto.Clone(&topicDesc).(*varlogpb.TopicDescriptor), nil
}

func (c *testAdmin) UnregisterTopic(ctx context.Context, topicID types.TopicID, opts ...varlog.AdminCallOption) error {
	panic("not implemented")
}

func (c *testAdmin) GetLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, opts ...varlog.AdminCallOption) (*varlogpb.LogStreamDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) ListLogStreams(ctx context.Context, tpid types.TopicID, opts ...varlog.AdminCallOption) ([]varlogpb.LogStreamDescriptor, error) {
	panic("not implemented")
}

func (c *testAdmin) DescribeTopic(ctx context.Context, topicID types.TopicID, opts ...varlog.AdminCallOption) (*admpb.DescribeTopicResponse, error) {
	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return nil, errors.New("no such topic")
	}

	rsp := &admpb.DescribeTopicResponse{
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

func (c *testAdmin) AddLogStream(_ context.Context, topicID types.TopicID, logStreamReplicas []*varlogpb.ReplicaDescriptor, opts ...varlog.AdminCallOption) (*varlogpb.LogStreamDescriptor, error) {
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
	lsd := varlogpb.LogStreamDescriptor{
		LogStreamID: logStreamID,
		TopicID:     topicID,
		Status:      varlogpb.LogStreamStatusRunning,
		Replicas:    make([]*varlogpb.ReplicaDescriptor, c.vt.replicationFactor),
	}

	if logStreamReplicas == nil {
		snids := c.vt.storageNodeIDs()
		for i, j := range c.vt.rng.Perm(len(snids))[:c.vt.replicationFactor] {
			snid := snids[j]
			snpath := c.vt.storageNodes[snid].Storages[0].Path
			dataPath := filepath.Join(snpath, volume.LogStreamDirName(topicID, logStreamID))
			lsd.Replicas[i] = &varlogpb.ReplicaDescriptor{
				StorageNodeID:   c.vt.storageNodes[snid].StorageNode.StorageNodeID,
				StorageNodePath: c.vt.storageNodes[snid].Storages[0].Path,
				DataPath:        dataPath,
			}
		}
	} else {
		lsd.Replicas = logStreamReplicas
	}

	c.vt.logStreams[logStreamID] = lsd

	invalidLogEntry := varlogpb.InvalidLogEntry()
	c.vt.localLogEntries[logStreamID] = []*varlogpb.LogEntry{&invalidLogEntry}

	topicDesc.LogStreams = append(topicDesc.LogStreams, logStreamID)
	c.vt.topics[topicID] = topicDesc

	return proto.Clone(&lsd).(*varlogpb.LogStreamDescriptor), nil
}

func (c *testAdmin) UpdateLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, poppedReplica varlogpb.ReplicaDescriptor, pushedReplica varlogpb.ReplicaDescriptor, opts ...varlog.AdminCallOption) (*varlogpb.LogStreamDescriptor, error) {
	if poppedReplica.StorageNodeID == pushedReplica.StorageNodeID {
		if poppedReplica.StorageNodePath != pushedReplica.StorageNodePath {
			return nil, status.Errorf(codes.Unimplemented, "update log stream: moving data directory")
		}
		return nil, status.Errorf(codes.InvalidArgument, "update log stream: the same replica")
	}

	if err := c.lock(); err != nil {
		return nil, err
	}
	defer c.unlock()

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return nil, status.Errorf(codes.NotFound, "update log stream: no such topic %d", topicID)
	}

	logStreamDesc, ok := c.vt.logStreams[logStreamID]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "update log stream: no such log stream %d", logStreamID)
	}

	popIdx, pushIdx := -1, -1
	for idx := range logStreamDesc.Replicas {
		snid := logStreamDesc.Replicas[idx].StorageNodeID
		if snid == poppedReplica.StorageNodeID {
			popIdx = idx
		} else if snid == pushedReplica.StorageNodeID {
			pushIdx = idx
		}
	}
	if popIdx < 0 && pushIdx >= 0 { // already updated
		return proto.Clone(&logStreamDesc).(*varlogpb.LogStreamDescriptor), nil
	}
	if popIdx < 0 && pushIdx < 0 {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"update log stream: no victim replica (snid=%v) in log stream %+v",
			poppedReplica.StorageNodeID,
			logStreamDesc.Replicas,
		)
	}
	if popIdx >= 0 && pushIdx >= 0 {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"update log stream: victim replica and new replica already exist in log stream %+v",
			logStreamDesc.Replicas,
		)
	}

	logStreamDesc.Replicas[popIdx] = &pushedReplica
	c.vt.logStreams[logStreamID] = logStreamDesc

	return proto.Clone(&logStreamDesc).(*varlogpb.LogStreamDescriptor), nil
}

func (c *testAdmin) UnregisterLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, opts ...varlog.AdminCallOption) error {
	panic("not implemented")
}

func (c *testAdmin) RemoveLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID, opts ...varlog.AdminCallOption) error {
	if err := c.lock(); err != nil {
		return err
	}
	defer c.unlock()

	_, ok := c.vt.storageNodes[storageNodeID]
	if !ok {
		return errors.New("no such storage node")
	}

	topicDesc, ok := c.vt.topics[topicID]
	if !ok || topicDesc.Status.Deleted() {
		return errors.New("no such topic")
	}

	logStreamDesc, ok := c.vt.logStreams[logStreamID]
	if !ok {
		return errors.New("no such logstream")
	}

	found := false
	for i, r := range logStreamDesc.Replicas {
		if r.StorageNodeID == storageNodeID {
			logStreamDesc.Replicas = append(logStreamDesc.Replicas[:i], logStreamDesc.Replicas[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return nil
	}

	c.vt.logStreams[logStreamID] = logStreamDesc

	return nil
}

func (c *testAdmin) Seal(_ context.Context, topicID types.TopicID, logStreamID types.LogStreamID, opts ...varlog.AdminCallOption) (*admpb.SealResponse, error) {
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

	rsp := &admpb.SealResponse{
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
			Path: replica.StorageNodePath,
		}
	}

	return rsp, nil
}

func (c *testAdmin) Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, opts ...varlog.AdminCallOption) (*varlogpb.LogStreamDescriptor, error) {
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

func (c *testAdmin) Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcStorageNodeID, dstStorageNodeID types.StorageNodeID, opts ...varlog.AdminCallOption) (*snpb.SyncStatus, error) {
	panic("not implemented")
}

func (c *testAdmin) Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN, opts ...varlog.AdminCallOption) (map[types.LogStreamID]map[types.StorageNodeID]error, error) {
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

func (c *testAdmin) GetMetadataRepositoryNode(ctx context.Context, nid types.NodeID, opts ...varlog.AdminCallOption) (*varlogpb.MetadataRepositoryNode, error) {
	panic("not implemented")
}

func (c *testAdmin) ListMetadataRepositoryNodes(ctx context.Context, opts ...varlog.AdminCallOption) ([]varlogpb.MetadataRepositoryNode, error) {
	panic("not implemented")
}

func (c *testAdmin) GetMRMembers(ctx context.Context, opts ...varlog.AdminCallOption) (*admpb.GetMRMembersResponse, error) {
	panic("not implemented")
}

func (c *testAdmin) AddMetadataRepositoryNode(ctx context.Context, raftURL, rpcAddr string, opts ...varlog.AdminCallOption) (*varlogpb.MetadataRepositoryNode, error) {
	panic("not implemented")
}

func (c *testAdmin) AddMRPeer(ctx context.Context, raftURL, rpcAddr string, opts ...varlog.AdminCallOption) (types.NodeID, error) {
	panic("not implemented")
}

func (c *testAdmin) DeleteMetadataRepositoryNode(ctx context.Context, nid types.NodeID, opts ...varlog.AdminCallOption) error {
	panic("not implemented")
}

func (c *testAdmin) RemoveMRPeer(ctx context.Context, raftURL string, opts ...varlog.AdminCallOption) error {
	panic("not implemented")
}

func (c *testAdmin) Close() error {
	c.vt.cond.L.Lock()
	defer c.vt.cond.L.Unlock()
	c.vt.adminClientClosed = true
	return nil
}
