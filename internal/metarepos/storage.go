package metarepos

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/mathutil"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type jobSnapshot struct {
	appliedIndex uint64
}

type jobMetadataCache struct {
	nodeIndex    uint64
	requestIndex uint64
}

type storageAsyncJob struct {
	job interface{}
}

type committedEntry struct {
	leader    uint64
	entry     *mrpb.RaftEntry
	confState *raftpb.ConfState
}

type SnapshotGetter interface {
	GetSnapshotIndex() uint64

	GetSnapshot() ([]byte, *raftpb.ConfState, uint64)
}

type Membership interface {
	SetLeader(types.NodeID)

	Leader() types.NodeID

	AddPeer(types.NodeID, string, bool, *raftpb.ConfState, uint64) error

	RemovePeer(types.NodeID, *raftpb.ConfState, uint64) error

	GetPeers() *mrpb.MetadataRepositoryDescriptor_PeerDescriptorMap

	IsMember(types.NodeID) bool

	IsLearner(types.NodeID) bool

	Clear()
}

type TopicLSID struct {
	TopicID     types.TopicID
	LogStreamID types.LogStreamID
}

// TODO:: refactoring
type MetadataStorage struct {
	// make orig immutable
	// and entries are applied to diff
	// while copyOnWrite set true
	origStateMachine *mrpb.MetadataRepositoryDescriptor
	diffStateMachine *mrpb.MetadataRepositoryDescriptor
	copyOnWrite      atomicutil.AtomicBool

	sortedTopicLSIDs []TopicLSID

	// snapshot orig for raft
	snap      []byte
	snapCount uint64

	// the largest index of the entry already applied to the snapshot
	snapIndex uint64

	// confState for snapshot
	snapConfState atomic.Value

	// confState for origStateMachine
	origConfState *raftpb.ConfState
	// confState for diffStateMachine
	diffConfState *raftpb.ConfState

	// the largest index of the entry already applied to the stateMachine
	appliedIndex uint64

	// immutable metadata cache for client request
	metaCache *varlogpb.MetadataDescriptor
	// change of metadata sequence number
	metaAppliedIndex uint64
	// callback after cache is completed
	cacheCompleteCB func(uint64, uint64, error)

	// The membership leader is not permanent to check whether the cluster is joined.
	leader uint64

	// number of running async job
	nrRunning           int64
	nrUpdateSinceCommit uint64

	lsMu sync.RWMutex // mutex for GlobalLogStream
	mtMu sync.RWMutex // mutex for Metadata
	prMu sync.RWMutex // mutex for Peers
	ssMu sync.RWMutex // mutex for Snapshot
	mcMu sync.RWMutex // mutex for Metadata Cache

	// async job (snapshot, cache)
	jobC chan *storageAsyncJob

	runner  *runner.Runner
	running atomicutil.AtomicBool

	logger *zap.Logger
}

func NewMetadataStorage(cb func(uint64, uint64, error), snapCount uint64, logger *zap.Logger) *MetadataStorage {
	if logger == nil {
		logger, _ = zap.NewDevelopment()
		logger = logger.Named("storage")
	}

	ms := &MetadataStorage{
		cacheCompleteCB: cb,
		logger:          logger,
	}
	ms.snapCount = snapCount

	ms.origStateMachine = &mrpb.MetadataRepositoryDescriptor{}
	ms.origStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	ms.origStateMachine.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	ms.origStateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)

	ms.diffStateMachine = &mrpb.MetadataRepositoryDescriptor{}
	ms.diffStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	ms.diffStateMachine.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	ms.diffStateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)

	ms.origStateMachine.PeersMap.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	ms.diffStateMachine.PeersMap.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)

	ms.origStateMachine.Endpoints = make(map[types.NodeID]string)
	ms.diffStateMachine.Endpoints = make(map[types.NodeID]string)

	ms.metaCache = &varlogpb.MetadataDescriptor{}

	ms.jobC = make(chan *storageAsyncJob, 4096)
	ms.running.Store(false)
	ms.releaseCopyOnWrite()

	return ms
}

func (ms *MetadataStorage) Run() {
	if !ms.running.Load() {
		ms.runner = runner.New("mr-storage", zap.NewNop())
		ms.runner.Run(ms.processSnapshot) //nolint:errcheck,revive // TODO:: Handle an error returned.
		ms.running.Store(true)
	}
}

func (ms *MetadataStorage) Close() {
	if ms.running.Load() {
		ms.runner.Stop()
		ms.running.Store(false)
	}
}

func (ms *MetadataStorage) processSnapshot(ctx context.Context) {
Loop:
	for {
		select {
		case f := <-ms.jobC:
			switch r := f.job.(type) {
			case *jobSnapshot:
				ms.createSnapshot(r)
			case *jobMetadataCache:
				ms.createMetadataCache(r)
			}

			atomic.AddInt64(&ms.nrRunning, -1)
		case <-ctx.Done():
			break Loop
		}
	}

	close(ms.jobC)

	for f := range ms.jobC {
		switch r := f.job.(type) {
		case *jobMetadataCache:
			if ms.cacheCompleteCB != nil {
				ms.cacheCompleteCB(r.nodeIndex, r.requestIndex, ctx.Err())
			}
		}

		atomic.AddInt64(&ms.nrRunning, -1)
	}
}

func (ms *MetadataStorage) lookupStorageNode(snID types.StorageNodeID) *varlogpb.StorageNodeDescriptor {
	pre, cur := ms.getStateMachine()
	sn := cur.Metadata.GetStorageNode(snID)
	if sn != nil {
		if sn.Status.Deleted() {
			return nil
		}
		return sn
	}

	if pre == cur {
		return nil
	}

	return pre.Metadata.GetStorageNode(snID)
}

func (ms *MetadataStorage) LookupStorageNode(snID types.StorageNodeID) *varlogpb.StorageNodeDescriptor {
	ms.mtMu.RLock()
	defer ms.mtMu.RUnlock()

	return ms.lookupStorageNode(snID)
}

func (ms *MetadataStorage) lookupLogStream(lsID types.LogStreamID) *varlogpb.LogStreamDescriptor {
	pre, cur := ms.getStateMachine()
	ls := cur.Metadata.GetLogStream(lsID)
	if ls != nil {
		if ls.Status.Deleted() {
			return nil
		}
		return ls
	}

	if pre == cur {
		return nil
	}

	return pre.Metadata.GetLogStream(lsID)
}

func (ms *MetadataStorage) LookupLogStream(lsID types.LogStreamID) *varlogpb.LogStreamDescriptor {
	ms.mtMu.RLock()
	defer ms.mtMu.RUnlock()

	return ms.lookupLogStream(lsID)
}

func (ms *MetadataStorage) lookupTopic(topicID types.TopicID) *varlogpb.TopicDescriptor {
	pre, cur := ms.getStateMachine()
	topic := cur.Metadata.GetTopic(topicID)
	if topic != nil {
		if topic.Status.Deleted() {
			return nil
		}
		return topic
	}

	if pre == cur {
		return nil
	}

	return pre.Metadata.GetTopic(topicID)
}

func (ms *MetadataStorage) registerStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
	old := ms.lookupStorageNode(sn.StorageNodeID)
	equal := old.Equal(sn)
	if old != nil && !equal {
		return verrors.ErrAlreadyExists
	}

	if equal {
		// To ensure that it is applied to the meta cache
		return nil
	}

	_, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	err := cur.Metadata.UpsertStorageNode(sn)
	if err != nil {
		return err
	}

	ms.metaAppliedIndex++
	return err
}

func (ms *MetadataStorage) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor, nodeIndex, requestIndex uint64) error {
	err := ms.registerStorageNode(sn)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) unregistableStorageNode(snID types.StorageNodeID) bool {
	for _, ls := range ms.GetLogStreams() {
		for _, r := range ls.Replicas {
			if r.StorageNodeID == snID {
				return false
			}
		}
	}

	return true
}

func (ms *MetadataStorage) unregisterStorageNode(snID types.StorageNodeID) error {
	if ms.lookupStorageNode(snID) == nil {
		return verrors.ErrNotExist
	}

	if !ms.unregistableStorageNode(snID) {
		return verrors.ErrInvalidArgument
	}

	pre, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	cur.Metadata.DeleteStorageNode(snID) //nolint:errcheck,revive // TODO:: Handle an error returned.
	if pre != cur {
		deleted := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
			Status: varlogpb.StorageNodeStatusDeleted,
		}

		cur.Metadata.InsertStorageNode(deleted) //nolint:errcheck,revive // TODO:: Handle an error returned.
	}

	ms.metaAppliedIndex++
	return nil
}

func (ms *MetadataStorage) insertSortedLSIDs(topicID types.TopicID, lsID types.LogStreamID) {
	i := sort.Search(len(ms.sortedTopicLSIDs), func(i int) bool {
		if ms.sortedTopicLSIDs[i].TopicID == topicID {
			return ms.sortedTopicLSIDs[i].LogStreamID >= lsID
		}

		return ms.sortedTopicLSIDs[i].TopicID > topicID
	})

	if i < len(ms.sortedTopicLSIDs) && ms.sortedTopicLSIDs[i].LogStreamID == lsID {
		return
	}

	ms.sortedTopicLSIDs = append(ms.sortedTopicLSIDs, TopicLSID{})
	copy(ms.sortedTopicLSIDs[i+1:], ms.sortedTopicLSIDs[i:])
	ms.sortedTopicLSIDs[i] = TopicLSID{
		TopicID:     topicID,
		LogStreamID: lsID,
	}
}

func (ms *MetadataStorage) deleteSortedLSIDs(topicID types.TopicID, lsID types.LogStreamID) {
	i := sort.Search(len(ms.sortedTopicLSIDs), func(i int) bool {
		if ms.sortedTopicLSIDs[i].TopicID == topicID {
			return ms.sortedTopicLSIDs[i].LogStreamID >= lsID
		}

		return ms.sortedTopicLSIDs[i].TopicID > topicID
	})

	if i < len(ms.sortedTopicLSIDs) && ms.sortedTopicLSIDs[i].LogStreamID == lsID {
		copy(ms.sortedTopicLSIDs[i:], ms.sortedTopicLSIDs[i+1:])
		ms.sortedTopicLSIDs = ms.sortedTopicLSIDs[:len(ms.sortedTopicLSIDs)-1]
	}
}

func (ms *MetadataStorage) UnregisterStorageNode(snID types.StorageNodeID, nodeIndex, requestIndex uint64) error {
	err := ms.unregisterStorageNode(snID)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) registerLogStream(ls *varlogpb.LogStreamDescriptor) error {
	if len(ls.Replicas) == 0 {
		return verrors.ErrInvalidArgument
	}

	topic := ms.lookupTopic(ls.TopicID)
	if topic == nil {
		return verrors.ErrInvalidArgument
	}

	for _, r := range ls.Replicas {
		if ms.lookupStorageNode(r.StorageNodeID) == nil {
			return verrors.ErrInvalidArgument
		}
	}

	old := ms.lookupLogStream(ls.LogStreamID)
	equal := old.Equal(ls)
	if old != nil && !equal {
		return verrors.ErrAlreadyExists
	}

	if equal {
		// To ensure that it is applied to the meta cache
		return nil
	}

	_, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	if err := cur.Metadata.UpsertLogStream(ls); err != nil {
		return err
	}

	lm := &mrpb.LogStreamUncommitReports{
		Replicas: make(map[types.StorageNodeID]snpb.LogStreamUncommitReport, len(ls.Replicas)),
		Status:   varlogpb.LogStreamStatusRunning,
	}

	if ls.Status.Sealed() {
		lm.Status = varlogpb.LogStreamStatusSealing
	}

	for _, r := range ls.Replicas {
		lm.Replicas[r.StorageNodeID] = snpb.LogStreamUncommitReport{
			LogStreamID:           ls.LogStreamID,
			Version:               ms.getLastCommitVersionNoLock(),
			UncommittedLLSNOffset: types.MinLLSN,
			UncommittedLLSNLength: 0,
		}
	}

	cur.LogStream.UncommitReports[ls.LogStreamID] = lm
	ms.insertSortedLSIDs(ls.TopicID, ls.LogStreamID)

	topic = proto.Clone(topic).(*varlogpb.TopicDescriptor)
	topic.InsertLogStream(ls.LogStreamID)
	if err := cur.Metadata.UpsertTopic(topic); err != nil {
		return err
	}

	ms.metaAppliedIndex++

	return nil
}

func (ms *MetadataStorage) RegisterLogStream(ls *varlogpb.LogStreamDescriptor, nodeIndex, requestIndex uint64) error {
	err := ms.registerLogStream(ls)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) unregisterLogStream(lsID types.LogStreamID) error {
	ls := ms.lookupLogStream(lsID)
	if ls == nil {
		return verrors.ErrNotExist
	}

	pre, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	cur.Metadata.DeleteLogStream(lsID) //nolint:errcheck,revive // TODO:: Handle an error returned.
	delete(cur.LogStream.UncommitReports, lsID)

	if pre != cur {
		deleted := &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID,
			Status:      varlogpb.LogStreamStatusDeleted,
		}

		cur.Metadata.InsertLogStream(deleted) //nolint:errcheck,revive // TODO:: Handle an error returned.

		lm := &mrpb.LogStreamUncommitReports{
			Status: varlogpb.LogStreamStatusDeleted,
		}

		cur.LogStream.UncommitReports[lsID] = lm
	}

	ms.deleteSortedLSIDs(ls.TopicID, lsID)
	ms.metaAppliedIndex++

	return nil
}

func (ms *MetadataStorage) UnregisterLogStream(lsID types.LogStreamID, nodeIndex, requestIndex uint64) error {
	err := ms.unregisterLogStream(lsID)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) updateLogStream(ls *varlogpb.LogStreamDescriptor) error {
	if len(ls.Replicas) == 0 {
		return verrors.ErrInvalidArgument
	}

	for _, r := range ls.Replicas {
		if ms.lookupStorageNode(r.StorageNodeID) == nil {
			return verrors.ErrInvalidArgument
		}
	}

	old := ms.lookupLogStream(ls.LogStreamID)
	if old == nil {
		return verrors.ErrNotExist
	}

	if equal := old.Equal(ls); equal {
		// To ensure that it is applied to the meta cache
		return nil
	}

	_, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	err := cur.Metadata.UpsertLogStream(ls)
	if err != nil {
		return err
	}

	ms.metaAppliedIndex++
	return err
}

func (ms *MetadataStorage) RegisterTopic(topic *varlogpb.TopicDescriptor, nodeIndex, requestIndex uint64) error {
	err := ms.registerTopic(topic)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) registerTopic(topic *varlogpb.TopicDescriptor) error {
	old := ms.lookupTopic(topic.TopicID)
	equal := old.Equal(topic)
	if old != nil && !equal {
		return verrors.ErrAlreadyExists
	}

	if equal {
		// To ensure that it is applied to the meta cache
		return nil
	}

	_, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	if err := cur.Metadata.UpsertTopic(topic); err != nil {
		return err
	}

	ms.metaAppliedIndex++
	return nil
}

func (ms *MetadataStorage) UnregisterTopic(topicID types.TopicID, nodeIndex, requestIndex uint64) error {
	err := ms.unregisterTopic(topicID)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) unregisterTopic(topicID types.TopicID) error {
	if ms.lookupTopic(topicID) == nil {
		return verrors.ErrNotExist
	}

	pre, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	cur.Metadata.DeleteTopic(topicID) //nolint:errcheck,revive // TODO:: Handle an error returned.

	if pre != cur {
		deleted := &varlogpb.TopicDescriptor{
			TopicID: topicID,
			Status:  varlogpb.TopicStatusDeleted,
		}

		cur.Metadata.InsertTopic(deleted) //nolint:errcheck,revive // TODO:: Handle an error returned.
	}

	ms.metaAppliedIndex++

	return nil
}

func (ms *MetadataStorage) updateUncommitReport(ls *varlogpb.LogStreamDescriptor) error {
	pre, cur := ms.getStateMachine()

	newReports := &mrpb.LogStreamUncommitReports{
		Replicas: make(map[types.StorageNodeID]snpb.LogStreamUncommitReport, len(ls.Replicas)),
	}

	oldReports, ok := cur.LogStream.UncommitReports[ls.LogStreamID]
	if !ok {
		tmp, ok := pre.LogStream.UncommitReports[ls.LogStreamID]
		if !ok {
			return verrors.ErrInternal
		}

		oldReports = proto.Clone(tmp).(*mrpb.LogStreamUncommitReports)
	}

	for _, r := range ls.Replicas {
		if o, ok := oldReports.Replicas[r.StorageNodeID]; ok {
			newReports.Replicas[r.StorageNodeID] = o
		} else {
			newReports.Replicas[r.StorageNodeID] = snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.MinLLSN,
				UncommittedLLSNLength: 0,
			}
		}
	}

	newReports.Status = ls.Status

	ms.logger.Info("reconfigure uncommit report",
		zap.Any("as-is", fmt.Sprintf("%+v", oldReports)),
		zap.Any("to-be", fmt.Sprintf("%+v", newReports)))

	cur.LogStream.UncommitReports[ls.LogStreamID] = newReports

	return nil
}

func (ms *MetadataStorage) UpdateLogStream(ls *varlogpb.LogStreamDescriptor, nodeIndex, requestIndex uint64) error {
	err := ms.updateLogStream(ls)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	err = ms.updateUncommitReport(ls)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func (ms *MetadataStorage) updateLogStreamDescStatus(lsID types.LogStreamID, status varlogpb.LogStreamStatus) error {
	ls := ms.lookupLogStream(lsID)
	if ls == nil {
		return verrors.ErrNotExist
	}

	ls = proto.Clone(ls).(*varlogpb.LogStreamDescriptor)

	if ls.Status == status ||
		(ls.Status.Sealed() && status == varlogpb.LogStreamStatusSealing) {
		return nil
	}

	ls.Status = status

	_, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	cur.Metadata.UpsertLogStream(ls) //nolint:errcheck,revive // TODO:: Handle an error returned.

	ms.metaAppliedIndex++

	ms.logger.Info("update log stream status", zap.Any("lsid", lsID), zap.Any("status", status))

	return nil
}

func (ms *MetadataStorage) updateUncommitReportStatus(lsID types.LogStreamID, status varlogpb.LogStreamStatus) error {
	pre, cur := ms.getStateMachine()

	lls, ok := cur.LogStream.UncommitReports[lsID]
	if !ok {
		o, ok := pre.LogStream.UncommitReports[lsID]
		if !ok {
			return verrors.ErrInternal
		}

		lls = proto.Clone(o).(*mrpb.LogStreamUncommitReports)
		cur.LogStream.UncommitReports[lsID] = lls
	}

	if lls.Status == status ||
		(lls.Status.Sealed() && status == varlogpb.LogStreamStatusSealing) {
		return nil
	}

	if status.Sealed() {
		min := types.InvalidLLSN
		for _, r := range lls.Replicas {
			if min.Invalid() || min > r.UncommittedLLSNEnd() {
				min = r.UncommittedLLSNEnd()
			}
		}

		for storageNodeID, r := range lls.Replicas {
			if r.Seal(min) == types.InvalidLLSN {
				return verrors.ErrInternal
			}
			lls.Replicas[storageNodeID] = r
		}
	} else {
		version := ms.getLastCommitVersionNoLock()
		for storageNodeID, r := range lls.Replicas {
			r.Version = version
			lls.Replicas[storageNodeID] = r
		}
	}

	lls.Status = status

	return nil
}

func (ms *MetadataStorage) updateLogStreamStatus(lsID types.LogStreamID, status varlogpb.LogStreamStatus, nodeIndex, requestIndex uint64) error {
	err := ms.updateLogStreamDescStatus(lsID, status)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	err = ms.updateUncommitReportStatus(lsID, status)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	ms.nrUpdateSinceCommit++

	return nil
}

func (ms *MetadataStorage) SealingLogStream(lsID types.LogStreamID, nodeIndex, requestIndex uint64) error {
	return ms.updateLogStreamStatus(lsID, varlogpb.LogStreamStatusSealing, nodeIndex, requestIndex)
}

func (ms *MetadataStorage) SealLogStream(lsID types.LogStreamID, nodeIndex, requestIndex uint64) error {
	return ms.updateLogStreamStatus(lsID, varlogpb.LogStreamStatusSealed, nodeIndex, requestIndex)
}

func (ms *MetadataStorage) UnsealLogStream(lsID types.LogStreamID, nodeIndex, requestIndex uint64) error {
	return ms.updateLogStreamStatus(lsID, varlogpb.LogStreamStatusRunning, nodeIndex, requestIndex)
}

func (ms *MetadataStorage) SetLeader(nodeID types.NodeID) {
	ms.logger.Info("set leader", zap.Any("leader", nodeID))
	atomic.StoreUint64(&ms.leader, uint64(nodeID))
}

func (ms *MetadataStorage) Leader() types.NodeID {
	return types.NodeID(atomic.LoadUint64(&ms.leader))
}

func (ms *MetadataStorage) Clear() {
	atomic.StoreUint64(&ms.leader, raft.None)
}

func (ms *MetadataStorage) AddPeer(nodeID types.NodeID, url string, isLearner bool, cs *raftpb.ConfState, appliedIndex uint64) error {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	ms.setConfState(cs)
	_, cur := ms.getStateMachine()

	peer := &mrpb.MetadataRepositoryDescriptor_PeerDescriptor{
		URL:       url,
		IsLearner: isLearner,
	}

	if exist, ok := cur.PeersMap.Peers[nodeID]; ok {
		if exist != nil && !exist.IsLearner {
			return nil
		}
	}
	cur.PeersMap.Peers[nodeID] = peer
	cur.PeersMap.AppliedIndex = appliedIndex

	return nil
}

func (ms *MetadataStorage) RemovePeer(nodeID types.NodeID, cs *raftpb.ConfState, appliedIndex uint64) error {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	ms.setConfState(cs)
	_, cur := ms.getStateMachine()

	if cur == ms.origStateMachine {
		delete(cur.PeersMap.Peers, nodeID)
		delete(cur.Endpoints, nodeID)
	} else {
		cur.PeersMap.Peers[nodeID] = nil
		cur.Endpoints[nodeID] = ""
	}
	cur.PeersMap.AppliedIndex = appliedIndex

	return nil
}

func (ms *MetadataStorage) GetPeers() *mrpb.MetadataRepositoryDescriptor_PeerDescriptorMap {
	peerMap := &mrpb.MetadataRepositoryDescriptor_PeerDescriptorMap{
		Peers: make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor),
	}

	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	for nodeID, peer := range ms.origStateMachine.PeersMap.Peers {
		peerMap.Peers[nodeID] = peer
	}

	for nodeID, peer := range ms.diffStateMachine.PeersMap.Peers {
		if peer != nil {
			peerMap.Peers[nodeID] = peer
		} else {
			delete(peerMap.Peers, nodeID)
		}
	}
	peerMap.AppliedIndex = mathutil.MaxUint64(
		ms.origStateMachine.PeersMap.AppliedIndex,
		ms.diffStateMachine.PeersMap.AppliedIndex,
	)

	return peerMap
}

func (ms *MetadataStorage) IsMember(nodeID types.NodeID) bool {
	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	pre, cur := ms.getStateMachine()

	if peer, ok := cur.PeersMap.Peers[nodeID]; ok {
		if peer == nil {
			return false
		}
		return !peer.IsLearner
	}

	if pre != cur {
		if peer, ok := pre.PeersMap.Peers[nodeID]; ok {
			return !peer.IsLearner
		}
	}

	return false
}

func (ms *MetadataStorage) IsLearner(nodeID types.NodeID) bool {
	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	pre, cur := ms.getStateMachine()

	if peer, ok := cur.PeersMap.Peers[nodeID]; ok {
		if peer == nil {
			return false
		}
		return peer.IsLearner
	}

	if pre != cur {
		if peer, ok := pre.PeersMap.Peers[nodeID]; ok {
			return peer.IsLearner
		}
	}

	return false
}

func (ms *MetadataStorage) RegisterEndpoint(nodeID types.NodeID, url string, nodeIndex, requestIndex uint64) error {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	_, cur := ms.getStateMachine()
	cur.Endpoints[nodeID] = url

	if ms.cacheCompleteCB != nil {
		ms.cacheCompleteCB(nodeIndex, requestIndex, nil)
	}

	return nil
}

func (ms *MetadataStorage) LookupEndpoint(nodeID types.NodeID) string {
	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	pre, cur := ms.getStateMachine()

	if url, ok := cur.Endpoints[nodeID]; ok {
		return url
	}

	if url, ok := pre.Endpoints[nodeID]; ok {
		return url
	}

	return ""
}

func (ms *MetadataStorage) lookupNextCommitResultsNoLock(ver types.Version) *mrpb.LogStreamCommitResults {
	pre, cur := ms.getStateMachine()
	if pre != cur {
		r := cur.LookupCommitResults(ver + 1)
		if r != nil {
			return r
		}
	}

	return pre.LookupCommitResults(ver + 1)
}

func (ms *MetadataStorage) lookupCommitResultsNoLock(ver types.Version) *mrpb.LogStreamCommitResults {
	pre, cur := ms.getStateMachine()
	if pre != cur {
		r := cur.LookupCommitResults(ver)
		if r != nil {
			return r
		}
	}

	return pre.LookupCommitResults(ver)
}

func (ms *MetadataStorage) getLastCommitResultsNoLock() *mrpb.LogStreamCommitResults {
	gls := ms.diffStateMachine.GetLastCommitResults()
	if gls != nil {
		return gls
	}

	return ms.origStateMachine.GetLastCommitResults()
}

func (ms *MetadataStorage) getFirstCommitResultsNoLock() *mrpb.LogStreamCommitResults {
	gls := ms.origStateMachine.GetFirstCommitResults()
	if gls != nil {
		return gls
	}

	return ms.diffStateMachine.GetFirstCommitResults()
}

func (ms *MetadataStorage) LookupUncommitReports(lsID types.LogStreamID) *mrpb.LogStreamUncommitReports {
	pre, cur := ms.getStateMachine()

	c := cur.LogStream.UncommitReports[lsID]
	if c != nil {
		if c.Status.Deleted() {
			return nil
		}

		return c
	}

	p := pre.LogStream.UncommitReports[lsID]
	return p
}

func (ms *MetadataStorage) LookupUncommitReport(lsID types.LogStreamID, snID types.StorageNodeID) (snpb.LogStreamUncommitReport, bool) {
	pre, cur := ms.getStateMachine()

	if lm, ok := cur.LogStream.UncommitReports[lsID]; ok {
		if lm.Status.Deleted() {
			return snpb.InvalidLogStreamUncommitReport, false
		}

		if s, ok := lm.Replicas[snID]; ok {
			return s, true
		}
	}

	if pre == cur {
		return snpb.InvalidLogStreamUncommitReport, false
	}

	if lm, ok := pre.LogStream.UncommitReports[lsID]; ok {
		if s, ok := lm.Replicas[snID]; ok {
			return s, true
		}
	}

	return snpb.InvalidLogStreamUncommitReport, false
}

func (ms *MetadataStorage) verifyUncommitReport(s snpb.LogStreamUncommitReport) bool {
	fgls := ms.getFirstCommitResultsNoLock()
	lgls := ms.getLastCommitResultsNoLock()

	if fgls == nil {
		return true
	}

	if fgls.Version > s.Version+1 ||
		lgls.Version < s.Version {
		return false
	}

	return s.Version+1 == fgls.Version ||
		ms.lookupCommitResultsNoLock(s.Version) != nil
}

func (ms *MetadataStorage) UpdateUncommitReport(lsID types.LogStreamID, snID types.StorageNodeID, s snpb.LogStreamUncommitReport) {
	pre, cur := ms.getStateMachine()

	lm, ok := cur.LogStream.UncommitReports[lsID]
	if !ok {
		o, ok := pre.LogStream.UncommitReports[lsID]
		if !ok {
			return
		}

		lm = proto.Clone(o).(*mrpb.LogStreamUncommitReports)
		cur.LogStream.UncommitReports[lsID] = lm
	}

	if lm.Status.Deleted() {
		return
	}

	r, ok := lm.Replicas[snID]
	if !ok {
		return
	}

	if !ms.verifyUncommitReport(s) {
		if !lm.Status.Sealed() {
			ms.logger.Warn("could not apply report: invalid ver",
				zap.Int32("lsid", int32(lsID)),
				zap.Int32("snid", int32(snID)),
				zap.Uint64("ver", uint64(s.Version)),
				zap.Uint64("first", uint64(ms.getFirstCommitResultsNoLock().GetVersion())),
				zap.Uint64("last", uint64(ms.getLastCommitResultsNoLock().GetVersion())),
			)
		}
		return
	}

	if lm.Status.Sealed() {
		if r.Version >= s.Version ||
			s.UncommittedLLSNOffset > r.UncommittedLLSNEnd() {
			return
		}

		s.UncommittedLLSNLength = uint64(r.UncommittedLLSNEnd() - s.UncommittedLLSNOffset)
	}

	lm.Replicas[snID] = s
	ms.nrUpdateSinceCommit++
}

func (ms *MetadataStorage) GetSortedTopicLogStreamIDs() []TopicLSID {
	return ms.sortedTopicLSIDs
}

func (ms *MetadataStorage) AppendLogStreamCommitHistory(cr *mrpb.LogStreamCommitResults) {
	ms.nrUpdateSinceCommit = 0

	if len(cr.CommitResults) == 0 {
		return
	}

	//ms.logger.Info("append commit", zap.String("logstreams", cr.String()))

	_, cur := ms.getStateMachine()

	ms.lsMu.Lock()
	defer ms.lsMu.Unlock()

	cur.LogStream.CommitHistory = append(cur.LogStream.CommitHistory, cr)
}

func (ms *MetadataStorage) TrimLogStreamCommitHistory(ver types.Version) error {
	_, cur := ms.getStateMachine()
	if ver != math.MaxUint64 && cur.LogStream.TrimVersion < ver {
		cur.LogStream.TrimVersion = ver
	}
	return nil
}

func (ms *MetadataStorage) UpdateAppliedIndex(appliedIndex uint64) {
	ms.appliedIndex = appliedIndex

	// make sure to merge before trigger snapshop
	// it makes orig have entry with appliedIndex
	ms.mergeStateMachine()

	ms.triggerSnapshot(appliedIndex)
}

func (ms *MetadataStorage) getLastCommitVersionNoLock() types.Version {
	gls := ms.getLastCommitResultsNoLock()
	return gls.GetVersion()
}

func (ms *MetadataStorage) GetLastCommitVersion() types.Version {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.getLastCommitVersionNoLock()
}

func (ms *MetadataStorage) GetMinCommitVersion() types.Version {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	gls := ms.getFirstCommitResultsNoLock()
	return gls.GetVersion()
}

func (ms *MetadataStorage) NumUpdateSinceCommit() uint64 {
	return ms.nrUpdateSinceCommit
}

func (ms *MetadataStorage) ResetUpdateSinceCommit() {
	ms.nrUpdateSinceCommit = 0
}

func (ms *MetadataStorage) LookupNextCommitResults(ver types.Version) (*mrpb.LogStreamCommitResults, error) {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	if oldest := ms.getFirstCommitResultsNoLock(); oldest != nil && oldest.Version > ver+1 {
		return nil, fmt.Errorf("already trimmed ver:%v, oldest:%v", ver, oldest.Version)
	}

	return ms.lookupNextCommitResultsNoLock(ver), nil
}

func (ms *MetadataStorage) GetFirstCommitResults() *mrpb.LogStreamCommitResults {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.getFirstCommitResultsNoLock()
}

func (ms *MetadataStorage) GetLastCommitResults() *mrpb.LogStreamCommitResults {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.getLastCommitResultsNoLock()
}

func (ms *MetadataStorage) GetMetadata() *varlogpb.MetadataDescriptor {
	ms.mcMu.RLock()
	defer ms.mcMu.RUnlock()

	return ms.metaCache
}

func (ms *MetadataStorage) GetLogStreamCommitResults() []*mrpb.LogStreamCommitResults {
	ver := ms.origStateMachine.LogStream.TrimVersion
	if ms.origStateMachine.LogStream.TrimVersion < ms.diffStateMachine.LogStream.TrimVersion {
		ver = ms.diffStateMachine.LogStream.TrimVersion
	}

	crs := append(ms.origStateMachine.LogStream.CommitHistory, ms.diffStateMachine.LogStream.CommitHistory...)

	i := sort.Search(len(crs), func(i int) bool {
		return crs[i].Version >= ver
	})

	if 0 < i &&
		i < len(crs) &&
		crs[i].Version == ver {
		crs = crs[i-1:]
	}

	return crs
}

func (ms *MetadataStorage) getSnapshotConfState() *raftpb.ConfState {
	f := ms.snapConfState.Load()
	if f == nil {
		return nil
	}

	return f.(*raftpb.ConfState)
}

func (ms *MetadataStorage) getSnapshotIndex() uint64 {
	return atomic.LoadUint64(&ms.snapIndex)
}

func (ms *MetadataStorage) GetSnapshotIndex() uint64 {
	return ms.getSnapshotIndex()
}

func (ms *MetadataStorage) GetSnapshot() ([]byte, *raftpb.ConfState, uint64) {
	ms.ssMu.RLock()
	defer ms.ssMu.RUnlock()

	return ms.snap, ms.getSnapshotConfState(), ms.getSnapshotIndex()
}

func (ms *MetadataStorage) ApplySnapshot(snap []byte, snapConfState *raftpb.ConfState, snapIndex uint64) error {
	if snapIndex < ms.appliedIndex {
		return errors.New("outdated snapshot")
	}

	stateMachine := &mrpb.MetadataRepositoryDescriptor{}
	err := stateMachine.Unmarshal(snap)
	if err != nil {
		return err
	}

	if stateMachine.Metadata == nil {
		stateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	}

	if stateMachine.LogStream == nil {
		stateMachine.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	}

	if stateMachine.LogStream.UncommitReports == nil {
		stateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)
	}

	if stateMachine.PeersMap.Peers == nil {
		stateMachine.PeersMap.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	}

	if stateMachine.Endpoints == nil {
		stateMachine.Endpoints = make(map[types.NodeID]string)
	}

	running := ms.running.Load()

	ms.Close()
	ms.releaseCopyOnWrite()

	ms.ssMu.Lock()
	ms.snap = snap
	ms.snapConfState.Store(snapConfState)
	atomic.StoreUint64(&ms.snapIndex, snapIndex)
	ms.ssMu.Unlock()

	ms.recoverCache(stateMachine, snapIndex)
	ms.recoverStateMachine(stateMachine, snapIndex)

	// make nrUpdateSinceCommit > 0 to enable commit
	ms.nrUpdateSinceCommit = 1
	ms.appliedIndex = snapIndex
	ms.origConfState = snapConfState

	ms.jobC = make(chan *storageAsyncJob, 4096)

	if running {
		ms.Run()
	}

	return nil
}

func (ms *MetadataStorage) RecoverStateMachine(stateMachine *mrpb.MetadataRepositoryDescriptor, appliedIndex, nodeIndex, requestIndex uint64) error {
	defer func() {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, nil)
		}
	}()

	running := ms.running.Load()

	ms.Close()
	ms.releaseCopyOnWrite()

	ms.mergePeers()

	stateMachine.Endpoints = ms.origStateMachine.Endpoints
	stateMachine.PeersMap = ms.origStateMachine.PeersMap

	ms.recoverLogStreams(stateMachine)
	ms.recoverCache(stateMachine, appliedIndex)
	ms.recoverStateMachine(stateMachine, appliedIndex)

	ms.trimLogStreamCommitHistory()

	fmt.Printf("recover commit result from [ver:%v]\n",
		ms.GetFirstCommitResults().GetVersion(),
	)

	ms.jobC = make(chan *storageAsyncJob, 4096)

	if running {
		ms.Run()
	}

	return nil
}

func (ms *MetadataStorage) recoverLogStreams(stateMachine *mrpb.MetadataRepositoryDescriptor) {
	commitResults := stateMachine.GetLastCommitResults()

	for _, ls := range stateMachine.Metadata.LogStreams {
		ls.Status = varlogpb.LogStreamStatusSealing

		lm, ok := stateMachine.LogStream.UncommitReports[ls.LogStreamID]
		if !ok {
			ms.logger.Panic("it could not recover state machine. invalid image")
		}
		lm.Status = varlogpb.LogStreamStatusSealing

		uncommittedLLSNLength := uint64(0)
		cr, _, ok := commitResults.LookupCommitResult(ls.TopicID, ls.LogStreamID, -1)
		if ok {
			uncommittedLLSNLength = uint64(cr.CommittedLLSNOffset) + cr.CommittedGLSNLength - 1
		}

		for storageNodeID, r := range lm.Replicas {
			r.Version = types.InvalidVersion
			r.UncommittedLLSNOffset = types.MinLLSN
			r.UncommittedLLSNLength = uncommittedLLSNLength
			lm.Replicas[storageNodeID] = r
		}
	}
}

func (ms *MetadataStorage) recoverCache(stateMachine *mrpb.MetadataRepositoryDescriptor, appliedIndex uint64) {
	cache := proto.Clone(stateMachine.Metadata).(*varlogpb.MetadataDescriptor)
	cache.AppliedIndex = appliedIndex

	ms.mcMu.Lock()
	ms.metaCache = cache
	ms.mcMu.Unlock()
}

func (ms *MetadataStorage) recoverStateMachine(stateMachine *mrpb.MetadataRepositoryDescriptor, appliedIndex uint64) {
	ms.mtMu.Lock()
	ms.lsMu.Lock()
	ms.prMu.Lock()

	ms.origStateMachine = stateMachine

	ms.diffStateMachine = &mrpb.MetadataRepositoryDescriptor{}
	ms.diffStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	ms.diffStateMachine.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	ms.diffStateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)
	ms.diffStateMachine.PeersMap.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	ms.diffStateMachine.Endpoints = make(map[types.NodeID]string)

	ms.metaAppliedIndex = appliedIndex
	ms.appliedIndex = appliedIndex

	ms.sortedTopicLSIDs = nil

	for _, ls := range stateMachine.Metadata.LogStreams {
		if !ls.Status.Deleted() {
			ms.sortedTopicLSIDs = append(ms.sortedTopicLSIDs, TopicLSID{TopicID: ls.TopicID, LogStreamID: ls.LogStreamID})
		}
	}

	sort.Slice(ms.sortedTopicLSIDs, func(i, j int) bool {
		if ms.sortedTopicLSIDs[i].TopicID == ms.sortedTopicLSIDs[j].TopicID {
			return ms.sortedTopicLSIDs[i].LogStreamID < ms.sortedTopicLSIDs[j].LogStreamID
		}
		return ms.sortedTopicLSIDs[i].TopicID < ms.sortedTopicLSIDs[j].TopicID
	})

	ms.prMu.Unlock()
	ms.lsMu.Unlock()
	ms.mtMu.Unlock()
}

func (ms *MetadataStorage) GetStorageNodes() []*varlogpb.StorageNodeDescriptor {
	o := ms.origStateMachine.Metadata.GetStorageNodes()
	d := ms.diffStateMachine.Metadata.GetStorageNodes()

	m := make([]*varlogpb.StorageNodeDescriptor, 0, len(o)+len(d))

	i, j := 0, 0
	for i < len(o) && j < len(d) {
		orig := o[i]
		diff := d[j]

		if orig.StorageNodeID < diff.StorageNodeID {
			m = append(m, orig)
			i++
		} else if orig.StorageNodeID > diff.StorageNodeID {
			m = append(m, diff)
			j++
		} else {
			if !diff.Status.Deleted() {
				m = append(m, diff)
			}
			i++
			j++
		}
	}

	if i < len(o) {
		m = append(m, o[i:]...)
	}

	for j < len(d) {
		diff := d[j]
		if !diff.Status.Deleted() {
			m = append(m, diff)
		}
		j++
	}

	return m
}

func (ms *MetadataStorage) GetLogStreams() []*varlogpb.LogStreamDescriptor {
	o := ms.origStateMachine.Metadata.GetLogStreams()
	d := ms.diffStateMachine.Metadata.GetLogStreams()

	m := make([]*varlogpb.LogStreamDescriptor, 0, len(o)+len(d))

	i, j := 0, 0
	for i < len(o) && j < len(d) {
		orig := o[i]
		diff := d[j]

		if orig.LogStreamID < diff.LogStreamID {
			m = append(m, orig)
			i++
		} else if orig.LogStreamID > diff.LogStreamID {
			m = append(m, diff)
			j++
		} else {
			if !diff.Status.Deleted() {
				m = append(m, diff)
			}
			i++
			j++
		}
	}

	if i < len(o) {
		m = append(m, o[i:]...)
	}

	for j < len(d) {
		if !d[j].Status.Deleted() {
			m = append(m, d[j])
		}
		j++
	}

	return m
}

func (ms *MetadataStorage) isCopyOnWrite() bool {
	return ms.copyOnWrite.Load()
}

func (ms *MetadataStorage) setCopyOnWrite() {
	ms.copyOnWrite.Store(true)
}

func (ms *MetadataStorage) releaseCopyOnWrite() {
	ms.copyOnWrite.Store(false)
}

func (ms *MetadataStorage) getStateMachine() (*mrpb.MetadataRepositoryDescriptor, *mrpb.MetadataRepositoryDescriptor) {
	pre := ms.origStateMachine
	cur := ms.origStateMachine

	if ms.isCopyOnWrite() {
		cur = ms.diffStateMachine
	}

	return pre, cur
}

func (ms *MetadataStorage) createSnapshot(job *jobSnapshot) {
	ms.logger.Info("create snapshot", zap.Uint64("index", job.appliedIndex))

	b, _ := ms.origStateMachine.Marshal()

	ms.ssMu.Lock()
	defer ms.ssMu.Unlock()

	ms.snap = b
	ms.snapConfState.Store(ms.origConfState)
	atomic.StoreUint64(&ms.snapIndex, job.appliedIndex)
}

func (ms *MetadataStorage) createMetadataCache(job *jobMetadataCache) {
	defer func() {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(job.nodeIndex, job.requestIndex, nil)
		}
	}()

	ms.mtMu.RLock()
	metaAppliedIndex := ms.metaAppliedIndex
	ms.mtMu.RUnlock()

	if ms.metaCache != nil && ms.metaCache.AppliedIndex >= metaAppliedIndex {
		return
	}

	cache := proto.Clone(ms.origStateMachine.Metadata).(*varlogpb.MetadataDescriptor)

	ms.mtMu.RLock()
	defer ms.mtMu.RUnlock()

	for _, sn := range ms.diffStateMachine.Metadata.StorageNodes {
		//TODO:: UpdateStorageNode
		if sn.Status.Deleted() {
			cache.DeleteStorageNode(sn.StorageNodeID) //nolint:errcheck,revive // TODO:: Handle an error returned.
		} else {
			cache.InsertStorageNode(sn) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}
	}

	for _, topic := range ms.diffStateMachine.Metadata.Topics {
		if topic.Status.Deleted() {
			cache.DeleteTopic(topic.TopicID) //nolint:errcheck,revive // TODO:: Handle an error returned.
		} else if cache.InsertTopic(topic) != nil {
			cache.UpdateTopic(topic) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}
	}

	for _, ls := range ms.diffStateMachine.Metadata.LogStreams {
		if ls.Status.Deleted() {
			cache.DeleteLogStream(ls.LogStreamID) //nolint:errcheck,revive // TODO:: Handle an error returned.
		} else if cache.InsertLogStream(ls) != nil {
			cache.UpdateLogStream(ls) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}
	}

	ms.mcMu.Lock()
	defer ms.mcMu.Unlock()

	cache.AppliedIndex = ms.metaAppliedIndex
	ms.metaCache = cache
}

func (ms *MetadataStorage) mergeMetadata() {
	if len(ms.diffStateMachine.Metadata.StorageNodes) == 0 &&
		len(ms.diffStateMachine.Metadata.LogStreams) == 0 &&
		len(ms.diffStateMachine.Metadata.Topics) == 0 {
		return
	}

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	for _, sn := range ms.diffStateMachine.Metadata.StorageNodes {
		//TODO:: UpdateStorageNode
		if sn.Status.Deleted() {
			ms.origStateMachine.Metadata.DeleteStorageNode(sn.StorageNodeID) //nolint:errcheck,revive // TODO:: Handle an error returned.
		} else {
			ms.origStateMachine.Metadata.InsertStorageNode(sn) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}
	}

	for _, topic := range ms.diffStateMachine.Metadata.Topics {
		if topic.Status.Deleted() {
			ms.origStateMachine.Metadata.DeleteTopic(topic.TopicID) //nolint:errcheck,revive // TODO:: Handle an error returned.
		} else if ms.origStateMachine.Metadata.InsertTopic(topic) != nil {
			ms.origStateMachine.Metadata.UpdateTopic(topic) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}
	}

	for _, ls := range ms.diffStateMachine.Metadata.LogStreams {
		if ls.Status.Deleted() {
			ms.origStateMachine.Metadata.DeleteLogStream(ls.LogStreamID) //nolint:errcheck,revive // TODO:: Handle an error returned.
		} else if ms.origStateMachine.Metadata.InsertLogStream(ls) != nil {
			ms.origStateMachine.Metadata.UpdateLogStream(ls) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}
	}

	ms.diffStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
}

func (ms *MetadataStorage) mergeLogStream() {
	for lsID, lm := range ms.diffStateMachine.LogStream.UncommitReports {
		if lm.Status.Deleted() {
			delete(ms.origStateMachine.LogStream.UncommitReports, lsID)
		} else {
			ms.origStateMachine.LogStream.UncommitReports[lsID] = lm
		}
	}

	if len(ms.diffStateMachine.LogStream.UncommitReports) > 0 {
		ms.diffStateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)
	}

	ms.lsMu.Lock()
	defer ms.lsMu.Unlock()

	if ms.origStateMachine.LogStream.TrimVersion < ms.diffStateMachine.LogStream.TrimVersion {
		ms.origStateMachine.LogStream.TrimVersion = ms.diffStateMachine.LogStream.TrimVersion
	}

	ms.origStateMachine.LogStream.CommitHistory = append(ms.origStateMachine.LogStream.CommitHistory, ms.diffStateMachine.LogStream.CommitHistory...)
	ms.diffStateMachine.LogStream.CommitHistory = nil

	ms.trimLogStreamCommitHistory()
}

func (ms *MetadataStorage) mergePeers() {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	for nodeID, peer := range ms.diffStateMachine.PeersMap.Peers {
		if peer == nil {
			delete(ms.origStateMachine.PeersMap.Peers, nodeID)
		} else {
			ms.origStateMachine.PeersMap.Peers[nodeID] = peer
		}
	}

	for nodeID, url := range ms.diffStateMachine.Endpoints {
		if url == "" {
			delete(ms.origStateMachine.Endpoints, nodeID)
		} else {
			ms.origStateMachine.Endpoints[nodeID] = url
		}
	}

	ms.origStateMachine.PeersMap.AppliedIndex = mathutil.MaxUint64(
		ms.origStateMachine.PeersMap.AppliedIndex,
		ms.diffStateMachine.PeersMap.AppliedIndex,
	)

	ms.diffStateMachine.PeersMap.AppliedIndex = 0
	ms.diffStateMachine.PeersMap.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	ms.diffStateMachine.Endpoints = make(map[types.NodeID]string)
}

func (ms *MetadataStorage) mergeConfState() {
	if ms.diffConfState != nil {
		ms.origConfState = ms.diffConfState
	}
	ms.diffConfState = nil
}

func (ms *MetadataStorage) setConfState(cs *raftpb.ConfState) {
	if ms.isCopyOnWrite() {
		ms.diffConfState = cs
	} else {
		ms.origConfState = cs
	}
}

func (ms *MetadataStorage) trimLogStreamCommitHistory() {
	s := ms.origStateMachine
	i := sort.Search(len(s.LogStream.CommitHistory), func(i int) bool {
		return s.LogStream.CommitHistory[i].Version >= s.LogStream.TrimVersion
	})

	if 1 < i &&
		i < len(s.LogStream.CommitHistory) &&
		s.LogStream.CommitHistory[i].Version == s.LogStream.TrimVersion {
		ms.logger.Info("trim", zap.Any("ver", s.LogStream.TrimVersion))
		s.LogStream.CommitHistory = s.LogStream.CommitHistory[i-1:]
	}
}

func (ms *MetadataStorage) mergeStateMachine() {
	if atomic.LoadInt64(&ms.nrRunning) != 0 {
		return
	}

	if !ms.isCopyOnWrite() {
		return
	}

	ms.mergeMetadata()
	ms.mergeLogStream()
	ms.mergePeers()
	ms.mergeConfState()

	ms.releaseCopyOnWrite()
}

func (ms *MetadataStorage) needSnapshot() bool {
	if ms.snapCount == 0 {
		return false
	}

	if ms.appliedIndex-ms.getSnapshotIndex() >= ms.snapCount {
		return true
	}

	// check if there was a conf change
	return ms.getSnapshotConfState() != ms.origConfState
}

func (ms *MetadataStorage) triggerSnapshot(appliedIndex uint64) {
	if !ms.running.Load() {
		return
	}

	if ms.isCopyOnWrite() {
		return
	}

	if !ms.needSnapshot() {
		return
	}

	if !atomic.CompareAndSwapInt64(&ms.nrRunning, 0, 1) {
		// While other snapshots are running,
		// there is no quarantee that an entry
		// with appliedIndex has been applied to origStateMachine
		return
	}

	ms.setCopyOnWrite()

	job := &jobSnapshot{
		appliedIndex: appliedIndex,
	}
	ms.jobC <- &storageAsyncJob{job: job}
}

func (ms *MetadataStorage) triggerMetadataCache(nodeIndex, requestIndex uint64) {
	if !ms.running.Load() {
		return
	}

	atomic.AddInt64(&ms.nrRunning, 1)
	ms.setCopyOnWrite()

	job := &jobMetadataCache{
		nodeIndex:    nodeIndex,
		requestIndex: requestIndex,
	}
	ms.jobC <- &storageAsyncJob{job: job}
}
