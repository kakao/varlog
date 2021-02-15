package metadata_repository

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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

	AddPeer(types.NodeID, string, bool, *raftpb.ConfState) error

	RemovePeer(types.NodeID, *raftpb.ConfState) error

	GetPeers() map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor

	IsMember(types.NodeID) bool

	IsLearner(types.NodeID) bool

	Clear()
}

// TODO:: refactoring
type MetadataStorage struct {
	// make orig immutable
	// and entries are applied to diff
	// while copyOnWrite set true
	origStateMachine *mrpb.MetadataRepositoryDescriptor
	diffStateMachine *mrpb.MetadataRepositoryDescriptor
	copyOnWrite      atomicutil.AtomicBool

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
	cancel  context.CancelFunc

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

	ms.origStateMachine.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	ms.diffStateMachine.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)

	ms.origStateMachine.Endpoints = make(map[types.NodeID]string)
	ms.diffStateMachine.Endpoints = make(map[types.NodeID]string)

	ms.metaCache = &varlogpb.MetadataDescriptor{}

	ms.jobC = make(chan *storageAsyncJob, 4096)

	return ms
}

func (ms *MetadataStorage) Run() {
	if !ms.running.Load() {
		ms.runner = runner.New("mr-storage", zap.NewNop())
		ms.runner.Run(ms.processSnapshot)
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
		} else {
			return sn
		}
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
		} else {
			return ls
		}
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

	cur.Metadata.DeleteStorageNode(snID)
	if pre != cur {
		deleted := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
			Status:        varlogpb.StorageNodeStatusDeleted,
		}

		cur.Metadata.InsertStorageNode(deleted)
	}

	ms.metaAppliedIndex++
	return nil
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
		Replicas: make(map[types.StorageNodeID]*snpb.LogStreamUncommitReport),
		Status:   varlogpb.LogStreamStatusRunning,
	}

	for _, r := range ls.Replicas {
		lm.Replicas[r.StorageNodeID] = &snpb.LogStreamUncommitReport{
			UncommittedLLSNOffset: types.MinLLSN,
			UncommittedLLSNLength: 0,
		}
	}

	cur.LogStream.UncommitReports[ls.LogStreamID] = lm
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
	if ms.lookupLogStream(lsID) == nil {
		return verrors.ErrNotExist
	}

	pre, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	cur.Metadata.DeleteLogStream(lsID)
	delete(cur.LogStream.UncommitReports, lsID)

	if pre != cur {
		deleted := &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID,
			Status:      varlogpb.LogStreamStatusDeleted,
		}

		cur.Metadata.InsertLogStream(deleted)

		lm := &mrpb.LogStreamUncommitReports{
			Status: varlogpb.LogStreamStatusDeleted,
		}

		cur.LogStream.UncommitReports[lsID] = lm
	}

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

func (ms *MetadataStorage) updateUncommitReport(ls *varlogpb.LogStreamDescriptor) error {
	pre, cur := ms.getStateMachine()

	newReports := &mrpb.LogStreamUncommitReports{
		Replicas: make(map[types.StorageNodeID]*snpb.LogStreamUncommitReport),
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
			newReports.Replicas[r.StorageNodeID] = &snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.MinLLSN,
				UncommittedLLSNLength: 0,
			}
		}
	}

	newReports.Status = ls.Status

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

func (ms *MetadataStorage) updateLogStreamStatus(lsID types.LogStreamID, status varlogpb.LogStreamStatus) error {
	ls := ms.lookupLogStream(lsID)
	if ls == nil {
		return verrors.ErrNotExist
	}

	ls = proto.Clone(ls).(*varlogpb.LogStreamDescriptor)

	if ls.Status == status {
		// To ensure that it is applied to the meta cache
		return nil
	}
	ls.Status = status

	_, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	cur.Metadata.UpsertLogStream(ls)

	ms.metaAppliedIndex++

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

	if lls.Status == status {
		// To ensure that it is applied to the meta cache
		return nil
	}

	if status == varlogpb.LogStreamStatusSealed {
		min := types.InvalidLLSN
		for _, r := range lls.Replicas {
			if min.Invalid() || min > r.UncommittedLLSNEnd() {
				min = r.UncommittedLLSNEnd()
			}
		}

		for _, r := range lls.Replicas {
			if r.Seal(min) == types.InvalidLLSN {
				return verrors.ErrInternal
			}
		}
	}

	lls.Status = status

	return nil
}

func (ms *MetadataStorage) SealLogStream(lsID types.LogStreamID, nodeIndex, requestIndex uint64) error {
	err := ms.updateLogStreamStatus(lsID, varlogpb.LogStreamStatusSealed)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	err = ms.updateUncommitReportStatus(lsID, varlogpb.LogStreamStatusSealed)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)

	return nil
}

func (ms *MetadataStorage) UnsealLogStream(lsID types.LogStreamID, nodeIndex, requestIndex uint64) error {
	err := ms.updateLogStreamStatus(lsID, varlogpb.LogStreamStatusRunning)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	err = ms.updateUncommitReportStatus(lsID, varlogpb.LogStreamStatusRunning)
	if err != nil {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(nodeIndex, requestIndex, err)
		}
		return err
	}

	ms.triggerMetadataCache(nodeIndex, requestIndex)

	return nil
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

func (ms *MetadataStorage) AddPeer(nodeID types.NodeID, url string, isLearner bool, cs *raftpb.ConfState) error {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	ms.setConfState(cs)
	_, cur := ms.getStateMachine()

	peer := &mrpb.MetadataRepositoryDescriptor_PeerDescriptor{
		URL:       url,
		IsLearner: isLearner,
	}

	if exist, ok := cur.Peers[nodeID]; ok {
		if exist != nil && !exist.IsLearner {
			return nil
		}
	}
	cur.Peers[nodeID] = peer

	return nil
}

func (ms *MetadataStorage) RemovePeer(nodeID types.NodeID, cs *raftpb.ConfState) error {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	ms.setConfState(cs)
	_, cur := ms.getStateMachine()

	if cur == ms.origStateMachine {
		delete(cur.Peers, nodeID)
		delete(cur.Endpoints, nodeID)
	} else {
		cur.Peers[nodeID] = nil
		cur.Endpoints[nodeID] = ""
	}

	return nil
}

func (ms *MetadataStorage) GetPeers() map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor {
	peers := make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)

	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	for nodeID, peer := range ms.origStateMachine.Peers {
		peers[nodeID] = peer
	}

	for nodeID, peer := range ms.diffStateMachine.Peers {
		if peer != nil {
			peers[nodeID] = peer
		} else {
			delete(peers, nodeID)
		}
	}

	return peers
}

func (ms *MetadataStorage) IsMember(nodeID types.NodeID) bool {
	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	pre, cur := ms.getStateMachine()

	if peer, ok := cur.Peers[nodeID]; ok {
		if peer == nil {
			return false
		}
		return !peer.IsLearner
	}

	if pre != cur {
		if peer, ok := pre.Peers[nodeID]; ok {
			return !peer.IsLearner
		}
	}

	return false
}

func (ms *MetadataStorage) IsLearner(nodeID types.NodeID) bool {
	ms.prMu.RLock()
	defer ms.prMu.RUnlock()

	pre, cur := ms.getStateMachine()

	if peer, ok := cur.Peers[nodeID]; ok {
		if peer == nil {
			return false
		}
		return peer.IsLearner
	}

	if pre != cur {
		if peer, ok := pre.Peers[nodeID]; ok {
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

func (ms *MetadataStorage) lookupNextCommitResultsNoLock(glsn types.GLSN) *mrpb.LogStreamCommitResults {
	pre, cur := ms.getStateMachine()
	if pre != cur {
		r := cur.LookupCommitResultsByPrev(glsn)
		if r != nil {
			return r
		}
	}

	return pre.LookupCommitResultsByPrev(glsn)
}

func (ms *MetadataStorage) lookupCommitResultsNoLock(glsn types.GLSN) *mrpb.LogStreamCommitResults {
	pre, cur := ms.getStateMachine()
	if pre != cur {
		r := cur.LookupCommitResults(glsn)
		if r != nil {
			return r
		}
	}

	return pre.LookupCommitResults(glsn)
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

func (ms *MetadataStorage) LookupUncommitReport(lsID types.LogStreamID, snID types.StorageNodeID) *snpb.LogStreamUncommitReport {
	pre, cur := ms.getStateMachine()

	if lm, ok := cur.LogStream.UncommitReports[lsID]; ok {
		if lm.Status.Deleted() {
			return nil
		}

		if s, ok := lm.Replicas[snID]; ok {
			return s
		}
	}

	if pre == cur {
		return nil
	}

	if lm, ok := pre.LogStream.UncommitReports[lsID]; ok {
		if s, ok := lm.Replicas[snID]; ok {
			return s
		}
	}

	return nil
}

func (ms *MetadataStorage) verifyUncommitReport(s *snpb.LogStreamUncommitReport) bool {
	fgls := ms.getFirstCommitResultsNoLock()
	lgls := ms.getLastCommitResultsNoLock()

	if fgls == nil {
		return true
	}

	if fgls.PrevHighWatermark > s.HighWatermark ||
		lgls.HighWatermark < s.HighWatermark {
		return false
	}

	return s.HighWatermark == fgls.PrevHighWatermark ||
		ms.lookupCommitResultsNoLock(s.HighWatermark) != nil
}

func (ms *MetadataStorage) UpdateUncommitReport(lsID types.LogStreamID, snID types.StorageNodeID, s *snpb.LogStreamUncommitReport) {
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
		ms.logger.Warn("could not apply report: invalid hwm",
			zap.Uint32("lsid", uint32(lsID)),
			zap.Uint32("snid", uint32(snID)),
			zap.Uint64("knownHWM", uint64(s.HighWatermark)),
			zap.Uint64("first", uint64(ms.getFirstCommitResultsNoLock().GetHighWatermark())),
			zap.Uint64("last", uint64(ms.getLastCommitResultsNoLock().GetHighWatermark())),
		)
		return
	}

	if lm.Status.Sealed() {
		if r.HighWatermark >= s.HighWatermark ||
			s.UncommittedLLSNOffset > r.UncommittedLLSNEnd() {
			return
		}

		s.UncommittedLLSNLength = uint64(r.UncommittedLLSNEnd() - s.UncommittedLLSNOffset)
	}

	lm.Replicas[snID] = s
	ms.nrUpdateSinceCommit++
}

func (ms *MetadataStorage) GetUncommitReportIDs() []types.LogStreamID {
	pre, cur := ms.getStateMachine()

	idset := set.New(len(pre.LogStream.UncommitReports))
	deleted := set.New(4)
	for lsID, lls := range pre.LogStream.UncommitReports {
		if lls.Status.Deleted() {
			deleted.Add(lsID)
		} else {
			idset.Add(lsID)
		}
	}

	if pre != cur {
		for lsID, lls := range cur.LogStream.UncommitReports {
			if lls.Status.Deleted() {
				deleted.Add(lsID)
			} else {
				idset.Add(lsID)
			}
		}
	}

	idset = idset.Diff(deleted)
	ids := make([]types.LogStreamID, 0, idset.Size())

	idset.Foreach(func(k interface{}) bool {
		ids = append(ids, k.(types.LogStreamID))
		return true
	})

	return ids
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

func (ms *MetadataStorage) TrimLogStreamCommitHistory(trimGLSN types.GLSN) error {
	_, cur := ms.getStateMachine()
	if trimGLSN != types.MaxGLSN && cur.LogStream.TrimGLSN < trimGLSN {
		cur.LogStream.TrimGLSN = trimGLSN
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

func (ms *MetadataStorage) getHighWatermarkNoLock() types.GLSN {
	gls := ms.getLastCommitResultsNoLock()
	if gls == nil {
		return types.InvalidGLSN
	}

	return gls.HighWatermark
}

func (ms *MetadataStorage) GetHighWatermark() types.GLSN {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.getHighWatermarkNoLock()
}

func (ms *MetadataStorage) GetMinHighWatermark() types.GLSN {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	gls := ms.getFirstCommitResultsNoLock()
	if gls == nil {
		return types.InvalidGLSN
	}

	return gls.HighWatermark
}

func (ms *MetadataStorage) NumUpdateSinceCommit() uint64 {
	return ms.nrUpdateSinceCommit
}

func (ms *MetadataStorage) LookupCommitResults(glsn types.GLSN) *mrpb.LogStreamCommitResults {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.lookupCommitResultsNoLock(glsn)
}

func (ms *MetadataStorage) LookupNextCommitResults(glsn types.GLSN) *mrpb.LogStreamCommitResults {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.lookupNextCommitResultsNoLock(glsn)
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

	if stateMachine.Peers == nil {
		stateMachine.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	}

	if stateMachine.Endpoints == nil {
		stateMachine.Endpoints = make(map[types.NodeID]string)
	}

	ms.Close()
	ms.releaseCopyOnWrite()

	ms.ssMu.Lock()
	ms.snap = snap
	ms.snapConfState.Store(snapConfState)
	atomic.StoreUint64(&ms.snapIndex, snapIndex)
	ms.ssMu.Unlock()

	cache := proto.Clone(stateMachine.Metadata).(*varlogpb.MetadataDescriptor)
	cache.AppliedIndex = snapIndex

	ms.mcMu.Lock()
	ms.metaCache = cache
	ms.mcMu.Unlock()

	ms.mtMu.Lock()
	ms.lsMu.Lock()
	ms.prMu.Lock()

	ms.origStateMachine = stateMachine

	ms.diffStateMachine = &mrpb.MetadataRepositoryDescriptor{}
	ms.diffStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	ms.diffStateMachine.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	ms.diffStateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)
	ms.diffStateMachine.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	ms.diffStateMachine.Endpoints = make(map[types.NodeID]string)

	ms.metaAppliedIndex = snapIndex

	ms.prMu.Unlock()
	ms.lsMu.Unlock()
	ms.mtMu.Unlock()

	// make nrUpdateSinceCommit > 0 to enable commit
	ms.nrUpdateSinceCommit = 1
	ms.appliedIndex = snapIndex
	ms.origConfState = snapConfState

	ms.jobC = make(chan *storageAsyncJob, 4096)
	ms.Run()

	return nil
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
			cache.DeleteStorageNode(sn.StorageNodeID)
		} else {
			cache.InsertStorageNode(sn)
		}
	}

	for _, ls := range ms.diffStateMachine.Metadata.LogStreams {
		if ls.Status.Deleted() {
			cache.DeleteLogStream(ls.LogStreamID)
		} else if cache.InsertLogStream(ls) != nil {
			cache.UpdateLogStream(ls)
		}
	}

	ms.mcMu.Lock()
	defer ms.mcMu.Unlock()

	cache.AppliedIndex = ms.metaAppliedIndex
	ms.metaCache = cache
}

func (ms *MetadataStorage) mergeMetadata() {
	if len(ms.diffStateMachine.Metadata.StorageNodes) == 0 &&
		len(ms.diffStateMachine.Metadata.LogStreams) == 0 {
		return
	}

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	for _, sn := range ms.diffStateMachine.Metadata.StorageNodes {
		//TODO:: UpdateStorageNode
		if sn.Status.Deleted() {
			ms.origStateMachine.Metadata.DeleteStorageNode(sn.StorageNodeID)
		} else {
			ms.origStateMachine.Metadata.InsertStorageNode(sn)
		}
	}

	for _, ls := range ms.diffStateMachine.Metadata.LogStreams {
		if ls.Status.Deleted() {
			ms.origStateMachine.Metadata.DeleteLogStream(ls.LogStreamID)
		} else if ms.origStateMachine.Metadata.InsertLogStream(ls) != nil {
			ms.origStateMachine.Metadata.UpdateLogStream(ls)
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

	if ms.origStateMachine.LogStream.TrimGLSN < ms.diffStateMachine.LogStream.TrimGLSN {
		ms.origStateMachine.LogStream.TrimGLSN = ms.diffStateMachine.LogStream.TrimGLSN
	}

	ms.origStateMachine.LogStream.CommitHistory = append(ms.origStateMachine.LogStream.CommitHistory, ms.diffStateMachine.LogStream.CommitHistory...)
	ms.diffStateMachine.LogStream.CommitHistory = nil

	ms.trimLogStreamCommitHistory()
}

func (ms *MetadataStorage) mergePeers() {
	ms.prMu.Lock()
	defer ms.prMu.Unlock()

	for nodeID, peer := range ms.diffStateMachine.Peers {
		if peer == nil {
			delete(ms.origStateMachine.Peers, nodeID)
		} else {
			ms.origStateMachine.Peers[nodeID] = peer
		}
	}

	for nodeID, url := range ms.diffStateMachine.Endpoints {
		if url == "" {
			delete(ms.origStateMachine.Endpoints, nodeID)
		} else {
			ms.origStateMachine.Endpoints[nodeID] = url
		}
	}

	ms.diffStateMachine.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
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
		return s.LogStream.CommitHistory[i].HighWatermark >= s.LogStream.TrimGLSN
	})

	if 0 < i &&
		i < len(s.LogStream.CommitHistory) &&
		s.LogStream.CommitHistory[i].HighWatermark == s.LogStream.TrimGLSN {
		ms.logger.Info("trim", zap.Any("glsn", s.LogStream.TrimGLSN))
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
	return
}

func (ms *MetadataStorage) needSnapshot() bool {
	if ms.appliedIndex-ms.getSnapshotIndex() > ms.snapCount {
		return true
	}

	// check if there was a conf change
	return ms.getSnapshotConfState() != ms.origConfState
}

func (ms *MetadataStorage) triggerSnapshot(appliedIndex uint64) {
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
	atomic.AddInt64(&ms.nrRunning, 1)
	ms.setCopyOnWrite()

	job := &jobMetadataCache{
		nodeIndex:    nodeIndex,
		requestIndex: requestIndex,
	}
	ms.jobC <- &storageAsyncJob{job: job}
}
