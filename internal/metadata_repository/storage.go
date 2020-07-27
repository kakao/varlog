package metadata_repository

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	varlog "github.com/kakao/varlog/pkg/varlog"
	types "github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	"github.com/kakao/varlog/pkg/varlog/util/syncutil/atomicutil"
	pb "github.com/kakao/varlog/proto/metadata_repository"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	"github.com/gogo/protobuf/proto"
)

type JobSnapshot struct {
	appliedIndex uint64
}

type JobMetadataCache struct {
	appliedIndex uint64
	nodeIndex    uint64
	requestIndex uint64
}

type StorageAsyncJob struct {
	job interface{}
}

type MetadataStorage struct {
	// make orig immutable
	// and entries are applied to diff
	// while copyOnWrite set true
	origStateMachine *pb.MetadataRepositoryDescriptor
	diffStateMachine *pb.MetadataRepositoryDescriptor
	copyOnWrite      atomicutil.AtomicBool

	// snapshot orig for raft
	snap []byte
	// the largest index of the entry already applied to the snapshot
	snapIndex uint64
	// the largest index of the entry already applied to the stateMachine
	appliedIndex uint64

	// immutable metadata cache for client request
	metaCache *varlogpb.MetadataDescriptor
	// the largest index of the entry already applied to the cache
	metaCacheIndex uint64
	// the largest index of the entry already applied to the stateMachine
	metaAppliedIndex uint64
	// callback after cache is completed
	cacheCompleteCB func(uint64, uint64, error)

	// number of running async job
	nrRunning           int64
	nrUpdateSinceCommit uint64

	lsMu sync.RWMutex // mutex for GlobalLogStream
	mtMu sync.RWMutex // mitex for Metadata
	ssMu sync.RWMutex // mutex for Snapshot
	mcMu sync.RWMutex // mutex for Metadata Cache

	// async job (snapshot, cache)
	jobC chan *StorageAsyncJob

	runner runner.Runner
	cancel context.CancelFunc
}

func NewMetadataStorage(cb func(uint64, uint64, error)) *MetadataStorage {
	ms := &MetadataStorage{cacheCompleteCB: cb}

	ms.origStateMachine = &pb.MetadataRepositoryDescriptor{}
	ms.origStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	ms.origStateMachine.LogStream = &pb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	ms.origStateMachine.LogStream.LocalLogStreams = make(map[types.LogStreamID]*pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas)

	ms.diffStateMachine = &pb.MetadataRepositoryDescriptor{}
	ms.diffStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
	ms.diffStateMachine.LogStream = &pb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
	ms.diffStateMachine.LogStream.LocalLogStreams = make(map[types.LogStreamID]*pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas)

	ms.jobC = make(chan *StorageAsyncJob, 4096)

	return ms
}

func (ms *MetadataStorage) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	ms.cancel = cancel

	ms.runner.Run(ctx, ms.processSnapshot)
}

func (ms *MetadataStorage) Close() {
	ms.cancel()
	ms.runner.CloseWait()
}

func (ms *MetadataStorage) processSnapshot(ctx context.Context) {
Loop:
	for {
		select {
		case f := <-ms.jobC:
			switch r := f.job.(type) {
			case *JobSnapshot:
				ms.createSnapshot(r)
			case *JobMetadataCache:
				ms.createMetadataCache(r)
			}

			atomic.AddInt64(&ms.nrRunning, -1)
		case <-ctx.Done():
			break Loop
		}
	}
}

func (ms *MetadataStorage) lookupStorageNode(snID types.StorageNodeID) *varlogpb.StorageNodeDescriptor {
	pre, cur := ms.getStateMachine()
	if pre != cur {
		sn := pre.Metadata.GetStorageNode(snID)
		if sn != nil {
			return sn
		}
	}

	return cur.Metadata.GetStorageNode(snID)
}

func (ms *MetadataStorage) registerStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
	defer func() { atomic.AddUint64(&ms.metaAppliedIndex, 1) }()

	pre, cur := ms.getStateMachine()
	if pre != cur {
		if pre.Metadata.GetStorageNode(sn.StorageNodeID) != nil {
			return varlog.ErrAlreadyExists
		}
	}

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	if err := cur.Metadata.InsertStorageNode(sn); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
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

func (ms *MetadataStorage) registerLogStream(ls *varlogpb.LogStreamDescriptor) error {
	defer func() { atomic.AddUint64(&ms.metaAppliedIndex, 1) }()

	if len(ls.Replicas) == 0 {
		return varlog.ErrInvalidArgument
	}

	for _, r := range ls.Replicas {
		if ms.lookupStorageNode(r.StorageNodeID) == nil {
			return varlog.ErrInvalidArgument
		}
	}

	pre, cur := ms.getStateMachine()
	if pre != cur {
		if pre.Metadata.GetLogStream(ls.LogStreamID) != nil {
			return varlog.ErrAlreadyExists
		}
	}

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	if err := cur.Metadata.InsertLogStream(ls); err != nil {
		return varlog.ErrAlreadyExists
	}

	lm := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas{
		Replicas: make(map[types.StorageNodeID]*pb.MetadataRepositoryDescriptor_LocalLogStreamReplica),
		Status:   varlogpb.LogStreamStatusNormal,
	}

	for _, r := range ls.Replicas {
		lm.Replicas[r.StorageNodeID] = &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{}
	}

	cur.LogStream.LocalLogStreams[ls.LogStreamID] = lm

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

func (ms *MetadataStorage) updateLogStream(ls *varlogpb.LogStreamDescriptor) error {
	defer func() { atomic.AddUint64(&ms.metaAppliedIndex, 1) }()

	if len(ls.Replicas) == 0 {
		return varlog.ErrInvalidArgument
	}

	for _, r := range ls.Replicas {
		if ms.lookupStorageNode(r.StorageNodeID) == nil {
			return varlog.ErrInvalidArgument
		}
	}

	pre, cur := ms.getStateMachine()

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	exist := cur.Metadata.GetLogStream(ls.LogStreamID) != nil
	if !exist && pre.Metadata.GetLogStream(ls.LogStreamID) == nil {
		return varlog.ErrNotExist
	}

	if exist {
		cur.Metadata.UpdateLogStream(ls)
	} else {
		cur.Metadata.InsertLogStream(ls)
	}

	new := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas{
		Replicas: make(map[types.StorageNodeID]*pb.MetadataRepositoryDescriptor_LocalLogStreamReplica),
	}

	old, _ := cur.LogStream.LocalLogStreams[ls.LogStreamID]

	for _, r := range ls.Replicas {
		if o, ok := old.Replicas[r.StorageNodeID]; ok {
			new.Replicas[r.StorageNodeID] = o
		} else {
			new.Replicas[r.StorageNodeID] = &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{}
		}
	}

	new.Status = old.Status

	cur.LogStream.LocalLogStreams[ls.LogStreamID] = new

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

	ms.triggerMetadataCache(nodeIndex, requestIndex)
	return nil
}

func lookupGlobalLogStreamByPrev(s *pb.MetadataRepositoryDescriptor, glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	i := sort.Search(len(s.LogStream.GlobalLogStreams), func(i int) bool {
		return s.LogStream.GlobalLogStreams[i].PrevNextGLSN >= glsn
	})

	if i < len(s.LogStream.GlobalLogStreams) && s.LogStream.GlobalLogStreams[i].PrevNextGLSN == glsn {
		return s.LogStream.GlobalLogStreams[i]
	}

	return nil
}

func (ms *MetadataStorage) LookupGlobalLogStreamByPrev(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	pre, cur := ms.getStateMachine()
	if pre != cur {
		r := lookupGlobalLogStreamByPrev(cur, glsn)
		if r != nil {
			return r
		}
	}

	return lookupGlobalLogStreamByPrev(pre, glsn)
}

func (ms *MetadataStorage) LookupLocalLogStream(lsID types.LogStreamID) *pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas {
	pre, cur := ms.getStateMachine()

	c, _ := cur.LogStream.LocalLogStreams[lsID]

	if c != nil {
		return c
	}

	p, _ := pre.LogStream.LocalLogStreams[lsID]
	return p
}

func (ms *MetadataStorage) LookupLocalLogStreamReplica(lsID types.LogStreamID, snID types.StorageNodeID) *pb.MetadataRepositoryDescriptor_LocalLogStreamReplica {
	pre, cur := ms.getStateMachine()

	if lm, ok := cur.LogStream.LocalLogStreams[lsID]; ok {
		if s, ok := lm.Replicas[snID]; ok {
			return s
		}
	}

	if pre == cur {
		return nil
	}

	if lm, ok := pre.LogStream.LocalLogStreams[lsID]; ok {
		if s, ok := lm.Replicas[snID]; ok {
			return s
		}
	}

	return nil
}

func (ms *MetadataStorage) UpdateLocalLogStreamReplica(lsID types.LogStreamID, snID types.StorageNodeID, s *pb.MetadataRepositoryDescriptor_LocalLogStreamReplica) {
	pre, cur := ms.getStateMachine()

	lm, ok := cur.LogStream.LocalLogStreams[lsID]
	if !ok {
		o, ok := pre.LogStream.LocalLogStreams[lsID]
		if !ok {
			// ignore
			return
		}

		lm = proto.Clone(o).(*pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas)
		cur.LogStream.LocalLogStreams[lsID] = lm
	}

	if lm.Status != varlogpb.LogStreamStatusNormal {
		return
	}

	if _, ok := lm.Replicas[snID]; !ok {
		// ignore
		return
	}

	lm.Replicas[snID] = s

	ms.nrUpdateSinceCommit++
}

func (ms *MetadataStorage) GetLocalLogStreamIDs() []types.LogStreamID {
	pre, cur := ms.getStateMachine()

	uniq := make(map[types.LogStreamID]struct{})
	for lsID := range pre.LogStream.LocalLogStreams {
		uniq[lsID] = struct{}{}
	}

	if pre != cur {
		for lsID := range cur.LogStream.LocalLogStreams {
			uniq[lsID] = struct{}{}
		}
	}

	lsIDs := make([]types.LogStreamID, 0, len(uniq))
	for lsID := range uniq {
		lsIDs = append(lsIDs, lsID)
	}

	sort.Slice(lsIDs, func(i, j int) bool { return lsIDs[i] < lsIDs[j] })

	return lsIDs
}

func (ms *MetadataStorage) AppendGlobalLogStream(gls *snpb.GlobalLogStreamDescriptor) {
	ms.nrUpdateSinceCommit = 0

	if len(gls.CommitResult) == 0 {
		return
	}

	sort.Slice(gls.CommitResult, func(i, j int) bool { return gls.CommitResult[i].LogStreamID < gls.CommitResult[j].LogStreamID })

	_, cur := ms.getStateMachine()

	ms.lsMu.Lock()
	defer ms.lsMu.Unlock()

	cur.LogStream.GlobalLogStreams = append(cur.LogStream.GlobalLogStreams, gls)
}

func (ms *MetadataStorage) TrimGlobalLogStream(trimGLSN types.GLSN) error {
	_, cur := ms.getStateMachine()
	cur.LogStream.TrimGLSN = trimGLSN
	return nil
}

func (ms *MetadataStorage) UpdateAppliedIndex(appliedIndex uint64) {
	ms.appliedIndex = appliedIndex

	// make sure to merge before trigger snapshop
	// it makes orig have entry with appliedIndex
	ms.mergeStateMachine()
	if ms.appliedIndex-atomic.LoadUint64(&ms.snapIndex) > defaultSnapshotCount {
		ms.triggerSnapshot(appliedIndex)
	}
}

func (ms *MetadataStorage) GetNextGLSNNoLock() types.GLSN {
	n := len(ms.diffStateMachine.LogStream.GlobalLogStreams)
	if n > 0 {
		return ms.diffStateMachine.LogStream.GlobalLogStreams[n-1].NextGLSN
	}

	n = len(ms.origStateMachine.LogStream.GlobalLogStreams)
	if n == 0 {
		return 0
	}

	return ms.origStateMachine.LogStream.GlobalLogStreams[n-1].NextGLSN
}

func (ms *MetadataStorage) GetNextGLSN() types.GLSN {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.GetNextGLSNNoLock()
}

func (ms *MetadataStorage) NumUpdateSinceCommit() uint64 {
	return ms.nrUpdateSinceCommit
}

func (ms *MetadataStorage) GetNextGLSFrom(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	return ms.LookupGlobalLogStreamByPrev(glsn)
}

func (ms *MetadataStorage) GetGLS() *snpb.GlobalLogStreamDescriptor {
	ms.lsMu.RLock()
	defer ms.lsMu.RUnlock()

	n := len(ms.diffStateMachine.LogStream.GlobalLogStreams)
	if n > 0 {
		return ms.diffStateMachine.LogStream.GlobalLogStreams[n-1]
	}

	n = len(ms.origStateMachine.LogStream.GlobalLogStreams)
	if n == 0 {
		return nil
	}

	return ms.origStateMachine.LogStream.GlobalLogStreams[n-1]
}

func (ms *MetadataStorage) GetMetadata() *varlogpb.MetadataDescriptor {
	ms.mcMu.RLock()
	defer ms.mcMu.RUnlock()

	return ms.metaCache
}

func (ms *MetadataStorage) GetSnapshot() ([]byte, uint64) {
	ms.ssMu.RLock()
	defer ms.ssMu.RUnlock()

	return ms.snap, atomic.LoadUint64(&ms.snapIndex)
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

func (ms *MetadataStorage) getStateMachine() (*pb.MetadataRepositoryDescriptor, *pb.MetadataRepositoryDescriptor) {
	pre := ms.origStateMachine
	cur := ms.origStateMachine

	if ms.isCopyOnWrite() {
		cur = ms.diffStateMachine
	}

	return pre, cur
}

func (ms *MetadataStorage) createSnapshot(job *JobSnapshot) {
	b, _ := ms.origStateMachine.Marshal()

	ms.ssMu.Lock()
	defer ms.ssMu.Unlock()

	ms.snap = b
	atomic.StoreUint64(&ms.snapIndex, job.appliedIndex)
}

func (ms *MetadataStorage) createMetadataCache(job *JobMetadataCache) {
	defer func() {
		if ms.cacheCompleteCB != nil {
			ms.cacheCompleteCB(job.nodeIndex, job.requestIndex, nil)
		}
	}()

	if ms.metaCacheIndex >= job.appliedIndex {
		return
	}

	cache := proto.Clone(ms.origStateMachine.Metadata).(*varlogpb.MetadataDescriptor)

	ms.mtMu.RLock()
	defer ms.mtMu.RUnlock()

	for _, sn := range ms.diffStateMachine.Metadata.StorageNodes {
		cache.InsertStorageNode(sn)
	}

	for _, ls := range ms.diffStateMachine.Metadata.LogStreams {
		cache.InsertLogStream(ls)
	}

	ms.mcMu.Lock()
	defer ms.mcMu.Unlock()

	ms.metaCache = cache
	ms.metaCacheIndex = atomic.LoadUint64(&ms.metaAppliedIndex)
}

func (ms *MetadataStorage) mergeMetadata() {
	if len(ms.diffStateMachine.Metadata.StorageNodes) == 0 &&
		len(ms.diffStateMachine.Metadata.LogStreams) == 0 {
		return
	}

	ms.mtMu.Lock()
	defer ms.mtMu.Unlock()

	for _, sn := range ms.diffStateMachine.Metadata.StorageNodes {
		ms.origStateMachine.Metadata.InsertStorageNode(sn)
	}

	for _, ls := range ms.diffStateMachine.Metadata.LogStreams {
		ms.origStateMachine.Metadata.InsertLogStream(ls)
	}

	ms.diffStateMachine.Metadata = &varlogpb.MetadataDescriptor{}
}

func (ms *MetadataStorage) mergeLogStream() {
	for lsID, lm := range ms.diffStateMachine.LogStream.LocalLogStreams {
		plm, ok := ms.origStateMachine.LogStream.LocalLogStreams[lsID]
		if !ok {
			ms.origStateMachine.LogStream.LocalLogStreams[lsID] = lm
			continue
		}

		for snID, r := range lm.Replicas {
			plm.Replicas[snID] = r
		}
	}

	if len(ms.diffStateMachine.LogStream.LocalLogStreams) > 0 {
		ms.diffStateMachine.LogStream.LocalLogStreams = make(map[types.LogStreamID]*pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas)
	}

	ms.lsMu.Lock()
	defer ms.lsMu.Unlock()

	if ms.origStateMachine.LogStream.TrimGLSN < ms.diffStateMachine.LogStream.TrimGLSN {
		ms.origStateMachine.LogStream.TrimGLSN = ms.diffStateMachine.LogStream.TrimGLSN
	}

	ms.origStateMachine.LogStream.GlobalLogStreams = append(ms.origStateMachine.LogStream.GlobalLogStreams, ms.diffStateMachine.LogStream.GlobalLogStreams...)
	ms.diffStateMachine.LogStream.GlobalLogStreams = nil
	ms.trimGlobalLogStream()
}

func (ms *MetadataStorage) trimGlobalLogStream() {
	s := ms.origStateMachine
	i := sort.Search(len(s.LogStream.GlobalLogStreams), func(i int) bool {
		return s.LogStream.GlobalLogStreams[i].NextGLSN >= s.LogStream.TrimGLSN
	})

	if i > 0 && i < len(s.LogStream.GlobalLogStreams) && s.LogStream.GlobalLogStreams[i].NextGLSN == s.LogStream.TrimGLSN {
		s.LogStream.GlobalLogStreams = s.LogStream.GlobalLogStreams[i:]
	}
}

func (ms *MetadataStorage) mergeStateMachine() {
	if atomic.LoadInt64(&ms.nrRunning) != 0 ||
		!ms.isCopyOnWrite() {
		return
	}

	ms.mergeMetadata()
	ms.mergeLogStream()

	ms.releaseCopyOnWrite()
}

func (ms *MetadataStorage) triggerSnapshot(appliedIndex uint64) {
	if !atomic.CompareAndSwapInt64(&ms.nrRunning, 0, 1) {
		// While other snapshots are running,
		// there is no quarantee that an entry
		// with appliedIndex has been applied to origStateMachine
		return
	}

	ms.setCopyOnWrite()

	job := &JobSnapshot{appliedIndex: appliedIndex}
	ms.jobC <- &StorageAsyncJob{job: job}
}

func (ms *MetadataStorage) triggerMetadataCache(nodeIndex, requestIndex uint64) {
	atomic.AddInt64(&ms.nrRunning, 1)
	ms.setCopyOnWrite()

	job := &JobMetadataCache{
		appliedIndex: atomic.LoadUint64(&ms.metaAppliedIndex),
		nodeIndex:    nodeIndex,
		requestIndex: requestIndex,
	}
	ms.jobC <- &StorageAsyncJob{job: job}
}
