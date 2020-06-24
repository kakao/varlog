package metadata_repository

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/prometheus/common/log"
	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const DefaultNumGlobalLogStreams int = 128

type localCutInfo struct {
	beginLlsn     types.LLSN
	endLlsn       types.LLSN
	knownNextGLSN types.GLSN
}

type RaftMetadataRepository struct {
	index      int
	nrReplica  int
	raftState  raft.StateType
	storageMap map[types.StorageNodeID]varlog.StorageNodeClient
	localCuts  map[types.LogStreamID]map[types.StorageNodeID]localCutInfo

	// SMR
	smr pb.MetadataRepositoryDescriptor
	mu  sync.RWMutex

	// for ack to session
	sessionNum uint64
	sessionMap sync.Map

	// for raft
	proposeC           chan *pb.RaftEntry
	commitC            chan *pb.RaftEntry
	rnConfChangeC      chan raftpb.ConfChange
	rnProposeC         chan string
	rnCommitC          <-chan *string
	rnErrorC           <-chan error
	rnStateC           <-chan raft.StateType
	rnSnapshotterReady <-chan *snap.Snapshotter

	wg sync.WaitGroup
}

func NewRaftMetadataRepository(index, nrRep int, peerList []string) *RaftMetadataRepository {
	mr := &RaftMetadataRepository{
		index:     index,
		nrReplica: nrRep,
	}

	mr.smr.Metadata = &varlogpb.MetadataDescriptor{}
	mr.smr.GlobalLogStreams = append(mr.smr.GlobalLogStreams, &snpb.GlobalLogStreamDescriptor{})
	mr.localCuts = make(map[types.LogStreamID]map[types.StorageNodeID]localCutInfo)

	mr.proposeC = make(chan *pb.RaftEntry, 4096)
	mr.commitC = make(chan *pb.RaftEntry, 4096)

	mr.rnConfChangeC = make(chan raftpb.ConfChange)
	mr.rnProposeC = make(chan string)
	commitC, errorC, stateC, snapshotterReady := newRaftNode(
		int(index)+1, // raftNode is 1-indexed
		peerList,
		false, // not to join an existing cluster
		mr.getSnapshot,
		mr.rnProposeC,
		mr.rnConfChangeC,
	)
	mr.rnCommitC = commitC
	mr.rnErrorC = errorC
	mr.rnStateC = stateC
	mr.rnSnapshotterReady = snapshotterReady
	return mr
}

func (mr *RaftMetadataRepository) Start() {
	go mr.runReplication()
	go mr.processCommit()
	go mr.processRNCommit()
	go mr.processRNState()
}

func (mr *RaftMetadataRepository) Close() error {
	close(mr.proposeC)
	close(mr.rnProposeC)
	err := <-mr.rnErrorC

	mr.wg.Wait()
	return err
}

func (mr *RaftMetadataRepository) isLeader() bool {
	return raft.StateLeader == raft.StateType(atomic.LoadUint64((*uint64)(&mr.raftState)))
}

func (mr *RaftMetadataRepository) runReplication() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for e := range mr.proposeC {
		b, err := e.Marshal()
		if err != nil {
			log.Errorf("%v", err)
			continue
		}

		mr.rnProposeC <- string(b)
	}
}

func (mr *RaftMetadataRepository) processCommit() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for e := range mr.commitC {
		mr.apply(e)
	}
}

func (mr *RaftMetadataRepository) processRNCommit() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for d := range mr.rnCommitC {
		if d == nil {
			// TODO: handle snapshots
			continue
		}

		e := &pb.RaftEntry{}
		err := e.Unmarshal([]byte(*d))
		if err != nil {
			log.Errorf("%v", err)
			continue
		}

		mr.commitC <- e
	}

	close(mr.commitC)
}

func (mr *RaftMetadataRepository) processRNState() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for d := range mr.rnStateC {
		atomic.StoreUint64((*uint64)(&mr.raftState), uint64(d))
	}
}

func (mr *RaftMetadataRepository) getSnapshot() ([]byte, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	b, _ := mr.smr.Marshal()
	return b, nil
}

func (mr *RaftMetadataRepository) sendAck(sessionNum uint64, err error) {
	f, ok := mr.sessionMap.Load(sessionNum)
	if !ok {
		return
	}

	c := f.(chan error)
	c <- err
}

func (mr *RaftMetadataRepository) apply(e *pb.RaftEntry) {
	var err error
	f := e.Request.GetValue()
	switch r := f.(type) {
	case *pb.RegisterStorageNode:
		err = mr.applyRegisterStorageNode(r)
	case *pb.CreateLogStream:
		err = mr.applyCreateLogStream(r)
	case *pb.Report:
		err = mr.applyReport(r)
	case *pb.Cut:
		err = mr.cut()
	case *pb.TrimCut:
		err = mr.trimCut(r)
	}

	mr.sendAck(e.SessionNum, err)
}

func (mr *RaftMetadataRepository) applyRegisterStorageNode(r *pb.RegisterStorageNode) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if err := mr.smr.Metadata.InsertStorageNode(r.StorageNode); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (mr *RaftMetadataRepository) applyCreateLogStream(r *pb.CreateLogStream) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if err := mr.smr.Metadata.InsertLogStream(r.LogStream); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (mr *RaftMetadataRepository) applyReport(r *pb.Report) error {
	//TODO:: handle failover StorageNode

	snId := r.LogStream.StorageNodeID
	for _, l := range r.LogStream.Uncommit {
		lsId := l.LogStreamID
		lm, ok := mr.localCuts[lsId]
		if !ok {
			lm = make(map[types.StorageNodeID]localCutInfo)
			mr.localCuts[lsId] = lm
		}

		u := localCutInfo{
			beginLlsn:     l.UncommittedLLSNBegin,
			endLlsn:       l.UncommittedLLSNEnd,
			knownNextGLSN: r.LogStream.NextGLSN,
		}

		s, ok := lm[snId]
		if !ok || s.endLlsn < u.endLlsn {
			// 같은 SN 으로부터 받은 report 정보중에
			// local cut 의 endLlsn 이 더 크다면
			// knownNextGLSN 은 크거나 같다.
			// knownNextGLSN 이 더 크고
			// endLlsn 이 더 작은 local cut 은 있을 수 없다.
			lm[snId] = u
		}
	}

	return nil
}

func getCommitResultFromGls(gls *snpb.GlobalLogStreamDescriptor, lsId types.LogStreamID) *snpb.GlobalLogStreamDescriptor_LogStreamCommitResult {
	i := sort.Search(len(gls.CommitResult), func(i int) bool {
		return gls.CommitResult[i].LogStreamID >= lsId
	})

	if i < len(gls.CommitResult) && gls.CommitResult[i].LogStreamID == lsId {
		return gls.CommitResult[i]
	}

	return nil
}

func (mr *RaftMetadataRepository) numCommitSince(lsId types.LogStreamID, glsn types.GLSN) (uint64, bool) {
	var i int
	var num uint64

Search:
	for i = len(mr.smr.GlobalLogStreams) - 1; i >= 0; i-- {
		gls := mr.smr.GlobalLogStreams[i]

		if gls.NextGLSN == glsn {
			break Search
		} else if gls.NextGLSN < glsn {
			panic("broken global cut consistency")
		}

		r := getCommitResultFromGls(gls, lsId)
		if r != nil {
			num += uint64(r.CommittedGLSNEnd - r.CommittedGLSNBegin)
		}
	}

	return num, i >= 0
}

func (mr *RaftMetadataRepository) cut() error {
	prev := mr.getNextGLSN()
	glsn := prev

	gls := &snpb.GlobalLogStreamDescriptor{
		PrevNextGLSN: prev,
	}

Loop:
	for lsId, l := range mr.localCuts {
		knownGlsn, nrUncommit := mr.calculateCut(l)
		if nrUncommit == 0 {
			continue Loop
		}

		if knownGlsn != prev {
			nrCommitted, ok := mr.numCommitSince(lsId, knownGlsn)
			if !ok {
				panic("can not issue GLSN")
			}

			if nrCommitted < nrUncommit {
				nrUncommit -= nrCommitted
			} else {
				continue Loop
			}
		}

		commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
			LogStreamID:        lsId,
			CommittedGLSNBegin: glsn,
			CommittedGLSNEnd:   glsn + types.GLSN(nrUncommit),
		}

		gls.CommitResult = append(gls.CommitResult, commit)
		glsn = commit.CommittedGLSNEnd
	}

	gls.NextGLSN = glsn

	mr.appendGlobalLogStream(gls)
	mr.proposeTrimCut()

	//TODO:: enable propose cut and handle idle
	//mr.proposeCut()

	return nil
}

func (mr *RaftMetadataRepository) trimCut(r *pb.TrimCut) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	pos := 0
	for idx, gls := range mr.smr.GlobalLogStreams {
		if gls.NextGLSN <= r.Glsn {
			pos = idx + 1
		}
	}

	if pos > 0 {
		mr.smr.GlobalLogStreams = mr.smr.GlobalLogStreams[pos:]
	}

	return nil
}

func (mr *RaftMetadataRepository) calculateCut(m map[types.StorageNodeID]localCutInfo) (types.GLSN, uint64) {
	var knownGlsn types.GLSN
	var beginLlsn types.LLSN
	var endLlsn types.LLSN = types.LLSN(math.MaxUint64)

	if len(m) < mr.nrReplica {
		return types.GLSN(0), 0
	}

	for _, l := range m {
		if l.beginLlsn > beginLlsn {
			beginLlsn = l.beginLlsn
		}

		if l.endLlsn < endLlsn {
			endLlsn = l.endLlsn
		}

		if l.knownNextGLSN > knownGlsn {
			// knownNextGLSN 이 다르다면,
			// 일부 SN 이 commitResult 를 받지 못했을 뿐이다.
			knownGlsn = l.knownNextGLSN
		}
	}

	if beginLlsn > endLlsn {
		return knownGlsn, 0
	}

	return knownGlsn, uint64(endLlsn - beginLlsn)
}

func (mr *RaftMetadataRepository) getLogStreamIDs() []types.LogStreamID {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	if len(mr.smr.Metadata.LogStreams) == 0 {
		return nil
	}

	lsIds := make([]types.LogStreamID, len(mr.smr.Metadata.LogStreams))

	for i, l := range mr.smr.Metadata.LogStreams {
		lsIds[i] = l.LogStreamID
	}

	return lsIds
}

func (mr *RaftMetadataRepository) getNextGLSN() types.GLSN {
	len := len(mr.smr.GlobalLogStreams)
	if len == 0 {
		return 0
	}

	return mr.smr.GlobalLogStreams[len-1].NextGLSN
}

func (mr *RaftMetadataRepository) appendGlobalLogStream(gls *snpb.GlobalLogStreamDescriptor) {
	if len(gls.CommitResult) == 0 {
		return
	}

	sort.Slice(gls.CommitResult, func(i, j int) bool { return gls.CommitResult[i].LogStreamID < gls.CommitResult[j].LogStreamID })

	mr.mu.Lock()
	defer mr.mu.Unlock()

	mr.smr.GlobalLogStreams = append(mr.smr.GlobalLogStreams, gls)
}

func (mr *RaftMetadataRepository) proposeTrimCut() {
	if !mr.isLeader() {
		return
	}

	//TODO:: check all storage node got commit result
	if len(mr.smr.GlobalLogStreams) <= DefaultNumGlobalLogStreams {
		return
	}

	r := &pb.TrimCut{
		Glsn: mr.smr.GlobalLogStreams[0].NextGLSN,
	}
	mr.propose(r)

	return
}

func (mr *RaftMetadataRepository) proposeCut() {
	if !mr.isLeader() {
		return
	}

	r := &pb.Cut{}
	mr.propose(r)

	return
}

func (mr *RaftMetadataRepository) proposeReport(lls *snpb.LocalLogStreamDescriptor) {
	r := &pb.Report{
		LogStream: lls,
	}
	mr.propose(r)

	return
}

func (mr *RaftMetadataRepository) propose(r interface{}) error {
	e := &pb.RaftEntry{}
	e.Request.SetValue(r)
	e.SessionNum = atomic.AddUint64(&mr.sessionNum, 1)

	c := make(chan error, 1)

	mr.sessionMap.Store(e.SessionNum, c)
	mr.proposeC <- e

	return <-c
}

func (mr *RaftMetadataRepository) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
	r := &pb.RegisterStorageNode{
		StorageNode: sn,
	}

	return mr.propose(r)
}

func (mr *RaftMetadataRepository) CreateLogStream(ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.CreateLogStream{
		LogStream: ls,
	}

	return mr.propose(r)
}

func (mr *RaftMetadataRepository) GetMetadata() (*varlogpb.MetadataDescriptor, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	// TODO:: make it thread-safe
	return mr.smr.Metadata, nil
}
