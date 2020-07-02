package metadata_repository

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	varlog "github.com/kakao/varlog/pkg/varlog"
	types "github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/metadata_repository"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

const DefaultNumGlobalLogStreams int = 128
const unusedRequestNum uint64 = 0

type Config struct {
	Index             types.NodeID
	NumRep            int
	PeerList          []string
	ReporterClientFac ReporterClientFactory
	Logger            *zap.Logger
}

func (config *Config) validate() error {
	if config.Index == types.InvalidNodeID {
		return errors.New("invalid index")
	}

	if config.NumRep < 1 {
		return errors.New("NumRep should be bigger than 0")
	}

	if len(config.PeerList) < 1 {
		return errors.New("# of PeerList should be bigger than 0")
	}

	if config.ReporterClientFac == nil {
		return errors.New("reporterClientFac should not be nil")
	}

	if config.Logger == nil {
		return errors.New("logger should not be nil")
	}

	return nil
}

type localLogStreamInfo struct {
	beginLlsn     types.LLSN
	endLlsn       types.LLSN
	knownNextGLSN types.GLSN
}

type RaftMetadataRepository struct {
	index             types.NodeID
	nrReplica         int
	raftState         raft.StateType
	localLogStreams   map[types.LogStreamID]map[types.StorageNodeID]localLogStreamInfo
	reportCollector   *ReportCollector
	logger            *zap.Logger
	raftNode          *raftNode
	reporterClientFac ReporterClientFactory

	// SMR
	smr pb.MetadataRepositoryDescriptor
	mu  sync.RWMutex

	nrUpdateSinceCommit uint64

	// for ack
	requestNum uint64
	requestMap sync.Map

	// for raft
	proposeC           chan *pb.RaftEntry
	commitC            chan *pb.RaftEntry
	rnConfChangeC      chan raftpb.ConfChange
	rnProposeC         chan string
	rnCommitC          chan *string
	rnErrorC           chan error
	rnStateC           chan raft.StateType
	rnSnapshotterReady chan *snap.Snapshotter

	// for stop
	stopC chan struct{}

	wg sync.WaitGroup
}

func NewRaftMetadataRepository(config *Config) *RaftMetadataRepository {
	if err := config.validate(); err != nil {
		panic(err)
	}

	mr := &RaftMetadataRepository{
		index:             config.Index,
		nrReplica:         config.NumRep,
		logger:            config.Logger,
		reporterClientFac: config.ReporterClientFac,
	}

	mr.smr.Metadata = &varlogpb.MetadataDescriptor{}
	//mr.smr.GlobalLogStreams = append(mr.smr.GlobalLogStreams, &snpb.GlobalLogStreamDescriptor{})
	mr.localLogStreams = make(map[types.LogStreamID]map[types.StorageNodeID]localLogStreamInfo)

	mr.proposeC = make(chan *pb.RaftEntry, 4096)
	mr.commitC = make(chan *pb.RaftEntry, 4096)
	mr.stopC = make(chan struct{})

	mr.rnConfChangeC = make(chan raftpb.ConfChange)
	mr.rnProposeC = make(chan string)
	mr.raftNode = newRaftNode(
		config.Index,
		config.PeerList,
		false, // not to join an existing cluster
		mr.getSnapshot,
		mr.rnProposeC,
		mr.rnConfChangeC,
		mr.logger.Named("raftnode"),
	)
	mr.rnCommitC = mr.raftNode.commitC
	mr.rnErrorC = mr.raftNode.errorC
	mr.rnStateC = mr.raftNode.stateC
	mr.rnSnapshotterReady = mr.raftNode.snapshotterReady

	cbs := ReportCollectorCallbacks{
		report:     mr.proposeReport,
		getClient:  mr.reporterClientFac.GetClient,
		getNextGLS: mr.getNextGLSFrom,
	}

	mr.reportCollector = NewReportCollector(cbs,
		mr.logger.Named("report"))

	return mr
}

func (mr *RaftMetadataRepository) Start() {
	mr.wg.Add(5)
	go mr.runReplication()
	go mr.processCommit()
	go mr.processRNCommit()
	go mr.processRNState()
	go mr.runCommitTrigger()

	go mr.raftNode.startRaft()
}

//TODO:: fix it
func (mr *RaftMetadataRepository) Close() error {
	mr.reportCollector.Close()

	close(mr.stopC)
	err := <-mr.rnErrorC

	mr.wg.Wait()

	//TODO:: handle pendding msg

	return err
}

func (mr *RaftMetadataRepository) isLeader() bool {
	return raft.StateLeader == raft.StateType(atomic.LoadUint64((*uint64)(&mr.raftState)))
}

func (mr *RaftMetadataRepository) runReplication() {
	defer mr.wg.Done()

Loop:
	for {
		select {
		case e := <-mr.proposeC:
			b, err := e.Marshal()
			if err != nil {
				mr.logger.Error(err.Error())
				continue
			}

			mr.rnProposeC <- string(b)
		case <-mr.stopC:
			break Loop
		}
	}

	close(mr.rnProposeC)
}

func (mr *RaftMetadataRepository) runCommitTrigger() {
	defer mr.wg.Done()

	ticker := time.NewTicker(time.Millisecond)
Loop:
	for {
		select {
		case <-ticker.C:
			mr.proposeCommit()
		case <-mr.stopC:
			break Loop
		}
	}

	ticker.Stop()
}

func (mr *RaftMetadataRepository) processCommit() {
	defer mr.wg.Done()

	for e := range mr.commitC {
		mr.apply(e)
	}
}

func (mr *RaftMetadataRepository) processRNCommit() {
	defer mr.wg.Done()

	for d := range mr.rnCommitC {
		if d == nil {
			// TODO: handle snapshots
			continue
		}

		e := &pb.RaftEntry{}
		err := e.Unmarshal([]byte(*d))
		if err != nil {
			mr.logger.Error(err.Error())
			continue
		}

		mr.commitC <- e
	}

	close(mr.commitC)
}

func (mr *RaftMetadataRepository) processRNState() {
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

func (mr *RaftMetadataRepository) sendAck(nodeIndex uint64, requestNum uint64, err error) {
	if mr.index != types.NodeID(nodeIndex) {
		return
	}

	f, ok := mr.requestMap.Load(requestNum)
	if !ok {
		return
	}

	c := f.(chan error)
	select {
	case c <- err:
	default:
	}
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
	case *pb.Commit:
		err = mr.commit()
	case *pb.TrimCommit:
		err = mr.trimCommit(r)
	}

	mr.sendAck(e.NodeIndex, e.RequestNum, err)
}

func (mr *RaftMetadataRepository) applyRegisterStorageNode(r *pb.RegisterStorageNode) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if err := mr.smr.Metadata.InsertStorageNode(r.StorageNode); err != nil {
		return varlog.ErrAlreadyExists
	}

	mr.reportCollector.RegisterStorageNode(r.StorageNode)

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
		lm, ok := mr.localLogStreams[lsId]
		if !ok {
			lm = make(map[types.StorageNodeID]localLogStreamInfo)
			mr.localLogStreams[lsId] = lm
		}

		u := localLogStreamInfo{
			beginLlsn:     l.UncommittedLLSNBegin,
			endLlsn:       l.UncommittedLLSNEnd,
			knownNextGLSN: r.LogStream.NextGLSN,
		}

		s, ok := lm[snId]
		if !ok || s.endLlsn < u.endLlsn {
			// 같은 SN 으로부터 받은 report 정보중에
			// localLogStream 의 endLlsn 이 더 크다면
			// knownNextGLSN 은 크거나 같다.
			// knownNextGLSN 이 더 크고
			// endLlsn 이 더 작은 localLogStream 은 있을 수 없다.
			lm[snId] = u
			mr.nrUpdateSinceCommit++
		}
	}

	return nil
}

func getCommitResultFromGLS(gls *snpb.GlobalLogStreamDescriptor, lsId types.LogStreamID) *snpb.GlobalLogStreamDescriptor_LogStreamCommitResult {
	i := sort.Search(len(gls.CommitResult), func(i int) bool {
		return gls.CommitResult[i].LogStreamID >= lsId
	})

	if i < len(gls.CommitResult) && gls.CommitResult[i].LogStreamID == lsId {
		return gls.CommitResult[i]
	}

	return nil
}

func (mr *RaftMetadataRepository) lookupGlobalLogStreamIdxByPrev(glsn types.GLSN) int {
	i := sort.Search(len(mr.smr.GlobalLogStreams), func(i int) bool {
		return mr.smr.GlobalLogStreams[i].PrevNextGLSN >= glsn
	})

	if i < len(mr.smr.GlobalLogStreams) && mr.smr.GlobalLogStreams[i].PrevNextGLSN == glsn {
		return i
	}

	return -1
}

func (mr *RaftMetadataRepository) numCommitSince(lsId types.LogStreamID, glsn types.GLSN) (uint64, bool) {
	var num uint64

	i := mr.lookupGlobalLogStreamIdxByPrev(glsn)
	if i < 0 {
		return 0, false
	}

	for ; i < len(mr.smr.GlobalLogStreams); i++ {
		gls := mr.smr.GlobalLogStreams[i]

		r := getCommitResultFromGLS(gls, lsId)
		if r != nil {
			num += uint64(r.CommittedGLSNEnd - r.CommittedGLSNBegin)
		}
	}

	return num, true
}

func (mr *RaftMetadataRepository) commit() error {
	prev := mr.getNextGLSN()
	glsn := prev

	gls := &snpb.GlobalLogStreamDescriptor{
		PrevNextGLSN: prev,
	}

	if mr.nrUpdateSinceCommit > 0 {
	Loop:
		for lsId, l := range mr.localLogStreams {
			knownGlsn, nrUncommit := mr.calculateCommit(l)
			if nrUncommit == 0 {
				continue Loop
			}

			if knownGlsn != prev {
				nrCommitted, ok := mr.numCommitSince(lsId, knownGlsn)
				if !ok {
					mr.logger.Panic("can not issue GLSN",
						zap.Uint64("known", uint64(knownGlsn)),
						zap.Uint64("uncommit", uint64(nrUncommit)),
						zap.Uint64("prev", uint64(prev)),
					)
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
	}

	mr.nrUpdateSinceCommit = 0

	gls.NextGLSN = glsn
	mr.appendGlobalLogStream(gls)
	mr.reportCollector.Commit(gls)

	//TODO:: trigger next commit

	return nil
}

func (mr *RaftMetadataRepository) trimCommit(r *pb.TrimCommit) error {
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

func (mr *RaftMetadataRepository) calculateCommit(m map[types.StorageNodeID]localLogStreamInfo) (types.GLSN, uint64) {
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

func (mr *RaftMetadataRepository) getNextGLSN4Test() types.GLSN {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	return mr.getNextGLSN()
}

func (mr *RaftMetadataRepository) getNextGLSFrom(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	i := mr.lookupGlobalLogStreamIdxByPrev(glsn)
	if i < 0 {
		return nil
	}

	return mr.smr.GlobalLogStreams[i]
}

func (mr *RaftMetadataRepository) appendGlobalLogStream(gls *snpb.GlobalLogStreamDescriptor) {
	if len(gls.CommitResult) == 0 {
		return
	}

	sort.Slice(gls.CommitResult, func(i, j int) bool { return gls.CommitResult[i].LogStreamID < gls.CommitResult[j].LogStreamID })

	mr.mu.Lock()
	defer mr.mu.Unlock()

	mr.smr.GlobalLogStreams = append(mr.smr.GlobalLogStreams, gls)
	mr.proposeTrimCommit()
}

func (mr *RaftMetadataRepository) proposeTrimCommit() {
	if !mr.isLeader() {
		return
	}

	//TODO:: check all storage node got commit result
	if len(mr.smr.GlobalLogStreams) <= DefaultNumGlobalLogStreams {
		return
	}

	r := &pb.TrimCommit{
		Glsn: mr.smr.GlobalLogStreams[0].NextGLSN,
	}
	mr.propose(context.TODO(), r, false)

	return
}

func (mr *RaftMetadataRepository) proposeCommit() {
	if !mr.isLeader() {
		return
	}

	r := &pb.Commit{}
	mr.propose(context.TODO(), r, false)

	return
}

func (mr *RaftMetadataRepository) proposeReport(lls *snpb.LocalLogStreamDescriptor) {
	r := &pb.Report{
		LogStream: lls,
	}
	mr.propose(context.TODO(), r, false)

	return
}

func (mr *RaftMetadataRepository) propose(ctx context.Context, r interface{}, guarantee bool) error {
	e := &pb.RaftEntry{}
	e.Request.SetValue(r)
	e.NodeIndex = uint64(mr.index)
	e.RequestNum = unusedRequestNum

	if guarantee {
		c := make(chan error, 1)
		e.RequestNum = atomic.AddUint64(&mr.requestNum, 1)
		mr.requestMap.Store(e.RequestNum, c)
		defer mr.requestMap.Delete(e.RequestNum)

		mr.proposeC <- e

		select {
		case err := <-c:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		select {
		case mr.proposeC <- e:
		default:
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) RegisterStorageNode(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) error {
	r := &pb.RegisterStorageNode{
		StorageNode: sn,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) CreateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.CreateLogStream{
		LogStream: ls,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	// TODO:: make it thread-safe
	return mr.smr.Metadata, nil
}
