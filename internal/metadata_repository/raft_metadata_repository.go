package metadata_repository

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

const DefaultNumGlobalLogStreams int = 128
const unusedRequestIndex uint64 = 0

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

type RaftMetadataRepository struct {
	index             types.NodeID
	nrReplica         int
	raftState         raft.StateType
	reportCollector   *ReportCollector
	logger            *zap.Logger
	raftNode          *raftNode
	reporterClientFac ReporterClientFactory

	storage *MetadataStorage

	// for ack
	requestNum uint64
	requestMap sync.Map

	// for raft
	proposeC      chan *pb.RaftEntry
	commitC       chan *pb.RaftEntry
	rnConfChangeC chan raftpb.ConfChange
	rnProposeC    chan string
	rnCommitC     chan *raftCommittedEntry
	rnErrorC      chan error
	rnStateC      chan raft.StateType

	runner runner.Runner
	cancel context.CancelFunc
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

	mr.storage = NewMetadataStorage(mr.sendAck)

	mr.proposeC = make(chan *pb.RaftEntry, 4096)
	mr.commitC = make(chan *pb.RaftEntry, 4096)

	mr.rnConfChangeC = make(chan raftpb.ConfChange)
	mr.rnProposeC = make(chan string)
	mr.raftNode = newRaftNode(
		config.Index,
		config.PeerList,
		false, // not to join an existing cluster
		mr.storage.GetSnapshot,
		mr.rnProposeC,
		mr.rnConfChangeC,
		mr.logger.Named("raftnode"),
	)
	mr.rnCommitC = mr.raftNode.commitC
	mr.rnErrorC = mr.raftNode.errorC
	mr.rnStateC = mr.raftNode.stateC

	cbs := ReportCollectorCallbacks{
		report:     mr.proposeReport,
		getClient:  mr.reporterClientFac.GetClient,
		getNextGLS: mr.storage.GetNextGLSFrom,
	}

	mr.reportCollector = NewReportCollector(cbs,
		mr.logger.Named("report"))

	return mr
}

func (mr *RaftMetadataRepository) Run() {
	mr.storage.Run()

	ctx, cancel := context.WithCancel(context.Background())
	mr.cancel = cancel
	mr.runner.Run(ctx, mr.runReplication)
	mr.runner.Run(ctx, mr.processCommit)
	mr.runner.Run(ctx, mr.processRNCommit)
	mr.runner.Run(ctx, mr.processRNState)
	mr.runner.Run(ctx, mr.runCommitTrigger)

	go mr.raftNode.startRaft()
}

//TODO:: fix it
func (mr *RaftMetadataRepository) Close() error {
	mr.reportCollector.Close()

	mr.cancel()
	err := <-mr.rnErrorC

	mr.runner.CloseWait()

	mr.storage.Close()

	//TODO:: handle pendding msg

	return err
}

func (mr *RaftMetadataRepository) isLeader() bool {
	return raft.StateLeader == raft.StateType(atomic.LoadUint64((*uint64)(&mr.raftState)))
}

func (mr *RaftMetadataRepository) runReplication(ctx context.Context) {
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
		case <-ctx.Done():
			break Loop
		}
	}

	close(mr.rnProposeC)
}

func (mr *RaftMetadataRepository) runCommitTrigger(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond)
Loop:
	for {
		select {
		case <-ticker.C:
			mr.proposeCommit()
		case <-ctx.Done():
			break Loop
		}
	}

	ticker.Stop()
}

func (mr *RaftMetadataRepository) processCommit(ctx context.Context) {
	for e := range mr.commitC {
		mr.apply(e)
	}
}

func (mr *RaftMetadataRepository) processRNCommit(ctx context.Context) {
	for d := range mr.rnCommitC {
		if d == nil {
			// TODO: handle snapshots
			continue
		}

		e := &pb.RaftEntry{}
		err := e.Unmarshal([]byte(d.data))
		if err != nil {
			mr.logger.Error(err.Error())
			continue
		}
		e.AppliedIndex = d.index

		mr.commitC <- e
	}

	close(mr.commitC)
}

func (mr *RaftMetadataRepository) processRNState(ctx context.Context) {
	for d := range mr.rnStateC {
		atomic.StoreUint64((*uint64)(&mr.raftState), uint64(d))
	}
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
	f := e.Request.GetValue()
	switch r := f.(type) {
	case *pb.RegisterStorageNode:
		mr.applyRegisterStorageNode(r, e.NodeIndex, e.RequestIndex)
	case *pb.UnregisterStorageNode:
		mr.applyUnregisterStorageNode(r, e.NodeIndex, e.RequestIndex)
	case *pb.RegisterLogStream:
		mr.applyRegisterLogStream(r, e.NodeIndex, e.RequestIndex)
	case *pb.UnregisterLogStream:
		mr.applyUnregisterLogStream(r, e.NodeIndex, e.RequestIndex)
	case *pb.UpdateLogStream:
		mr.applyUpdateLogStream(r, e.NodeIndex, e.RequestIndex)
	case *pb.Report:
		mr.applyReport(r)
	case *pb.TrimCommit:
		mr.applyTrimCommit(r)
	case *pb.Commit:
		mr.applyCommit()
	case *pb.Seal:
		mr.applySeal(r, e.NodeIndex, e.RequestIndex)
	case *pb.Unseal:
		mr.applyUnseal(r, e.NodeIndex, e.RequestIndex)
	}

	mr.storage.UpdateAppliedIndex(e.AppliedIndex)
}

func (mr *RaftMetadataRepository) applyRegisterStorageNode(r *pb.RegisterStorageNode, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterStorageNode(r.StorageNode, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.RegisterStorageNode(r.StorageNode, mr.storage.GetNextGLSN())

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterStorageNode(r *pb.UnregisterStorageNode, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UnregisterStorageNode(r.StorageNodeID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.UnregisterStorageNode(r.StorageNodeID)

	return nil
}

func (mr *RaftMetadataRepository) applyRegisterLogStream(r *pb.RegisterLogStream, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterLogStream(r.LogStream, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterLogStream(r *pb.UnregisterLogStream, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UnregisterLogStream(r.LogStreamID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUpdateLogStream(r *pb.UpdateLogStream, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UpdateLogStream(r.LogStream, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyReport(r *pb.Report) error {
	snID := r.LogStream.StorageNodeID
	for _, l := range r.LogStream.Uncommit {
		lsID := l.LogStreamID

		u := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
			BeginLLSN:     l.UncommittedLLSNBegin,
			EndLLSN:       l.UncommittedLLSNEnd,
			KnownNextGLSN: r.LogStream.NextGLSN,
		}

		s := mr.storage.LookupLocalLogStreamReplica(lsID, snID)
		if s == nil || s.EndLLSN < u.EndLLSN {
			mr.storage.UpdateLocalLogStreamReplica(lsID, snID, u)
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyTrimCommit(r *pb.TrimCommit) error {
	return mr.storage.TrimGlobalLogStream(r.GLSN)
}

func (mr *RaftMetadataRepository) applyCommit() error {
	curGLSN := mr.storage.GetNextGLSNNoLock()
	newGLSN := curGLSN

	gls := &snpb.GlobalLogStreamDescriptor{
		PrevNextGLSN: curGLSN,
	}

	if mr.storage.NumUpdateSinceCommit() > 0 {
		lsIDs := mr.storage.GetLocalLogStreamIDs()

		for _, lsID := range lsIDs {
			replicas := mr.storage.LookupLocalLogStream(lsID)
			knownGLSN, nrUncommit := mr.calculateCommit(replicas)

			if knownGLSN != curGLSN {
				nrCommitted, ok := mr.numCommitSince(lsID, knownGLSN)
				if !ok {
					mr.logger.Panic("can not issue GLSN",
						zap.Uint64("known", uint64(knownGLSN)),
						zap.Uint64("uncommit", uint64(nrUncommit)),
						zap.Uint64("cur", uint64(curGLSN)),
					)
				}

				if nrCommitted > nrUncommit {
					mr.logger.Panic("# of uncommit should be bigger than # of commit",
						zap.Uint64("lsID", uint64(lsID)),
						zap.Uint64("known", uint64(knownGLSN)),
						zap.Uint64("cur", uint64(curGLSN)),
						zap.Uint64("uncommit", uint64(nrUncommit)),
						zap.Uint64("commit", uint64(nrCommitted)),
					)
				}

				nrUncommit -= nrCommitted
			}

			commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				LogStreamID:        lsID,
				CommittedGLSNBegin: newGLSN,
				CommittedGLSNEnd:   newGLSN + types.GLSN(nrUncommit),
			}

			if nrUncommit > 0 {
				newGLSN = commit.CommittedGLSNEnd
			} else {
				commit.CommittedGLSNBegin = mr.getLastCommitted(lsID)
				commit.CommittedGLSNEnd = commit.CommittedGLSNBegin
			}

			gls.CommitResult = append(gls.CommitResult, commit)
		}
	}
	gls.NextGLSN = newGLSN

	if newGLSN > curGLSN {
		mr.storage.AppendGlobalLogStream(gls)
	}
	mr.reportCollector.Commit(gls)
	mr.proposeTrimCommit()

	//TODO:: trigger next commit

	return nil
}

func (mr *RaftMetadataRepository) applySeal(r *pb.Seal, nodeIndex, requestIndex uint64) error {
	mr.applyCommit()
	err := mr.storage.SealLogStream(r.LogStreamID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUnseal(r *pb.Unseal, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UnsealLogStream(r.LogStreamID, nodeIndex, requestIndex)
	if err != nil {
		return err
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

func (mr *RaftMetadataRepository) numCommitSince(lsId types.LogStreamID, glsn types.GLSN) (uint64, bool) {
	var num uint64

	highest := mr.storage.GetNextGLSNNoLock()

	for glsn < highest {
		gls := mr.storage.LookupGlobalLogStreamByPrev(glsn)

		r := getCommitResultFromGLS(gls, lsId)
		if r != nil {
			num += uint64(r.CommittedGLSNEnd - r.CommittedGLSNBegin)
		}

		glsn = gls.NextGLSN
	}

	return num, true
}

func (mr *RaftMetadataRepository) calculateCommit(replicas *pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas) (types.GLSN, uint64) {
	var knownGLSN types.GLSN
	var beginLLSN types.LLSN
	var endLLSN types.LLSN = types.LLSN(math.MaxUint64)

	if replicas == nil {
		return types.GLSN(0), 0
	}

	if len(replicas.Replicas) < mr.nrReplica {
		return types.GLSN(0), 0
	}

	for _, l := range replicas.Replicas {
		if l.BeginLLSN > beginLLSN {
			beginLLSN = l.BeginLLSN
		}

		if l.EndLLSN < endLLSN {
			endLLSN = l.EndLLSN
		}

		if l.KnownNextGLSN > knownGLSN {
			// knownNextGLSN 이 다르다면,
			// 일부 SN 이 commitResult 를 받지 못했을 뿐이다.
			knownGLSN = l.KnownNextGLSN
		}
	}

	if beginLLSN > endLLSN {
		return knownGLSN, 0
	}

	return knownGLSN, uint64(endLLSN - beginLLSN)
}

func (mr *RaftMetadataRepository) getLastCommitted(lsID types.LogStreamID) types.GLSN {
	gls := mr.storage.GetGLS()
	if gls == nil {
		return types.GLSN(0)
	}

	r := getCommitResultFromGLS(gls, lsID)
	if r == nil {
		mr.logger.Panic("get last committed")
	}

	return r.CommittedGLSNEnd
}

func (mr *RaftMetadataRepository) proposeCommit() {
	if !mr.isLeader() {
		return
	}

	r := &pb.Commit{}
	mr.propose(context.TODO(), r, false)
}

func (mr *RaftMetadataRepository) proposeTrimCommit() {
	if !mr.isLeader() {
		return
	}

	//TODO:: check all storage node got commit result
	/*
		if len(mr.smr.GlobalLogStreams) <= DefaultNumGlobalLogStreams {
			return
		}

		r := &pb.TrimCommit{
			Glsn: mr.smr.GlobalLogStreams[0].NextGLSN,
		}
		mr.propose(context.TODO(), r, false)
	*/
}

func (mr *RaftMetadataRepository) proposeReport(lls *snpb.LocalLogStreamDescriptor) error {
	r := &pb.Report{
		LogStream: lls,
	}

	return mr.propose(context.TODO(), r, false)
}

func (mr *RaftMetadataRepository) propose(ctx context.Context, r interface{}, guarantee bool) error {
	e := &pb.RaftEntry{}
	e.Request.SetValue(r)
	e.NodeIndex = uint64(mr.index)
	e.RequestIndex = unusedRequestIndex

	if guarantee {
		c := make(chan error, 1)
		e.RequestIndex = atomic.AddUint64(&mr.requestNum, 1)
		mr.requestMap.Store(e.RequestIndex, c)
		defer mr.requestMap.Delete(e.RequestIndex)

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
			return varlog.ErrIgnore
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

func (mr *RaftMetadataRepository) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	r := &pb.UnregisterStorageNode{
		StorageNodeID: snID,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.RegisterLogStream{
		LogStream: ls,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	r := &pb.UnregisterLogStream{
		LogStreamID: lsID,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.UpdateLogStream{
		LogStream: ls,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	m := mr.storage.GetMetadata()
	return m, nil
}

func (mr *RaftMetadataRepository) Seal(ctx context.Context, lsID types.LogStreamID) (types.GLSN, error) {
	r := &pb.Seal{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != nil && err != varlog.ErrIgnore {
		return types.GLSN(0), err
	}

	return mr.getLastCommitted(lsID), nil
}

func (mr *RaftMetadataRepository) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	r := &pb.Unseal{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != nil && err != varlog.ErrIgnore {
		return err
	}

	return nil
}
