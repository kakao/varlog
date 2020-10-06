package metadata_repository

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner/stopwaiter"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"google.golang.org/grpc"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type RaftMetadataRepository struct {
	clusterID         types.ClusterID
	nodeID            types.NodeID
	nrReplica         int
	raftState         raft.StateType
	reportCollector   *ReportCollector
	raftNode          *raftNode
	reporterClientFac ReporterClientFactory

	storage *MetadataStorage

	// for ack
	requestNum uint64
	requestMap sync.Map

	// for raft
	proposeC      chan *pb.RaftEntry
	commitC       chan *committedEntry
	rnConfChangeC chan raftpb.ConfChange
	rnProposeC    chan string
	rnCommitC     chan *raftCommittedEntry

	options *MetadataRepositoryOptions
	logger  *zap.Logger

	sw     *stopwaiter.StopWaiter
	runner *runner.Runner
	cancel context.CancelFunc

	server     *grpc.Server
	serverAddr string

	nrReport uint64
}

func NewRaftMetadataRepository(options *MetadataRepositoryOptions) *RaftMetadataRepository {
	if err := options.validate(); err != nil {
		panic(err)
	}

	mr := &RaftMetadataRepository{
		clusterID:         options.ClusterID,
		nodeID:            options.NodeID,
		nrReplica:         options.NumRep,
		logger:            options.Logger,
		reporterClientFac: options.ReporterClientFac,
		options:           options,
		runner:            runner.New("mr", options.Logger),
		sw:                stopwaiter.New(),
	}

	mr.storage = NewMetadataStorage(mr.sendAck, options.SnapCount, mr.logger.Named("storage"))

	mr.proposeC = make(chan *pb.RaftEntry, 4096)
	mr.commitC = make(chan *committedEntry, 4096)

	mr.rnConfChangeC = make(chan raftpb.ConfChange)
	mr.rnProposeC = make(chan string)
	mr.raftNode = newRaftNode(
		options.NodeID,
		options.PeerList.Value(),
		options.Join, // if false, not to join an existing cluster
		options.SnapCount,
		options.RaftTick,
		mr.storage.GetSnapshot,
		mr.rnProposeC,
		mr.rnConfChangeC,
		//mr.logger.Named("raftnode"),
		mr.logger.Named(fmt.Sprintf("%v", options.NodeID)),
	)
	mr.rnCommitC = mr.raftNode.commitC

	cbs := ReportCollectorCallbacks{
		report:        mr.proposeReport,
		getClient:     mr.reporterClientFac.GetClient,
		lookupNextGLS: mr.storage.LookupNextGLS,
		getOldestGLS:  mr.storage.GetFirstGLS,
	}

	mr.reportCollector = NewReportCollector(cbs, mr.options.RPCTimeout,
		mr.logger.Named("report"))

	mr.server = grpc.NewServer()
	NewMetadataRepositoryService(mr).Register(mr.server)
	NewManagementService(mr).Register(mr.server)

	return mr
}

func (mr *RaftMetadataRepository) Run() {
	mr.storage.Run()
	mr.reportCollector.Run()

	mctx, cancel := mr.runner.WithManagedCancel(context.Background())

	mr.cancel = cancel
	if err := mr.runner.RunC(mctx, mr.runReplication); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.processCommit); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.processRNCommit); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.runCommitTrigger); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}

	mr.raftNode.start()

	mr.logger.Info("listening", zap.String("address", mr.options.RPCBindAddress))
	lis, err := netutil.NewStoppableListener(mctx, mr.options.RPCBindAddress)
	if err != nil {
		mr.logger.Panic("could not listen", zap.Error(err))
	}

	addrs, _ := netutil.GetListenerAddrs(lis.Addr())
	mr.serverAddr = addrs[0]

	if err := mr.runner.RunC(mctx, func(ctx context.Context) {
		//TODO:: graceful shutdown
		if err := mr.server.Serve(lis); err != nil && err != varlog.ErrStopped {
			mr.logger.Panic("could not serve", zap.Error(err))
			//r.Close()
		}
	}); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}

	mr.logger.Info("starting metadata repository")
}

//TODO:: handle pendding msg
func (mr *RaftMetadataRepository) Close() error {
	defer mr.sw.Stop()

	mr.reportCollector.Close()
	if mr.cancel != nil {
		mr.cancel()
		mr.raftNode.stop(true)
		mr.runner.Stop()
		mr.storage.Close()

		close(mr.proposeC)
		close(mr.rnProposeC)
		close(mr.rnConfChangeC)

		mr.cancel = nil
	}

	mr.clearMembership()

	return nil
}

func (mr *RaftMetadataRepository) Wait() {
	mr.sw.Wait()
}

func (mr *RaftMetadataRepository) isLeader() bool {
	return mr.raftNode.membership.getLeader() == uint64(mr.nodeID)
}

func (mr *RaftMetadataRepository) hasLeader() bool {
	return mr.raftNode.membership.hasLeader()
}

func (mr *RaftMetadataRepository) clearMembership() {
	mr.raftNode.membership.clearMembership()
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

			select {
			case mr.rnProposeC <- string(b):
			case <-ctx.Done():
				mr.sendAck(e.NodeIndex, e.RequestIndex, ctx.Err())
			}
		case <-ctx.Done():
			break Loop
		}
	}
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
	for c := range mr.commitC {
		if c == nil {
			snap := mr.raftNode.loadSnapshot()
			if snap != nil {
				mr.reportCollector.Close()

				err := mr.storage.ApplySnapshot(snap.Data, &snap.Metadata.ConfState, snap.Metadata.Index)
				if err != nil {
					mr.logger.Panic("load snapshot fail")
				}

				err = mr.reportCollector.Recover(
					mr.storage.GetAllStorageNodes(),
					mr.storage.GetHighWatermark())
				if err != nil {
					mr.logger.Panic("recover report collector fail")
				}
			}

			continue
		}

		mr.apply(c)
	}
}

func (mr *RaftMetadataRepository) processRNCommit(ctx context.Context) {
	for d := range mr.rnCommitC {
		var c *committedEntry
		var e *pb.RaftEntry

		if d != nil {
			e = &pb.RaftEntry{}
			switch d.entryType {
			case raftpb.EntryNormal:
				err := e.Unmarshal(d.data)
				if err != nil {
					mr.logger.Error(err.Error())
					continue
				}
				c = &committedEntry{
					entry: e,
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				err := cc.Unmarshal(d.data)
				if err != nil {
					mr.logger.Error(err.Error())
					continue
				}

				switch cc.Type {
				case raftpb.ConfChangeAddNode:
					p := &pb.AddPeer{
						NodeID: types.NodeID(cc.NodeID),
						Url:    string(cc.Context),
					}

					e.Request.SetValue(p)
				case raftpb.ConfChangeRemoveNode:
					p := &pb.RemovePeer{
						NodeID: types.NodeID(cc.NodeID),
					}

					e.Request.SetValue(p)
				default:
					mr.logger.Panic("unknown type")
				}

				c = &committedEntry{
					entry:     e,
					confState: d.confState,
				}
			default:
				mr.logger.Panic("unknown type")
			}
			e.AppliedIndex = d.index

		}

		mr.commitC <- c
	}

	close(mr.commitC)
}

func (mr *RaftMetadataRepository) sendAck(nodeIndex uint64, requestNum uint64, err error) {
	if mr.nodeID != types.NodeID(nodeIndex) {
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

func (mr *RaftMetadataRepository) apply(c *committedEntry) {
	e := c.entry
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
	case *pb.Commit:
		mr.applyCommit()
	case *pb.Seal:
		mr.applySeal(r, e.NodeIndex, e.RequestIndex)
	case *pb.Unseal:
		mr.applyUnseal(r, e.NodeIndex, e.RequestIndex)
	case *pb.AddPeer:
		mr.applyAddPeer(r, c.confState)
	case *pb.RemovePeer:
		mr.applyRemovePeer(r, c.confState)
	}

	mr.storage.UpdateAppliedIndex(e.AppliedIndex)
}

func (mr *RaftMetadataRepository) applyRegisterStorageNode(r *pb.RegisterStorageNode, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterStorageNode(r.StorageNode, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.RegisterStorageNode(r.StorageNode, mr.storage.GetHighWatermark())

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
	atomic.AddUint64(&mr.nrReport, 1)

	snID := r.LogStream.StorageNodeID
	for _, l := range r.LogStream.Uncommit {
		lsID := l.LogStreamID

		u := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
			UncommittedLLSNOffset: l.UncommittedLLSNOffset,
			UncommittedLLSNLength: l.UncommittedLLSNLength,
			KnownHighWatermark:    r.LogStream.HighWatermark,
		}

		s := mr.storage.LookupLocalLogStreamReplica(lsID, snID)
		if s == nil ||
			s.UncommittedLLSNEnd() < u.UncommittedLLSNEnd() ||
			s.KnownHighWatermark < u.KnownHighWatermark {
			mr.storage.UpdateLocalLogStreamReplica(lsID, snID, u)
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyCommit() error {
	curHWM := mr.storage.getHighWatermarkNoLock()
	trimHWM := types.MaxGLSN
	committedOffset := curHWM + types.GLSN(1)
	nrCommitted := uint64(0)

	gls := &snpb.GlobalLogStreamDescriptor{
		PrevHighWatermark: curHWM,
	}

	if mr.storage.NumUpdateSinceCommit() > 0 {
		lsIDs := mr.storage.GetLocalLogStreamIDs()

		for _, lsID := range lsIDs {
			replicas := mr.storage.LookupLocalLogStream(lsID)
			knownHWM, minHWM, nrUncommit := mr.calculateCommit(replicas)
			if minHWM < trimHWM {
				trimHWM = minHWM
			}

			if knownHWM != curHWM {
				nrCommitted := mr.numCommitSince(lsID, knownHWM)
				if nrCommitted > nrUncommit {
					mr.logger.Panic("# of uncommit should be bigger than # of commit",
						zap.Uint64("lsID", uint64(lsID)),
						zap.Uint64("known", uint64(knownHWM)),
						zap.Uint64("cur", uint64(curHWM)),
						zap.Uint64("uncommit", uint64(nrUncommit)),
						zap.Uint64("commit", uint64(nrCommitted)),
						zap.Uint64("first", uint64(mr.storage.getFirstGLSNoLock().GetHighWatermark())),
						zap.Uint64("last", uint64(mr.storage.getLastGLSNoLock().GetHighWatermark())),
					)
				}

				nrUncommit -= nrCommitted
			}

			commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				LogStreamID:         lsID,
				CommittedGLSNOffset: committedOffset,
				CommittedGLSNLength: nrUncommit,
			}

			if nrUncommit > 0 {
				committedOffset = commit.CommittedGLSNOffset + types.GLSN(commit.CommittedGLSNLength)
			} else {
				commit.CommittedGLSNOffset = mr.getLastCommitted(lsID) + types.GLSN(1)
				commit.CommittedGLSNLength = 0
			}

			gls.CommitResult = append(gls.CommitResult, commit)

			nrCommitted += nrUncommit
		}
	}
	gls.HighWatermark = curHWM + types.GLSN(nrCommitted)

	if nrCommitted > 0 {
		mr.storage.AppendGlobalLogStream(gls)
	}

	if !trimHWM.Invalid() && trimHWM != types.MaxGLSN {
		mr.storage.TrimGlobalLogStream(trimHWM)
	}

	mr.reportCollector.Commit(gls)

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

func (mr *RaftMetadataRepository) applyAddPeer(r *pb.AddPeer, cs *raftpb.ConfState) error {
	err := mr.storage.AddPeer(r.NodeID, r.Url, cs)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyRemovePeer(r *pb.RemovePeer, cs *raftpb.ConfState) error {
	err := mr.storage.RemovePeer(r.NodeID, cs)
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

func (mr *RaftMetadataRepository) numCommitSince(lsID types.LogStreamID, glsn types.GLSN) uint64 {
	var num uint64

	highest := mr.storage.getHighWatermarkNoLock()

	for glsn < highest {
		gls := mr.storage.lookupNextGLSNoLock(glsn)
		if gls == nil {
			mr.logger.Panic("gls should be exist",
				zap.Uint64("highest", uint64(highest)),
				zap.Uint64("cur", uint64(glsn)),
				zap.Uint64("first", uint64(mr.storage.getFirstGLSNoLock().GetHighWatermark())),
				zap.Uint64("last", uint64(mr.storage.getLastGLSNoLock().GetHighWatermark())),
			)
		}

		r := getCommitResultFromGLS(gls, lsID)
		if r == nil {
			mr.logger.Panic("ls should be exist",
				zap.Uint64("lsID", uint64(lsID)),
				zap.Uint64("highest", uint64(highest)),
				zap.Uint64("cur", uint64(glsn)),
			)
		}

		num += uint64(r.CommittedGLSNLength)
		glsn = gls.HighWatermark
	}

	return num
}

func (mr *RaftMetadataRepository) calculateCommit(replicas *pb.MetadataRepositoryDescriptor_LocalLogStreamReplicas) (types.GLSN, types.GLSN, uint64) {
	var trimHWM types.GLSN = types.MaxGLSN
	var knownHWM types.GLSN = types.InvalidGLSN
	var beginLLSN types.LLSN = types.InvalidLLSN
	var endLLSN types.LLSN = types.InvalidLLSN

	if replicas == nil {
		return types.InvalidGLSN, types.InvalidGLSN, 0
	}

	if len(replicas.Replicas) < mr.nrReplica {
		return types.InvalidGLSN, types.InvalidGLSN, 0
	}

	for _, l := range replicas.Replicas {
		if beginLLSN.Invalid() || l.UncommittedLLSNOffset > beginLLSN {
			beginLLSN = l.UncommittedLLSNOffset
		}

		if endLLSN.Invalid() || l.UncommittedLLSNEnd() < endLLSN {
			endLLSN = l.UncommittedLLSNEnd()
		}

		if knownHWM.Invalid() || l.KnownHighWatermark > knownHWM {
			// knownHighWatermark 이 다르다면,
			// 일부 SN 이 commitResult 를 받지 못했을 뿐이다.
			knownHWM = l.KnownHighWatermark
		}

		if l.KnownHighWatermark < trimHWM {
			trimHWM = l.KnownHighWatermark
		}
	}

	if trimHWM == types.MaxGLSN {
		trimHWM = types.InvalidGLSN
	}

	if beginLLSN > endLLSN {
		return knownHWM, trimHWM, 0
	}

	return knownHWM, trimHWM, uint64(endLLSN - beginLLSN)
}

func (mr *RaftMetadataRepository) getLastCommitted(lsID types.LogStreamID) types.GLSN {
	gls := mr.storage.GetLastGLS()
	if gls == nil {
		return types.InvalidGLSN
	}

	r := getCommitResultFromGLS(gls, lsID)
	if r == nil {
		// newbie
		return types.InvalidGLSN
	}

	if r.CommittedGLSNOffset+types.GLSN(r.CommittedGLSNLength) == types.InvalidGLSN {
		return types.InvalidGLSN
	}

	return r.CommittedGLSNOffset + types.GLSN(r.CommittedGLSNLength) - types.GLSN(1)
}

func (mr *RaftMetadataRepository) proposeCommit() {
	if !mr.isLeader() {
		return
	}

	r := &pb.Commit{}
	mr.propose(context.TODO(), r, false)
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
	e.NodeIndex = uint64(mr.nodeID)
	e.RequestIndex = UnusedRequestIndex

	if guarantee {
		c := make(chan error, 1)
		e.RequestIndex = atomic.AddUint64(&mr.requestNum, 1)
		mr.requestMap.Store(e.RequestIndex, c)
		defer mr.requestMap.Delete(e.RequestIndex)

		t := time.NewTimer(mr.options.RaftProposeTimeout)
		defer t.Stop()

	PROPOSE:
		select {
		case mr.proposeC <- e:
		case <-t.C:
			t.Reset(mr.options.RaftProposeTimeout)
			goto PROPOSE
		case <-ctx.Done():
			return ctx.Err()
		}

		select {
		case err := <-c:
			return err
		case <-t.C:
			t.Reset(mr.options.RaftProposeTimeout)
			goto PROPOSE
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

func (mr *RaftMetadataRepository) proposeConfChange(ctx context.Context, r raftpb.ConfChange) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case mr.rnConfChangeC <- r:
	}

	return nil
}

func (mr *RaftMetadataRepository) RegisterStorageNode(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) error {
	r := &pb.RegisterStorageNode{
		StorageNode: sn,
	}

	err := mr.propose(ctx, r, true)
	if err != varlog.ErrIgnore &&
		err != varlog.ErrAlreadyExists {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	r := &pb.UnregisterStorageNode{
		StorageNodeID: snID,
	}

	err := mr.propose(ctx, r, true)
	if err != varlog.ErrIgnore &&
		err != varlog.ErrNotExist {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.RegisterLogStream{
		LogStream: ls,
	}

	err := mr.propose(ctx, r, true)
	if err != varlog.ErrIgnore &&
		err != varlog.ErrAlreadyExists {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	r := &pb.UnregisterLogStream{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != varlog.ErrIgnore &&
		err != varlog.ErrNotExist {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.UpdateLogStream{
		LogStream: ls,
	}

	err := mr.propose(ctx, r, true)
	if err != varlog.ErrIgnore {
		return err
	}

	return nil
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
		return types.InvalidGLSN, err
	}

	return mr.getLastCommitted(lsID), nil
}

func (mr *RaftMetadataRepository) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	r := &pb.Unseal{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != varlog.ErrIgnore {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error {
	if mr.raftNode.membership.isMember(nodeID) {
		return varlog.ErrAlreadyExists
	}

	r := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(nodeID),
		Context: []byte(url),
	}

	timer := time.NewTimer(mr.raftNode.raftTick)
	defer timer.Stop()

	for !mr.raftNode.membership.isMember(nodeID) {
		if err := mr.proposeConfChange(ctx, r); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(mr.raftNode.raftTick)
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	if !mr.raftNode.membership.isMember(nodeID) {
		return varlog.ErrNotExist
	}

	r := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: uint64(nodeID),
	}

	timer := time.NewTimer(mr.raftNode.raftTick)
	defer timer.Stop()

	for mr.raftNode.membership.isMember(nodeID) {
		if err := mr.proposeConfChange(ctx, r); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(mr.raftNode.raftTick)
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (types.NodeID, []string, error) {
	return types.NodeID(mr.raftNode.membership.getLeader()), mr.raftNode.GetMembership(), nil
}

func (mr *RaftMetadataRepository) GetServerAddr() string {
	return mr.serverAddr
}

func (mr *RaftMetadataRepository) GetReportCount() uint64 {
	return atomic.LoadUint64(&mr.nrReport)
}

func (mr *RaftMetadataRepository) GetHighWatermark() types.GLSN {
	return mr.storage.GetHighWatermark()
}

func (mr *RaftMetadataRepository) GetMinHighWatermark() types.GLSN {
	return mr.storage.GetMinHighWatermark()
}
