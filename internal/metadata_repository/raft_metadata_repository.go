package metadata_repository

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/storagenode/reportcommitter"
	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/util/netutil"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/runner/stopwaiter"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	PROMOTE_RATE = 0.9
)

type ReportCollectorHelper interface {
	ProposeReport(types.StorageNodeID, []*snpb.LogStreamUncommitReport) error

	GetReporterClient(context.Context, *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error)

	LookupNextCommitResults(types.GLSN) (*mrpb.LogStreamCommitResults, error)
}

type RaftMetadataRepository struct {
	clusterID             types.ClusterID
	nodeID                types.NodeID
	nrReplica             int
	reportCollector       ReportCollector
	raftNode              *raftNode
	reporterClientFac     ReporterClientFactory
	snManagementClientFac StorageNodeManagementClientFactory

	storage         *MetadataStorage
	stateMachineLog StateMachineLog

	// for ack
	requestNum uint64
	requestMap sync.Map

	// for raft
	proposeC      chan *mrpb.RaftEntry
	commitC       chan *committedEntry
	rnConfChangeC chan raftpb.ConfChange
	rnProposeC    chan string
	rnCommitC     chan *raftCommittedEntry

	listenNotifyC chan struct{}

	options *MetadataRepositoryOptions
	logger  *zap.Logger

	sw     *stopwaiter.StopWaiter
	runner *runner.Runner
	cancel context.CancelFunc

	server       *grpc.Server
	healthServer *health.Server
	debugServer  *http.Server
	endpointAddr atomic.Value

	// membership
	membership Membership

	nrReport uint64

	tmStub *telemetryStub
}

func NewRaftMetadataRepository(options *MetadataRepositoryOptions) *RaftMetadataRepository {
	options.NodeID = types.NewNodeIDFromURL(options.RaftAddress)

	// FIXME(pharrell): Is this good or not? - add the missing local address in peers
	found := false
	for _, peer := range options.Peers {
		if peer == options.RaftAddress {
			found = true
			break
		}
	}
	if !found {
		options.Peers = append(options.Peers, options.RaftAddress)
	}

	if err := options.validate(); err != nil {
		panic(err)
	}

	logger := options.Logger.Named("vmr").With(zap.Any("nodeid", options.NodeID))

	// TODO: use initTimeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tmStub, err := newTelemetryStub(ctx, options.TelemetryOptions.CollectorName, options.NodeID, options.TelemetryOptions.CollectorEndpoint)
	if err != nil {
		logger.Panic("telemetry", zap.Error(err))
	}

	mr := &RaftMetadataRepository{
		clusterID:             options.ClusterID,
		nodeID:                options.NodeID,
		nrReplica:             options.NumRep,
		logger:                logger,
		reporterClientFac:     options.ReporterClientFac,
		snManagementClientFac: options.StorageNodeManagementClientFac,
		options:               options,
		runner:                runner.New("mr", options.Logger),
		sw:                    stopwaiter.New(),
		tmStub:                tmStub,
	}

	mr.storage = NewMetadataStorage(mr.sendAck, options.SnapCount, mr.logger.Named("storage"))
	mr.membership = mr.storage

	mr.stateMachineLog = newStateMachineLog(logger.Named("sml"),
		fmt.Sprintf("%s/sml/%d", options.RaftDir, options.NodeID))

	// TODO:: recover read
	if options.EnableSML && !options.RecoverFromSML {
		if err = mr.stateMachineLog.OpenForWrite(0); err != nil {
			logger.Panic("stateMachineLog", zap.Error(err))
		}
	}

	mr.listenNotifyC = make(chan struct{})

	mr.proposeC = make(chan *mrpb.RaftEntry, 4096)
	mr.commitC = make(chan *committedEntry, 4096)

	mr.rnConfChangeC = make(chan raftpb.ConfChange, 1)
	mr.rnProposeC = make(chan string)

	options.SnapCount = 1
	mr.raftNode = newRaftNode(
		options.RaftOptions,
		mr.storage,
		mr.rnProposeC,
		mr.rnConfChangeC,
		mr.tmStub,
		mr.logger.Named("raftnode"),
	)
	mr.rnCommitC = mr.raftNode.commitC

	mr.reportCollector = NewReportCollector(mr, mr.options.RPCTimeout,
		mr.logger.Named("report"))

	mr.server = grpc.NewServer()
	mr.healthServer = health.NewServer()
	grpc_health_v1.RegisterHealthServer(mr.server, mr.healthServer)
	NewMetadataRepositoryService(mr).Register(mr.server)
	NewManagementService(mr).Register(mr.server)

	return mr
}

func (mr *RaftMetadataRepository) Run() {
	mr.logger.Info("starting metadata repository")

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
	if err := mr.runner.RunC(mctx, mr.processPurge); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if mr.options.DebugAddress != "" {
		if err := mr.runner.RunC(mctx, mr.runDebugServer); err != nil {
			mr.logger.Panic("could not run", zap.Error(err))
		}
	}

	mr.raftNode.start()

	if err := mr.runner.RunC(mctx, func(ctx context.Context) {
		select {
		case <-ctx.Done():
			return
		case <-mr.listenNotifyC:
		}

		if mr.options.RecoverFromSML {
			if err := mr.recoverStateMachine(mctx); err != nil {
				mr.logger.Panic("could not recover", zap.Error(err))
			}
		}

		mr.logger.Info("listening", zap.String("address", mr.options.RPCBindAddress))
		lis, err := netutil.NewStoppableListener(mctx, mr.options.RPCBindAddress)
		if err != nil {
			mr.logger.Panic("could not listen", zap.Error(err))
		}

		addrs, _ := netutil.GetListenerAddrs(lis.Addr())
		mr.endpointAddr.Store(addrs[0])

		if err := mr.runner.RunC(ctx, mr.registerEndpoint); err != nil {
			mr.logger.Panic("could not run", zap.Error(err))
		}

		mr.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

		//TODO:: graceful shutdown
		if err := mr.server.Serve(lis); err != nil && err != verrors.ErrStopped {
			mr.logger.Panic("could not serve", zap.Error(err))
			//r.Close()
		}
	}); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
}

func (mr *RaftMetadataRepository) runDebugServer(ctx context.Context) {
	httpMux := http.NewServeMux()
	mr.debugServer = &http.Server{Addr: mr.options.DebugAddress, Handler: httpMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	defer mr.debugServer.Close()

	httpMux.HandleFunc("/debug/pprof/", pprof.Index)
	httpMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	httpMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	httpMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	httpMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	httpMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	httpMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	httpMux.Handle("/debug/pprof/block", pprof.Handler("block"))

	lis, err := netutil.NewStoppableListener(ctx, mr.options.DebugAddress)
	if err != nil {
		mr.logger.Panic("could not listen", zap.Error(err))
	}

	if err := mr.debugServer.Serve(lis); err != nil && err != verrors.ErrStopped {
		mr.logger.Panic("could not serve", zap.Error(err))
	}
}

//TODO:: handle pendding msg
func (mr *RaftMetadataRepository) Close() error {
	mr.logger.Info("metadata repository closing")
	defer func() {
		mr.sw.Stop()
		mr.logger.Info("metadata repository closed")
	}()

	mr.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

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

	// FIXME (jun, pharrell): Stop gracefully
	mr.server.Stop()
	mr.tmStub.close(context.TODO())
	return nil
}

func (mr *RaftMetadataRepository) Wait() {
	mr.sw.Wait()
}

func (mr *RaftMetadataRepository) isLeader() bool {
	return mr.membership.Leader() == mr.nodeID
}

func (mr *RaftMetadataRepository) hasLeader() bool {
	return uint64(mr.membership.Leader()) != raft.None
}

func (mr *RaftMetadataRepository) clearMembership() {
	mr.membership.Clear()
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
	ticker := time.NewTicker(mr.options.CommitTick)
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
	listenNoti := false

	for c := range mr.commitC {
		if c == nil {
			snap := mr.raftNode.loadSnapshot()
			if snap != nil {
				mr.reportCollector.Reset()

				err := mr.storage.ApplySnapshot(snap.Data, &snap.Metadata.ConfState, snap.Metadata.Index)
				if err != nil {
					mr.logger.Panic("load snapshot fail")
				}

				err = mr.reportCollector.Recover(
					mr.storage.GetStorageNodes(),
					mr.storage.GetLogStreams(),
					mr.storage.GetFirstCommitResults().GetHighWatermark(),
				)
				if err != nil &&
					err != verrors.ErrStopped {
					mr.logger.Panic("recover report collector fail")
				}
			}
		} else if c.leader != raft.None {
			mr.membership.SetLeader(types.NodeID(c.leader))
		}

		if !listenNoti && mr.IsMember() {
			close(mr.listenNotifyC)
			listenNoti = true
		}

		mr.apply(c)
	}
}

func (mr *RaftMetadataRepository) processRNCommit(ctx context.Context) {
	for d := range mr.rnCommitC {
		var c *committedEntry
		var e *mrpb.RaftEntry

		if d != nil {
			e = &mrpb.RaftEntry{}
			switch d.entryType {
			case raftpb.EntryNormal:
				if d.data != nil {
					err := e.Unmarshal(d.data)
					if err != nil {
						mr.logger.Error(err.Error())
						continue
					}
					c = &committedEntry{
						entry: e,
					}
				} else {
					c = &committedEntry{
						leader: d.leader,
					}
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				err := cc.Unmarshal(d.data)
				if err != nil {
					mr.logger.Error(err.Error())
					continue
				}

				switch cc.Type {
				case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
					p := &mrpb.AddPeer{
						NodeID: types.NodeID(cc.NodeID),
						Url:    string(cc.Context),
					}

					if cc.Type == raftpb.ConfChangeAddLearnerNode {
						p.IsLearner = true
					}

					e.Request.SetValue(p)
				case raftpb.ConfChangeRemoveNode:
					p := &mrpb.RemovePeer{
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

func (mr *RaftMetadataRepository) processPurge(ctx context.Context) {
	var ddonec, sdonec, wdonec, mdonec <-chan struct{}
	var derrc, serrc, werrc, merrc <-chan error

	waldir := fmt.Sprintf("%s/wal/%d", mr.options.RaftDir, mr.nodeID)
	smldir := fmt.Sprintf("%s/sml/%d", mr.options.RaftDir, mr.nodeID)
	snapdir := fmt.Sprintf("%s/snap/%d", mr.options.RaftDir, mr.nodeID)

	if mr.options.MaxSnapPurgeCount > 0 {
		ddonec, derrc = fileutil.PurgeFileWithDoneNotify(mr.logger, snapdir, "snap.db",
			mr.options.MaxSnapPurgeCount, purgeInterval, ctx.Done())
		sdonec, serrc = fileutil.PurgeFileWithDoneNotify(mr.logger, snapdir, "snap",
			mr.options.MaxSnapPurgeCount, purgeInterval, ctx.Done())
	}

	if mr.options.MaxWalPurgeCount > 0 {
		wdonec, werrc = fileutil.PurgeFileWithDoneNotify(mr.logger, waldir, "wal",
			mr.options.MaxWalPurgeCount, purgeInterval, ctx.Done())

		mdonec, merrc = fileutil.PurgeFileWithDoneNotify(mr.logger, smldir, "sml",
			mr.options.MaxWalPurgeCount, purgeInterval, ctx.Done())
	}

	for {
		select {
		case e := <-derrc:
			if err, ok := e.(*os.PathError); ok && err.Err == syscall.ENOENT {
				continue
			}
			mr.logger.Fatal("failed to purge snap db file", zap.Error(e))
		case e := <-serrc:
			if err, ok := e.(*os.PathError); ok && err.Err == syscall.ENOENT {
				continue
			}
			mr.logger.Fatal("failed to purge snap file", zap.Error(e))
		case e := <-werrc:
			if err, ok := e.(*os.PathError); ok && err.Err == syscall.ENOENT {
				continue
			}
			mr.logger.Fatal("failed to purge wal file", zap.Error(e))
		case e := <-merrc:
			if err, ok := e.(*os.PathError); ok && err.Err == syscall.ENOENT {
				continue
			}
			mr.logger.Fatal("failed to purge state machine log file", zap.Error(e))
		case <-ctx.Done():
			if ddonec != nil {
				<-ddonec
			}
			if sdonec != nil {
				<-sdonec
			}
			if wdonec != nil {
				<-wdonec
			}
			if mdonec != nil {
				<-mdonec
			}
			return
		}
	}
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

func (mr *RaftMetadataRepository) saveSML(entry *mrpb.StateMachineLogEntry) {
	mr.withTelemetry(context.TODO(), "save_sml", func(ctx context.Context) (interface{}, error) {
		mr.stateMachineLog.Append(entry)
		return nil, nil
	})
}

func (mr *RaftMetadataRepository) apply(c *committedEntry) {
	mr.withTelemetry(context.TODO(), "apply", func(ctx context.Context) (interface{}, error) {
		if c == nil || c.entry == nil {
			return nil, nil
		}

		e := c.entry
		f := e.Request.GetValue()

		if mr.options.EnableSML {
			lentry := &mrpb.StateMachineLogEntry{}
			lentry.AppliedIndex = e.AppliedIndex
			if lentry.Payload.SetValue(f) {
				mr.saveSML(lentry)
			}
		}

		switch r := f.(type) {
		case *mrpb.RegisterStorageNode:
			mr.applyRegisterStorageNode(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UnregisterStorageNode:
			mr.applyUnregisterStorageNode(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.RegisterLogStream:
			mr.applyRegisterLogStream(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UnregisterLogStream:
			mr.applyUnregisterLogStream(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UpdateLogStream:
			mr.applyUpdateLogStream(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.Report:
			mr.applyReport(r)
		case *mrpb.Commit:
			mr.applyCommit(r, e.AppliedIndex)
		case *mrpb.Seal:
			mr.applySeal(r, e.NodeIndex, e.RequestIndex, e.AppliedIndex)
		case *mrpb.Unseal:
			mr.applyUnseal(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.AddPeer:
			mr.applyAddPeer(r, c.confState)
		case *mrpb.RemovePeer:
			mr.applyRemovePeer(r, c.confState)
		case *mrpb.Endpoint:
			mr.applyEndpoint(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.RecoverStateMachine:
			// TODO:: handle duplicated recover entry
			mr.applyRecoverStateMachine(r, e.AppliedIndex, e.NodeIndex, e.RequestIndex)
			if mr.options.EnableSML {
				if err := mr.stateMachineLog.OpenForWrite(0); err != nil {
					mr.logger.Panic("stateMachineLog", zap.Error(err))
				}
			}

		}

		mr.storage.UpdateAppliedIndex(e.AppliedIndex)

		return nil, nil
	})
}

func (mr *RaftMetadataRepository) applyRegisterStorageNode(r *mrpb.RegisterStorageNode, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterStorageNode(r.StorageNode, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.RegisterStorageNode(r.StorageNode, mr.storage.GetHighWatermark())

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterStorageNode(r *mrpb.UnregisterStorageNode, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UnregisterStorageNode(r.StorageNodeID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.UnregisterStorageNode(r.StorageNodeID)

	return nil
}

func (mr *RaftMetadataRepository) applyRegisterLogStream(r *mrpb.RegisterLogStream, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterLogStream(r.LogStream, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	for _, replica := range r.LogStream.Replicas {
		err := mr.reportCollector.RegisterLogStream(replica.StorageNodeID, r.LogStream.LogStreamID)
		if err != nil &&
			err != verrors.ErrExist &&
			err != verrors.ErrStopped {
			mr.logger.Panic("could not register reportcommitter", zap.String("err", err.Error()))
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterLogStream(r *mrpb.UnregisterLogStream, nodeIndex, requestIndex uint64) error {
	ls := mr.storage.LookupLogStream(r.LogStreamID)
	if ls == nil {
		return verrors.ErrNotExist
	}

	err := mr.storage.UnregisterLogStream(r.LogStreamID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	for _, replica := range ls.Replicas {
		err := mr.reportCollector.UnregisterLogStream(replica.StorageNodeID, r.LogStreamID)
		if err != nil &&
			err != verrors.ErrNotExist &&
			err != verrors.ErrStopped {
			mr.logger.Panic("could not unregister reporter", zap.String("err", err.Error()))
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUpdateLogStream(r *mrpb.UpdateLogStream, nodeIndex, requestIndex uint64) error {
	ls := mr.storage.LookupLogStream(r.LogStream.LogStreamID)
	if ls == nil {
		return verrors.ErrNotExist
	}

	err := mr.storage.UpdateLogStream(r.LogStream, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	oldStorageNodes := set.New(len(ls.Replicas))
	for _, replica := range ls.Replicas {
		oldStorageNodes.Add(replica.StorageNodeID)
	}

	updateStorageNodes := set.New(len(r.LogStream.Replicas))
	for _, replica := range r.LogStream.Replicas {
		updateStorageNodes.Add(replica.StorageNodeID)
	}

	oldStorageNodes.Diff(updateStorageNodes).Foreach(func(k interface{}) bool {
		err := mr.reportCollector.UnregisterLogStream(k.(types.StorageNodeID), r.LogStream.LogStreamID)
		if err != nil &&
			err != verrors.ErrNotExist &&
			err != verrors.ErrStopped {
			mr.logger.Panic("could not unregister reporter", zap.String("err", err.Error()))
		}
		return true
	})

	for _, replica := range r.LogStream.Replicas {
		err := mr.reportCollector.RegisterLogStream(replica.StorageNodeID, r.LogStream.LogStreamID)
		if err != nil &&
			err != verrors.ErrExist &&
			err != verrors.ErrStopped {
			mr.logger.Panic("could not register reporter", zap.String("err", err.Error()))
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyReport(r *mrpb.Report) error {
	atomic.AddUint64(&mr.nrReport, 1)

	snID := r.StorageNodeID
	for _, u := range r.UncommitReport {
		s := mr.storage.LookupUncommitReport(u.LogStreamID, snID)
		if s == nil ||
			s.UncommittedLLSNEnd() < u.UncommittedLLSNEnd() ||
			s.HighWatermark < u.HighWatermark {
			mr.storage.UpdateUncommitReport(u.LogStreamID, snID, u)
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyCommit(r *mrpb.Commit, appliedIndex uint64) error {
	if r.GetNodeID() == mr.nodeID {
		mr.tmStub.mb.Records("raft_delay").Record(context.TODO(),
			float64(time.Since(r.CreatedTime).Nanoseconds())/float64(time.Millisecond),
			attribute.KeyValue{
				Key:   "nodeid",
				Value: attribute.StringValue(mr.nodeID.String()),
			})
	}

	_, err := mr.withTelemetry(context.TODO(), "commit", func(ctx context.Context) (interface{}, error) {
		prevCommitResults := mr.storage.GetLastCommitResults()
		curHWM := prevCommitResults.GetHighWatermark()
		trimHWM := types.MaxGLSN
		committedOffset := curHWM + types.GLSN(1)
		totalCommitted := uint64(0)

		crs := &mrpb.LogStreamCommitResults{
			PrevHighWatermark: curHWM,
		}

		if mr.storage.NumUpdateSinceCommit() > 0 {
			lsIDs := mr.storage.GetUncommitReportIDs()

			sort.Slice(lsIDs, func(i, j int) bool { return lsIDs[i] < lsIDs[j] })

			for _, lsID := range lsIDs {
				reports := mr.storage.LookupUncommitReports(lsID)
				knownHWM, minHWM, nrUncommit := mr.calculateCommit(reports)
				if minHWM < trimHWM {
					trimHWM = minHWM
				}

				if reports.Status.Sealed() {
					nrUncommit = 0
				}

				if nrUncommit > 0 {
					if knownHWM != curHWM {
						nrCommitted := mr.numCommitSince(lsID, knownHWM)
						if nrCommitted > nrUncommit {
							msg := fmt.Sprintf("# of uncommit should be bigger than # of commit:: lsID[%v] cur[%v] first[%v] last[%v] reports[%+v] nrCommitted[%v] nrUncommit[%v]",
								lsID, curHWM,
								mr.storage.getFirstCommitResultsNoLock().GetHighWatermark(),
								mr.storage.getLastCommitResultsNoLock().GetHighWatermark(),
								reports,
								nrCommitted, nrUncommit,
							)
							mr.logger.Panic(msg)
						}

						nrUncommit -= nrCommitted
					}
				}

				committedLLSNOffset := types.MinLLSN
				prevCommitResult := prevCommitResults.LookupCommitResult(lsID)
				if prevCommitResult != nil {
					committedLLSNOffset = prevCommitResult.CommittedLLSNOffset + types.LLSN(prevCommitResult.CommittedGLSNLength)
				}

				commit := &snpb.LogStreamCommitResult{
					LogStreamID:         lsID,
					CommittedLLSNOffset: committedLLSNOffset,
					CommittedGLSNOffset: committedOffset,
					CommittedGLSNLength: nrUncommit,
				}

				if nrUncommit > 0 {
					committedOffset = commit.CommittedGLSNOffset + types.GLSN(commit.CommittedGLSNLength)
				} else {
					commit.CommittedGLSNOffset = mr.getLastCommitted(lsID) + types.GLSN(1)
					commit.CommittedGLSNLength = 0
				}

				crs.CommitResults = append(crs.CommitResults, commit)
				totalCommitted += nrUncommit
			}
		}
		crs.HighWatermark = curHWM + types.GLSN(totalCommitted)

		if totalCommitted > 0 {
			if mr.options.EnableSML {
				lentry := &mrpb.StateMachineLogEntry{
					AppliedIndex: appliedIndex,
				}

				lentry.Payload.SetValue(&mrpb.StateMachineLogCommitResult{
					TrimGlsn:     trimHWM,
					CommitResult: crs,
				})
				mr.saveSML(lentry)
			}
			mr.storage.AppendLogStreamCommitHistory(crs)
		}

		if !trimHWM.Invalid() && trimHWM != types.MaxGLSN {
			mr.storage.TrimLogStreamCommitHistory(trimHWM)
		}

		mr.reportCollector.Commit()

		//TODO:: trigger next commit

		return nil, nil
	})

	return err
}

func (mr *RaftMetadataRepository) applySeal(r *mrpb.Seal, nodeIndex, requestIndex, appliedIndex uint64) error {
	mr.applyCommit(nil, appliedIndex)
	err := mr.storage.SealLogStream(r.LogStreamID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUnseal(r *mrpb.Unseal, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UnsealLogStream(r.LogStreamID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyAddPeer(r *mrpb.AddPeer, cs *raftpb.ConfState) error {
	err := mr.storage.AddPeer(r.NodeID, r.Url, r.IsLearner, cs)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyRemovePeer(r *mrpb.RemovePeer, cs *raftpb.ConfState) error {
	err := mr.storage.RemovePeer(r.NodeID, cs)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyEndpoint(r *mrpb.Endpoint, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterEndpoint(r.NodeID, r.Url, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyRecoverStateMachine(r *mrpb.RecoverStateMachine, appliedIndex, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RecoverStateMachine(r.StateMachine, appliedIndex, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.Reset()

	return mr.reportCollector.Recover(
		mr.storage.GetStorageNodes(),
		mr.storage.GetLogStreams(),
		mr.storage.GetFirstCommitResults().GetHighWatermark(),
	)
}

func (mr *RaftMetadataRepository) numCommitSince(lsID types.LogStreamID, glsn types.GLSN) uint64 {
	var num uint64

	highest := mr.storage.getHighWatermarkNoLock()

	for glsn < highest {
		crs := mr.storage.lookupNextCommitResultsNoLock(glsn)
		if crs == nil {
			mr.logger.Panic("gls should be exist",
				zap.Uint64("highest", uint64(highest)),
				zap.Uint64("cur", uint64(glsn)),
				zap.Uint64("first", uint64(mr.storage.getFirstCommitResultsNoLock().GetHighWatermark())),
				zap.Uint64("last", uint64(mr.storage.getLastCommitResultsNoLock().GetHighWatermark())),
			)
		}

		r := crs.LookupCommitResult(lsID)
		if r == nil {
			mr.logger.Panic("ls should be exist",
				zap.Uint64("lsID", uint64(lsID)),
				zap.Uint64("highest", uint64(highest)),
				zap.Uint64("cur", uint64(glsn)),
			)
		}

		num += r.CommittedGLSNLength
		glsn = crs.HighWatermark
	}

	return num
}

func (mr *RaftMetadataRepository) calculateCommit(reports *mrpb.LogStreamUncommitReports) (types.GLSN, types.GLSN, uint64) {
	var trimHWM types.GLSN = types.MaxGLSN
	var knownHWM types.GLSN = types.InvalidGLSN
	var beginLLSN types.LLSN = types.InvalidLLSN
	var endLLSN types.LLSN = types.InvalidLLSN

	if reports == nil {
		return types.InvalidGLSN, types.InvalidGLSN, 0
	}

	if len(reports.Replicas) < mr.nrReplica {
		return types.InvalidGLSN, types.InvalidGLSN, 0
	}

	for _, r := range reports.Replicas {
		if beginLLSN.Invalid() || r.UncommittedLLSNOffset > beginLLSN {
			beginLLSN = r.UncommittedLLSNOffset
		}

		if endLLSN.Invalid() || r.UncommittedLLSNEnd() < endLLSN {
			endLLSN = r.UncommittedLLSNEnd()
		}

		if knownHWM.Invalid() || r.HighWatermark > knownHWM {
			// knownHighWatermark 이 다르다면,
			// 일부 SN 이 commitResult 를 받지 못했을 뿐이다.
			knownHWM = r.HighWatermark
		}

		if r.HighWatermark < trimHWM {
			trimHWM = r.HighWatermark
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
	crs := mr.storage.GetLastCommitResults()
	if crs == nil {
		return types.InvalidGLSN
	}

	r := crs.LookupCommitResult(lsID)
	if r == nil {
		// newbie
		return types.InvalidGLSN
	}

	if r.CommittedGLSNOffset+types.GLSN(r.CommittedGLSNLength) == types.InvalidGLSN {
		return types.InvalidGLSN
	}

	return r.CommittedGLSNOffset + types.GLSN(r.CommittedGLSNLength) - types.GLSN(1)
}

func (mr *RaftMetadataRepository) getLastCommittedLength(lsID types.LogStreamID) uint64 {
	crs := mr.storage.GetLastCommitResults()
	if crs == nil {
		return 0
	}

	return crs.LookupCommitResult(lsID).GetCommittedGLSNLength()
}

func (mr *RaftMetadataRepository) proposeCommit() {
	if !mr.isLeader() {
		return
	}

	r := &mrpb.Commit{
		NodeID:      mr.nodeID,
		CreatedTime: time.Now(),
	}
	mr.propose(context.TODO(), r, false)
}

func (mr *RaftMetadataRepository) proposeReport(snID types.StorageNodeID, ur []*snpb.LogStreamUncommitReport) error {
	r := &mrpb.Report{
		StorageNodeID:  snID,
		UncommitReport: ur,
	}

	return mr.propose(context.TODO(), r, false)
}

func (mr *RaftMetadataRepository) propose(ctx context.Context, r interface{}, guarantee bool) error {
	e := &mrpb.RaftEntry{}
	e.Request.SetValue(r)
	e.NodeIndex = uint64(mr.nodeID)
	e.RequestIndex = UnusedRequestIndex

	if guarantee {
		c := make(chan error, 1)
		rIdx := atomic.AddUint64(&mr.requestNum, 1)

		e.RequestIndex = rIdx
		mr.requestMap.Store(rIdx, c)
		defer mr.requestMap.Delete(rIdx)

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
		case <-ctx.Done():
			return ctx.Err()
		case mr.proposeC <- e:
		default:
			return verrors.ErrIgnore
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
	r := &mrpb.RegisterStorageNode{
		StorageNode: sn,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	r := &mrpb.UnregisterStorageNode{
		StorageNodeID: snID,
	}

	err := mr.propose(ctx, r, true)
	if err != verrors.ErrNotExist {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &mrpb.RegisterLogStream{
		LogStream: ls,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	r := &mrpb.UnregisterLogStream{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != verrors.ErrNotExist {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	r := &mrpb.UpdateLogStream{
		LogStream: ls,
	}

	err := mr.propose(ctx, r, true)
	if err != verrors.ErrIgnore {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	if !mr.IsMember() {
		return nil, verrors.ErrNotMember
	}

	m := mr.storage.GetMetadata()
	mr.logger.Info("GetMetadata",
		zap.Int("SN", len(m.GetStorageNodes())),
		zap.Int("LS", len(m.GetLogStreams())),
	)
	return m, nil
}

func (mr *RaftMetadataRepository) Seal(ctx context.Context, lsID types.LogStreamID) (types.GLSN, error) {
	r := &mrpb.Seal{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != nil && err != verrors.ErrIgnore {
		return types.InvalidGLSN, err
	}

	lastCommitted := mr.getLastCommitted(lsID)
	mr.logger.Info("seal",
		zap.Uint32("lsid", uint32(lsID)),
		zap.Uint64("last", uint64(lastCommitted)))

	return lastCommitted, nil
}

func (mr *RaftMetadataRepository) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	r := &mrpb.Unseal{
		LogStreamID: lsID,
	}

	err := mr.propose(ctx, r, true)
	if err != verrors.ErrIgnore {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error {
	if mr.membership.IsMember(nodeID) ||
		mr.membership.IsLearner(nodeID) {
		return verrors.ErrAlreadyExists
	}

	r := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddLearnerNode,
		NodeID:  uint64(nodeID),
		Context: []byte(url),
	}

	timer := time.NewTimer(mr.raftNode.raftTick)
	defer timer.Stop()

	for !mr.membership.IsMember(nodeID) &&
		!mr.membership.IsLearner(nodeID) {
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
	if !mr.membership.IsMember(nodeID) &&
		!mr.membership.IsLearner(nodeID) {
		return verrors.ErrNotExist
	}

	r := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: uint64(nodeID),
	}

	timer := time.NewTimer(mr.raftNode.raftTick)
	defer timer.Stop()

	for mr.membership.IsMember(nodeID) ||
		mr.membership.IsLearner(nodeID) {
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

func (mr *RaftMetadataRepository) registerEndpoint(ctx context.Context) {
	endpoint := mr.endpointAddr.Load()
	r := &mrpb.Endpoint{
		NodeID: mr.nodeID,
		Url:    endpoint.(string),
	}

	mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.ClusterInfo, error) {
	if !mr.IsMember() {
		return nil, verrors.ErrNotMember
	}

	member := mr.membership.GetPeers()

	clusterInfo := &mrpb.ClusterInfo{
		ClusterID:         mr.options.ClusterID,
		NodeID:            mr.nodeID,
		Leader:            mr.membership.Leader(),
		ReplicationFactor: int32(mr.nrReplica),
	}

	if len(member) > 0 {
		clusterInfo.Members = make(map[types.NodeID]*mrpb.ClusterInfo_Member)

		for nodeID, peer := range member {
			member := &mrpb.ClusterInfo_Member{
				Peer:     peer.URL,
				Endpoint: mr.storage.LookupEndpoint(nodeID),
				Learner:  peer.IsLearner,
			}

			clusterInfo.Members[nodeID] = member
		}
	}

	return clusterInfo, nil
}

func (mr *RaftMetadataRepository) GetServerAddr() string {
	endpoint := mr.endpointAddr.Load()
	if endpoint == nil {
		return ""
	}
	return endpoint.(string)
}

func (mr *RaftMetadataRepository) GetReportCount() uint64 {
	return atomic.LoadUint64(&mr.nrReport)
}

func (mr *RaftMetadataRepository) GetHighWatermark() types.GLSN {
	return mr.storage.GetHighWatermark()
}

func (mr *RaftMetadataRepository) GetPrevHighWatermark() types.GLSN {
	r := mr.storage.GetLastCommitResults()
	if r == nil {
		return types.InvalidGLSN
	}
	return r.PrevHighWatermark
}

func (mr *RaftMetadataRepository) GetMinHighWatermark() types.GLSN {
	return mr.storage.GetMinHighWatermark()
}

func (mr *RaftMetadataRepository) IsMember() bool {
	return mr.hasLeader() && mr.storage.IsMember(mr.nodeID)
}

func (mr *RaftMetadataRepository) IsLearner() bool {
	return mr.hasLeader() && mr.storage.IsLearner(mr.nodeID)
}

func (mr *RaftMetadataRepository) ProposeReport(snID types.StorageNodeID, ur []*snpb.LogStreamUncommitReport) error {
	return mr.proposeReport(snID, ur)
}

func (mr *RaftMetadataRepository) GetReporterClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return mr.reporterClientFac.GetReporterClient(ctx, sn)
}

func (mr *RaftMetadataRepository) GetSNManagementClient(ctx context.Context, address string) (snc.StorageNodeManagementClient, error) {
	return mr.snManagementClientFac.GetManagementClient(ctx, mr.clusterID, address, mr.logger)
}

func (mr *RaftMetadataRepository) LookupNextCommitResults(glsn types.GLSN) (*mrpb.LogStreamCommitResults, error) {
	return mr.storage.LookupNextCommitResults(glsn)
}

type handler func(ctx context.Context) (interface{}, error)

func (mr *RaftMetadataRepository) withTelemetry(ctx context.Context, name string, h handler) (interface{}, error) {
	st := time.Now()
	rsp, err := h(ctx)
	mr.tmStub.mb.Records(name).Record(ctx,
		float64(time.Since(st).Nanoseconds())/float64(time.Millisecond),
		attribute.KeyValue{
			Key:   "nodeid",
			Value: attribute.StringValue(mr.nodeID.String()),
		})
	return rsp, err
}

func (mr *RaftMetadataRepository) recoverStateMachine(ctx context.Context) error {
	storage := NewMetadataStorage(nil, 0, mr.logger.Named("storage"))

	err := mr.restoreStateMachineFromStateMachineLog(ctx, storage)
	if err != nil {
		return err
	}

	// TODO:: sync with SNs. recover commitResults & UncommitReports
	err = mr.syncStateMachineFromStorageNodes(ctx, storage)
	if err != nil {
		return err
	}

	r := &mrpb.RecoverStateMachine{
		StateMachine: storage.origStateMachine,
	}

	mr.propose(ctx, r, true)

	return nil
}

func (mr *RaftMetadataRepository) restoreStateMachineFromStateMachineLog(ctx context.Context, storage *MetadataStorage) error {
	logIndex := uint64(0)

	snap := mr.raftNode.loadSnapshot()
	if snap != nil {
		err := storage.ApplySnapshot(snap.Data, nil, 0)
		if err != nil {
			return err
		}

		logIndex = snap.Metadata.Index + 1
	}

	ents, err := mr.stateMachineLog.ReadFrom(logIndex)
	if err != nil {
		mr.logger.Warn("read stateMachineLog", zap.Uint64("from", logIndex), zap.Error(err))
	}

	for _, ent := range ents {
		f := ent.Payload.GetValue()
		switch r := f.(type) {
		case *mrpb.RegisterStorageNode:
			storage.RegisterStorageNode(r.StorageNode, 0, 0)
		case *mrpb.UnregisterStorageNode:
			storage.UnregisterStorageNode(r.StorageNodeID, 0, 0)
		case *mrpb.RegisterLogStream:
			storage.RegisterLogStream(r.LogStream, 0, 0)
		case *mrpb.UnregisterLogStream:
			storage.UnregisterLogStream(r.LogStreamID, 0, 0)
		case *mrpb.UpdateLogStream:
			storage.UpdateLogStream(r.LogStream, 0, 0)
		case *mrpb.StateMachineLogCommitResult:
			storage.AppendLogStreamCommitHistory(r.CommitResult)
			if !r.TrimGlsn.Invalid() {
				storage.TrimLogStreamCommitHistory(r.TrimGlsn)
			}
		}
	}

	// reset applied index & merge state machine
	storage.UpdateAppliedIndex(0)

	return nil
}

func (mr *RaftMetadataRepository) syncStateMachineFromStorageNodes(ctx context.Context, storage *MetadataStorage) error {
	syncer, err := NewStateMachineSyncer(mr.options.SyncStorageNodes, mr.nrReplica, mr.GetSNManagementClient)
	if err != nil {
		return err
	}

	defer syncer.Close()

	return syncer.SyncCommitResults(ctx, storage)
}
