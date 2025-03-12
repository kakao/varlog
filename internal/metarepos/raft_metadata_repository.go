package metarepos

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/pprof"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gogo/status"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/reportcommitter"
	"github.com/kakao/varlog/pkg/rpc"
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
	PromoteRate = 0.9
)

type ReportCollectorHelper interface {
	ProposeReport(types.StorageNodeID, []snpb.LogStreamUncommitReport) error

	GetReporterClient(context.Context, *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error)

	GetLastCommitResults() *mrpb.LogStreamCommitResults

	LookupNextCommitResults(types.Version) (*mrpb.LogStreamCommitResults, error)
}

type RaftMetadataRepository struct {
	config

	reportCollector ReportCollector
	raftNode        *raftNode
	storage         *MetadataStorage

	// for ack
	requestNum atomic.Uint64
	requestMap sync.Map

	// for raft
	proposeC      chan *mrpb.RaftEntry
	commitC       chan *committedEntry
	rnConfChangeC chan raftpb.ConfChange
	rnProposeC    chan []byte
	rnCommitC     chan *raftCommittedEntry

	// for report
	reportQueue   []*mrpb.Report
	muReportQueue sync.Mutex

	listenNotifyC chan struct{}

	sw     *stopwaiter.StopWaiter
	runner *runner.Runner
	cancel context.CancelFunc

	server       *grpc.Server
	healthServer *health.Server
	debugServer  *http.Server
	endpointAddr atomic.Value

	// membership
	membership Membership

	nrReport            atomic.Uint64
	nrReportSinceCommit uint64

	// commit helper
	topicEndPos map[types.TopicID]int

	tmStub *telemetryStub
}

func NewRaftMetadataRepository(opts ...Option) *RaftMetadataRepository {
	cfg, err := newConfig(opts)
	if err != nil {
		panic(err)
	}

	tmStub, err := newTelemetryStub(context.Background(), cfg.telemetryCollectorName, cfg.nodeID, cfg.telemetryCollectorEndpoint)
	if err != nil {
		cfg.logger.Panic("telemetry", zap.Error(err))
	}

	mr := &RaftMetadataRepository{
		config:        cfg,
		proposeC:      make(chan *mrpb.RaftEntry, 4096),
		commitC:       make(chan *committedEntry, 4096),
		rnConfChangeC: make(chan raftpb.ConfChange, 1),
		rnProposeC:    make(chan []byte),
		reportQueue:   mrpb.NewReportQueue(),
		runner:        runner.New("mr", cfg.logger),
		sw:            stopwaiter.New(),
		tmStub:        tmStub,
		topicEndPos:   make(map[types.TopicID]int),
	}
	mr.requestNum.Store(uint64(time.Now().UnixNano()))
	mr.storage = NewMetadataStorage(mr.sendAck, cfg.snapCount, mr.logger.Named("storage"))
	mr.storage.limits.maxTopicsCount = mr.maxTopicsCount
	mr.storage.limits.maxLogStreamsCountPerTopic = mr.maxLogStreamsCountPerTopic

	mr.membership = mr.storage

	mr.listenNotifyC = make(chan struct{})

	mr.snapCount = 1
	mr.raftNode = newRaftNode(
		mr.raftConfig,
		mr.storage,
		mr.rnProposeC,
		mr.rnConfChangeC,
		mr.tmStub,
		mr.logger.Named("raftnode"),
	)
	mr.rnCommitC = mr.raftNode.commitC

	mr.reportCollector = NewReportCollector(mr, mr.rpcTimeout, mr.tmStub, mr.logger.Named("report"))

	mr.server = rpc.NewServer(mr.defaultGRPCServerOptions...)
	mr.healthServer = health.NewServer()
	grpc_health_v1.RegisterHealthServer(mr.server, mr.healthServer)
	NewMetadataRepositoryService(mr).Register(mr.server)
	NewManagementService(mr).Register(mr.server)

	return mr
}

func (mr *RaftMetadataRepository) Run() {
	mr.logger.Info("starting metadata repository")

	mr.storage.Run()
	mr.reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.

	mctx, cancel := mr.runner.WithManagedCancel(context.Background())

	mr.cancel = cancel
	if err := mr.runner.RunC(mctx, mr.runReplication); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.processCommit); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.processReport); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.processRNCommit); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if err := mr.runner.RunC(mctx, mr.processPurge); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
	if mr.debugAddr != "" {
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

		// commit trigger should run after recover complete
		if err := mr.runner.RunC(mctx, mr.runCommitTrigger); err != nil {
			mr.logger.Panic("could not run", zap.Error(err))
		}

		mr.logger.Info("listening", zap.String("address", mr.rpcAddr))
		lis, err := netutil.NewStoppableListener(mctx, mr.rpcAddr)
		if err != nil {
			mr.logger.Panic("could not listen", zap.Error(err))
		}

		addrs, _ := netutil.GetListenerAddrs(lis.Addr())
		mr.endpointAddr.Store(addrs[0])

		if err := mr.runner.RunC(ctx, mr.registerEndpoint); err != nil {
			mr.logger.Panic("could not run", zap.Error(err))
		}

		mr.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

		// TODO:: graceful shutdown
		if err := mr.server.Serve(lis); err != nil && err != verrors.ErrStopped {
			mr.logger.Panic("could not serve", zap.Error(err))
			// r.Close()
		}
	}); err != nil {
		mr.logger.Panic("could not run", zap.Error(err))
	}
}

func (mr *RaftMetadataRepository) runDebugServer(ctx context.Context) {
	httpMux := http.NewServeMux()
	mr.debugServer = &http.Server{
		Addr: mr.debugAddr, Handler: httpMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	defer mr.debugServer.Close() //nolint:errcheck,revive // TODO:: Handle an error returned.

	httpMux.HandleFunc("/debug/pprof/", pprof.Index)
	httpMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	httpMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	httpMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	httpMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	httpMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	httpMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	httpMux.Handle("/debug/pprof/block", pprof.Handler("block"))

	lis, err := netutil.NewStoppableListener(ctx, mr.debugAddr)
	if err != nil {
		mr.logger.Panic("could not listen", zap.Error(err))
	}

	if err := mr.debugServer.Serve(lis); err != nil && err != verrors.ErrStopped {
		mr.logger.Panic("could not serve", zap.Error(err))
	}
}

// TODO:: handle pendding msg
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
				e.Release()
				continue
			}
			nodeIndex, requestIndex := e.NodeIndex, e.RequestIndex
			e.Release()

			select {
			case mr.rnProposeC <- b:
			case <-ctx.Done():
				mr.sendAck(nodeIndex, requestIndex, ctx.Err())
			}
		case <-ctx.Done():
			break Loop
		}
	}
}

func (mr *RaftMetadataRepository) runCommitTrigger(ctx context.Context) {
	ticker := time.NewTicker(mr.commitTick)
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

func (mr *RaftMetadataRepository) processReport(ctx context.Context) {
	ticker := time.NewTicker(mr.commitTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var reports *mrpb.Reports

			mr.muReportQueue.Lock()
			num := len(mr.reportQueue)
			if num > 0 {
				reports = mrpb.NewReports(mr.nodeID, time.Now())
				reports.Reports = mr.reportQueue
				mr.reportQueue = mrpb.NewReportQueue()
			}
			mr.muReportQueue.Unlock()

			if reports != nil {
				mr.propose(context.TODO(), reports, false) //nolint:errcheck,revive // TODO:: Handle an error returned.
			}
		}
	}
}

func (mr *RaftMetadataRepository) processCommit(context.Context) {
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
					mr.storage.GetFirstCommitResults().GetVersion(),
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

func (mr *RaftMetadataRepository) processRNCommit(context.Context) {
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

	waldir := fmt.Sprintf("%s/wal/%d", mr.raftDir, mr.nodeID)
	smldir := fmt.Sprintf("%s/sml/%d", mr.raftDir, mr.nodeID)
	snapdir := fmt.Sprintf("%s/snap/%d", mr.raftDir, mr.nodeID)

	if mr.maxSnapPurgeCount > 0 {
		ddonec, derrc = fileutil.PurgeFileWithDoneNotify(mr.logger, snapdir, "snap.db",
			mr.maxSnapPurgeCount, purgeInterval, ctx.Done())
		sdonec, serrc = fileutil.PurgeFileWithDoneNotify(mr.logger, snapdir, "snap",
			mr.maxSnapPurgeCount, purgeInterval, ctx.Done())
	}

	if mr.maxWalPurgeCount > 0 {
		wdonec, werrc = fileutil.PurgeFileWithDoneNotify(mr.logger, waldir, "wal",
			mr.maxWalPurgeCount, purgeInterval, ctx.Done())

		mdonec, merrc = fileutil.PurgeFileWithDoneNotify(mr.logger, smldir, "sml",
			mr.maxWalPurgeCount, purgeInterval, ctx.Done())
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

func (mr *RaftMetadataRepository) apply(c *committedEntry) {
	mr.withTelemetry(context.TODO(), "apply", func(ctx context.Context) (interface{}, error) { //nolint:errcheck,revive // TODO:: Handle an error returned.
		if c == nil || c.entry == nil {
			return nil, nil
		}

		e := c.entry
		f := e.Request.GetValue()

		//nolint:errcheck,revive // TODO:: Handle an error returned.
		switch r := f.(type) {
		case *mrpb.RegisterStorageNode:
			mr.applyRegisterStorageNode(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UnregisterStorageNode:
			mr.applyUnregisterStorageNode(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.RegisterTopic:
			mr.applyRegisterTopic(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UnregisterTopic:
			mr.applyUnregisterTopic(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.RegisterLogStream:
			mr.applyRegisterLogStream(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UnregisterLogStream:
			mr.applyUnregisterLogStream(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.UpdateLogStream:
			mr.applyUpdateLogStream(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.Reports:
			mr.applyReport(r)
		case *mrpb.Commit:
			mr.applyCommit(r, e.AppliedIndex)
		case *mrpb.Seal:
			mr.applySeal(r, e.NodeIndex, e.RequestIndex, e.AppliedIndex)
		case *mrpb.Unseal:
			mr.applyUnseal(r, e.NodeIndex, e.RequestIndex)
		case *mrpb.AddPeer:
			mr.applyAddPeer(r, c.confState, e.AppliedIndex)
		case *mrpb.RemovePeer:
			mr.applyRemovePeer(r, c.confState, e.AppliedIndex)
		case *mrpb.Endpoint:
			mr.applyEndpoint(r, e.NodeIndex, e.RequestIndex)
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

	mr.reportCollector.RegisterStorageNode(r.StorageNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterStorageNode(r *mrpb.UnregisterStorageNode, nodeIndex, requestIndex uint64) error {
	err := mr.storage.UnregisterStorageNode(r.StorageNodeID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	mr.reportCollector.UnregisterStorageNode(r.StorageNodeID) //nolint:errcheck,revive // TODO:: Handle an error returned.

	return nil
}

func (mr *RaftMetadataRepository) applyRegisterTopic(r *mrpb.RegisterTopic, nodeIndex, requestIndex uint64) error {
	topicDesc := &varlogpb.TopicDescriptor{
		TopicID: r.TopicID,
	}
	err := mr.storage.RegisterTopic(topicDesc, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterTopic(r *mrpb.UnregisterTopic, nodeIndex, requestIndex uint64) error {
	topic := mr.storage.lookupTopic(r.TopicID)
	if topic == nil {
		return verrors.ErrNotExist
	}

	for _, lsID := range topic.LogStreams {
		ls := mr.storage.lookupLogStream(lsID)
		if ls == nil {
			continue
		}

		err := mr.storage.unregisterLogStream(lsID)
		if err != nil {
			continue
		}

		for _, replica := range ls.Replicas {
			err := mr.reportCollector.UnregisterLogStream(replica.StorageNodeID, lsID)
			if err != nil &&
				err != verrors.ErrNotExist &&
				err != verrors.ErrStopped {
				mr.logger.Panic("could not unregister reporter", zap.String("err", err.Error()))
			}
		}
	}

	err := mr.storage.UnregisterTopic(r.TopicID, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyRegisterLogStream(r *mrpb.RegisterLogStream, nodeIndex, requestIndex uint64) error {
	err := mr.storage.RegisterLogStream(r.LogStream, nodeIndex, requestIndex)
	if err != nil {
		return err
	}

	for _, replica := range r.LogStream.Replicas {
		err := mr.reportCollector.RegisterLogStream(r.GetLogStream().GetTopicID(), replica.StorageNodeID, r.LogStream.LogStreamID, mr.GetLastCommitVersion(), varlogpb.LogStreamStatusRunning)
		if err != nil &&
			err != verrors.ErrExist &&
			err != verrors.ErrStopped {
			mr.logger.Panic("could not register reportcommitter", zap.String("err", err.Error()))
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyUnregisterLogStream(r *mrpb.UnregisterLogStream, nodeIndex, requestIndex uint64) error {
	ls := mr.storage.lookupLogStream(r.LogStreamID)
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
	ls := mr.storage.lookupLogStream(r.LogStream.LogStreamID)
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

	rcstatus := varlogpb.LogStreamStatusRunning
	if ls.Status == varlogpb.LogStreamStatusSealed {
		rcstatus = varlogpb.LogStreamStatusSealed
	}

	for _, replica := range r.LogStream.Replicas {
		err := mr.reportCollector.RegisterLogStream(ls.GetTopicID(), replica.StorageNodeID, r.LogStream.LogStreamID, mr.GetLastCommitVersion(), rcstatus)
		if err != nil &&
			err != verrors.ErrExist &&
			err != verrors.ErrStopped {
			mr.logger.Panic("could not register reporter", zap.String("err", err.Error()))
		}
	}

	return nil
}

func (mr *RaftMetadataRepository) applyReport(reports *mrpb.Reports) error {
	mr.nrReport.Add(1)
	mr.nrReportSinceCommit++

	mr.tmStub.mb.Records("mr.raft.reports.delay").Record(context.TODO(),
		float64(time.Since(reports.CreatedTime).Nanoseconds())/float64(time.Millisecond),
	)

	for _, r := range reports.Reports {
		snID := r.StorageNodeID
	LS:
		for _, u := range r.UncommitReport {
			if u.Invalid() {
				continue LS
			}

			s, ok := mr.storage.LookupUncommitReport(u.LogStreamID, snID)
			if !ok {
				continue LS
			}

			if (s.Version == u.Version &&
				s.UncommittedLLSNEnd() < u.UncommittedLLSNEnd()) ||
				s.Version < u.Version {
				if s.UncommittedLLSNEnd() > u.UncommittedLLSNEnd() {
					if ce := mr.logger.Check(zap.DebugLevel, "unexpected report"); ce != nil {
						ce.Write(
							zap.Any("nodeID", reports.GetNodeID()),
							zap.Any("sn", snID),
							zap.Any("ls", u.GetLogStreamID()),
							zap.Any("ver", u.GetVersion()),
							zap.Any("off", u.GetUncommittedLLSNOffset()),
							zap.Any("end", u.UncommittedLLSNEnd()),
							zap.Any("cver", s.GetVersion()),
							zap.Any("coff", s.GetUncommittedLLSNOffset()),
							zap.Any("cend", s.UncommittedLLSNEnd()),
						)
					}
					continue LS
				}
				mr.storage.UpdateUncommitReport(u.LogStreamID, snID, u)
			}
		}
	}

	return nil
}

func topicBoundary(topicLSIDs []TopicLSID, idx int) (begin bool, end bool) {
	if idx == 0 {
		begin = true
	} else {
		begin = topicLSIDs[idx].TopicID != topicLSIDs[idx-1].TopicID
	}

	if idx == len(topicLSIDs)-1 {
		end = true
	} else {
		end = topicLSIDs[idx].TopicID != topicLSIDs[idx+1].TopicID
	}

	return
}

func (mr *RaftMetadataRepository) applyCommit(r *mrpb.Commit, appliedIndex uint64) error {
	if r != nil {
		mr.tmStub.mb.Records("mr.raft.commit.delay").Record(context.TODO(),
			float64(time.Since(r.CreatedTime).Nanoseconds())/float64(time.Millisecond),
		)
	}

	_, err := mr.withTelemetry(context.TODO(), "mr.build_commit_results.duration", func(ctx context.Context) (interface{}, error) {
		defer mr.storage.ResetUpdateSinceCommit()

		prevCommitResults := mr.storage.getLastCommitResultsNoLock()
		curVer := prevCommitResults.GetVersion()
		trimVer := types.MaxVersion

		totalCommitted := uint64(0)

		crs := &mrpb.LogStreamCommitResults{}

		mr.tmStub.mb.Records("mr.reports_log.count").Record(context.Background(), float64(mr.nrReportSinceCommit))

		mr.tmStub.mb.Records("mr.update_reports.count").Record(context.Background(),
			float64(mr.storage.NumUpdateSinceCommit()),
		)

		mr.nrReportSinceCommit = 0

		if mr.storage.NumUpdateSinceCommit() > 0 {
			st := time.Now()

			topicLSIDs := mr.storage.GetSortedTopicLogStreamIDs()
			crs.CommitResults = make([]snpb.LogStreamCommitResult, 0, len(topicLSIDs))

			commitResultsMap := make(map[types.Version]*mrpb.LogStreamCommitResults)

			committedOffset := types.InvalidGLSN

			// TODO:: apply topic
			for idx, topicLSID := range topicLSIDs {
				beginTopic, endTopic := topicBoundary(topicLSIDs, idx)

				if beginTopic {
					hpos := mr.topicEndPos[topicLSID.TopicID]

					committedOffset, hpos = prevCommitResults.LastHighWatermark(topicLSID.TopicID, hpos)
					committedOffset += types.GLSN(1)

					mr.topicEndPos[topicLSID.TopicID] = hpos
				}

				reports := mr.storage.LookupUncommitReports(topicLSID.LogStreamID)
				knownVer, minVer, knownHWM, nrUncommit := mr.calculateCommit(reports)
				if reports.Status.Sealed() {
					nrUncommit = 0
				}

				if reports.Status == varlogpb.LogStreamStatusSealed {
					minVer = curVer
				}

				if reports.Status == varlogpb.LogStreamStatusSealing &&
					mr.getLastCommitted(topicLSID.TopicID, topicLSID.LogStreamID, idx) <= knownHWM {
					if err := mr.storage.SealLogStream(topicLSID.LogStreamID, 0, 0); err == nil {
						mr.reportCollector.Seal(topicLSID.LogStreamID)
					}
				}

				if minVer < trimVer {
					trimVer = minVer
				}

				if nrUncommit > 0 {
					if knownVer != curVer {
						baseCommitResults, ok := commitResultsMap[knownVer]
						if !ok {
							baseCommitResults = mr.storage.lookupNextCommitResultsNoLock(knownVer)
							if baseCommitResults == nil {
								mr.logger.Panic("commit history should be exist",
									zap.Any("ver", knownVer),
									zap.Any("first", mr.storage.getFirstCommitResultsNoLock().GetVersion()),
									zap.Any("last", mr.storage.getLastCommitResultsNoLock().GetVersion()),
								)
							}

							commitResultsMap[knownVer] = baseCommitResults
						}

						nrCommitted := mr.numCommitSince(topicLSID.TopicID, topicLSID.LogStreamID, baseCommitResults, prevCommitResults, idx)
						if nrCommitted > nrUncommit {
							msg := fmt.Sprintf("# of uncommit should be bigger than # of commit:: lsID[%v] cur[%v] first[%v] last[%v] reports[%+v] nrCommitted[%v] nrUncommit[%v]",
								topicLSID.LogStreamID, curVer,
								mr.storage.getFirstCommitResultsNoLock().GetVersion(),
								mr.storage.getLastCommitResultsNoLock().GetVersion(),
								reports,
								nrCommitted, nrUncommit,
							)
							mr.logger.Error(msg)
							nrUncommit = 0
						} else {
							nrUncommit -= nrCommitted
						}
					}
				}

				committedLLSNOffset := types.MinLLSN
				prevCommitResult, _, ok := prevCommitResults.LookupCommitResult(topicLSID.TopicID, topicLSID.LogStreamID, idx)
				if ok {
					committedLLSNOffset = prevCommitResult.CommittedLLSNOffset + types.LLSN(prevCommitResult.CommittedGLSNLength)
				}

				commit := snpb.LogStreamCommitResult{
					TopicID:             topicLSID.TopicID,
					LogStreamID:         topicLSID.LogStreamID,
					CommittedLLSNOffset: committedLLSNOffset,
					CommittedGLSNOffset: committedOffset,
					CommittedGLSNLength: nrUncommit,
				}

				if nrUncommit > 0 {
					committedOffset += types.GLSN(commit.CommittedGLSNLength)
				} else {
					commit.CommittedGLSNOffset = mr.getLastCommitted(topicLSID.TopicID, topicLSID.LogStreamID, idx) + types.GLSN(1)
					commit.CommittedGLSNLength = 0
				}

				// set highWatermark of topic
				if endTopic {
					commit.HighWatermark = committedOffset - types.MinGLSN
				}

				crs.CommitResults = append(crs.CommitResults, commit)
				totalCommitted += nrUncommit
			}

			mr.tmStub.mb.Records("mr.build_commit_results.pure.duration").Record(context.TODO(),
				float64(time.Since(st).Nanoseconds())/float64(time.Millisecond),
			)
		}
		crs.Version = curVer + 1

		if totalCommitted > 0 {
			mr.storage.AppendLogStreamCommitHistory(crs)
		}

		if trimVer != 0 && trimVer != math.MaxUint64 {
			mr.storage.TrimLogStreamCommitHistory(trimVer) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}

		mr.reportCollector.Commit()

		// TODO:: trigger next commit

		return nil, nil
	})

	return err
}

func (mr *RaftMetadataRepository) applySeal(r *mrpb.Seal, nodeIndex, requestIndex, appliedIndex uint64) error {
	mr.applyCommit(nil, appliedIndex) //nolint:errcheck,revive // TODO:: Handle an error returned.
	err := mr.storage.SealingLogStream(r.LogStreamID, nodeIndex, requestIndex)
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

	ver := mr.GetLastCommitVersion()
	if ver > 0 {
		// Let logstream know the last commit version.
		// This tricks the committer into thinking that the last version
		// that storagenode knows is last commit version - 1.
		// So, committer delivers last commit result to storagenode.
		ver = ver - 1
	}
	mr.reportCollector.Unseal(r.LogStreamID, ver)

	return nil
}

func (mr *RaftMetadataRepository) applyAddPeer(r *mrpb.AddPeer, cs *raftpb.ConfState, appliedIndex uint64) error {
	err := mr.storage.AddPeer(r.NodeID, r.Url, r.IsLearner, cs, appliedIndex)
	if err != nil {
		return err
	}

	return nil
}

func (mr *RaftMetadataRepository) applyRemovePeer(r *mrpb.RemovePeer, cs *raftpb.ConfState, appliedIndex uint64) error {
	err := mr.storage.RemovePeer(r.NodeID, cs, appliedIndex)
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

func (mr *RaftMetadataRepository) numCommitSince(topicID types.TopicID, lsID types.LogStreamID, base, latest *mrpb.LogStreamCommitResults, hintPos int) uint64 {
	if latest == nil {
		return 0
	}

	start, _, ok := base.LookupCommitResult(topicID, lsID, hintPos)
	if !ok {
		mr.logger.Panic("ls should be exist",
			zap.Uint64("lsID", uint64(lsID)),
		)
	}

	end, _, ok := latest.LookupCommitResult(topicID, lsID, hintPos)
	if !ok {
		mr.logger.Panic("ls should be exist at latest",
			zap.Uint64("lsID", uint64(lsID)),
		)
	}

	return uint64(end.CommittedLLSNOffset-start.CommittedLLSNOffset) + end.CommittedGLSNLength
}

func (mr *RaftMetadataRepository) calculateCommit(reports *mrpb.LogStreamUncommitReports) (types.Version, types.Version, types.GLSN, uint64) {
	trimVer := types.MaxVersion
	knownVer := types.InvalidVersion
	beginLLSN := types.InvalidLLSN
	endLLSN := types.InvalidLLSN
	highWatermark := types.InvalidGLSN

	if reports == nil {
		return types.InvalidVersion, types.InvalidVersion, types.InvalidGLSN, 0
	}

	if len(reports.Replicas) < mr.replicationFactor {
		return types.InvalidVersion, types.InvalidVersion, types.InvalidGLSN, 0
	}

	for _, r := range reports.Replicas {
		if beginLLSN.Invalid() || r.UncommittedLLSNOffset > beginLLSN {
			beginLLSN = r.UncommittedLLSNOffset
		}

		if endLLSN.Invalid() || r.UncommittedLLSNEnd() < endLLSN {
			endLLSN = r.UncommittedLLSNEnd()
		}

		if knownVer.Invalid() || r.Version > knownVer {
			// knownVersion 이 다르다면,
			// 일부 SN 이 commitResult 를 받지 못했을 뿐이다.
			knownVer = r.Version
			highWatermark = r.HighWatermark
		}

		if r.Version < trimVer {
			trimVer = r.Version
		}
	}

	if trimVer == types.MaxVersion {
		trimVer = 0
	}

	if beginLLSN > endLLSN {
		return knownVer, trimVer, highWatermark, 0
	}

	return knownVer, trimVer, highWatermark, uint64(endLLSN - beginLLSN)
}

func (mr *RaftMetadataRepository) getLastCommitted(topicID types.TopicID, lsID types.LogStreamID, hintPos int) types.GLSN {
	crs := mr.storage.GetLastCommitResults()
	if crs == nil {
		return types.InvalidGLSN
	}

	r, _, ok := crs.LookupCommitResult(topicID, lsID, hintPos)
	if !ok {
		// newbie
		return types.InvalidGLSN
	}

	if r.CommittedGLSNOffset+types.GLSN(r.CommittedGLSNLength) == types.InvalidGLSN {
		return types.InvalidGLSN
	}

	return r.CommittedGLSNOffset + types.GLSN(r.CommittedGLSNLength) - types.MinGLSN
}

func (mr *RaftMetadataRepository) getLastCommitVersion(topicID types.TopicID, lsID types.LogStreamID) types.Version {
	crs := mr.storage.GetLastCommitResults()
	if crs == nil {
		return types.InvalidVersion
	}

	_, _, ok := crs.LookupCommitResult(topicID, lsID, -1)
	if !ok {
		// newbie
		return types.InvalidVersion
	}

	return crs.Version
}

func (mr *RaftMetadataRepository) proposeCommit() {
	if !mr.isLeader() {
		return
	}

	r := &mrpb.Commit{
		NodeID:      mr.nodeID,
		CreatedTime: time.Now(),
	}
	mr.propose(context.TODO(), r, false) //nolint:errcheck,revive // TODO:: Handle an error returned.
}

func (mr *RaftMetadataRepository) proposeReport(snID types.StorageNodeID, ur []snpb.LogStreamUncommitReport) error {
	r := &mrpb.Report{
		StorageNodeID:  snID,
		UncommitReport: ur,
	}

	mr.muReportQueue.Lock()
	defer mr.muReportQueue.Unlock()

	mr.reportQueue = append(mr.reportQueue, r)

	return nil
}

func (mr *RaftMetadataRepository) leaseRaftEntry(request any, requestIndex uint64) *mrpb.RaftEntry {
	re := mrpb.NewRaftEntry()
	re.Request.SetValue(request)
	re.NodeIndex = uint64(mr.nodeID)
	re.RequestIndex = requestIndex
	return re
}

func (mr *RaftMetadataRepository) proposeWithoutGuarantee(ctx context.Context, request any) (err error) {
	re := mr.leaseRaftEntry(request, UnusedRequestIndex)
	select {
	case mr.proposeC <- re:
		return nil
	case <-ctx.Done():
		err = ctx.Err()
	default:
		err = verrors.ErrIgnore
	}
	if err != nil {
		re.Release()
	}
	return err
}

func (mr *RaftMetadataRepository) proposeWithGuarantee(ctx context.Context, request any) error {
	rIdx := mr.requestNum.Add(1)
	c := make(chan error, 1)
	mr.requestMap.Store(rIdx, c)
	defer mr.requestMap.Delete(rIdx)

	t := time.NewTimer(mr.raftProposeTimeout)
	defer t.Stop()

	re := mr.leaseRaftEntry(request, rIdx)

Propose:
	select {
	case mr.proposeC <- re:
	case <-t.C:
		t.Reset(mr.raftProposeTimeout)
		goto Propose
	case <-ctx.Done():
		re.Release()
		return ctx.Err()
	}

	select {
	case err := <-c:
		return err
	case <-t.C:
		t.Reset(mr.raftProposeTimeout)
		re = mr.leaseRaftEntry(request, rIdx)
		goto Propose
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mr *RaftMetadataRepository) propose(ctx context.Context, request interface{}, guarantee bool) error {
	if !guarantee {
		return mr.proposeWithoutGuarantee(ctx, request)
	}
	return mr.proposeWithGuarantee(ctx, request)
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

func (mr *RaftMetadataRepository) RegisterTopic(ctx context.Context, topicID types.TopicID) error {
	r := &mrpb.RegisterTopic{
		TopicID: topicID,
	}

	return mr.propose(ctx, r, true)
}

func (mr *RaftMetadataRepository) UnregisterTopic(ctx context.Context, topicID types.TopicID) error {
	r := &mrpb.UnregisterTopic{
		TopicID: topicID,
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

func (mr *RaftMetadataRepository) GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error) {
	if !mr.IsMember() {
		return nil, verrors.ErrNotMember
	}

	m := mr.storage.GetMetadata()
	if ce := mr.logger.Check(zap.DebugLevel, "GetMetadata"); ce != nil {
		ce.Write(
			zap.Int("SN", len(m.GetStorageNodes())),
			zap.Int("LS", len(m.GetLogStreams())),
		)
	}
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

	ls := mr.storage.LookupLogStream(lsID)
	if ls == nil {
		mr.logger.Panic("can't find logStream")
	}

	lastCommitted := mr.getLastCommitted(ls.TopicID, lsID, -1)
	mr.logger.Info("seal",
		zap.Int32("lsid", int32(lsID)),
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

func (mr *RaftMetadataRepository) AddPeer(ctx context.Context, _ types.ClusterID, nodeID types.NodeID, url string) error {
	if nodeID == types.InvalidNodeID {
		return status.Error(codes.InvalidArgument, "invalid node id")
	}

	if mr.membership.IsMember(nodeID) ||
		mr.membership.IsLearner(nodeID) {
		return status.Errorf(codes.AlreadyExists, "node %d, addr:%s", nodeID, url)
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

func (mr *RaftMetadataRepository) RemovePeer(ctx context.Context, _ types.ClusterID, nodeID types.NodeID) error {
	if nodeID == types.InvalidNodeID {
		return status.Error(codes.InvalidArgument, "invalid node id")
	}

	if !mr.membership.IsMember(nodeID) &&
		!mr.membership.IsLearner(nodeID) {
		return status.Errorf(codes.NotFound, "node %d", nodeID)
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

	mr.propose(ctx, r, true) //nolint:errcheck,revive // TODO:: Handle an error returned.
}

func (mr *RaftMetadataRepository) GetClusterInfo(context.Context, types.ClusterID) (*mrpb.ClusterInfo, error) {
	if !mr.IsMember() {
		return nil, status.Errorf(codes.Unavailable, "this mr is not member")
	}

	peerMap := mr.membership.GetPeers()

	clusterInfo := &mrpb.ClusterInfo{
		ClusterID:         mr.clusterID,
		NodeID:            mr.nodeID,
		Leader:            mr.membership.Leader(),
		ReplicationFactor: int32(mr.replicationFactor),
		AppliedIndex:      peerMap.AppliedIndex,
	}

	if len(peerMap.Peers) > 0 {
		clusterInfo.Members = make(map[types.NodeID]*mrpb.ClusterInfo_Member)

		for nodeID, peer := range peerMap.Peers {
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
	return mr.nrReport.Load()
}

func (mr *RaftMetadataRepository) GetLastCommitVersion() types.Version {
	return mr.storage.GetLastCommitResults().GetVersion()
}

func (mr *RaftMetadataRepository) GetOldestCommitVersion() types.Version {
	return mr.storage.GetFirstCommitResults().GetVersion()
}

func (mr *RaftMetadataRepository) IsMember() bool {
	return mr.hasLeader() && mr.storage.IsMember(mr.nodeID)
}

func (mr *RaftMetadataRepository) IsLearner() bool {
	return mr.hasLeader() && mr.storage.IsLearner(mr.nodeID)
}

func (mr *RaftMetadataRepository) ProposeReport(snID types.StorageNodeID, ur []snpb.LogStreamUncommitReport) error {
	return mr.proposeReport(snID, ur)
}

func (mr *RaftMetadataRepository) GetReporterClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return mr.reporterClientFac.GetReporterClient(ctx, sn)
}

func (mr *RaftMetadataRepository) GetLastCommitResults() *mrpb.LogStreamCommitResults {
	return mr.storage.GetLastCommitResults()
}

func (mr *RaftMetadataRepository) LookupNextCommitResults(ver types.Version) (*mrpb.LogStreamCommitResults, error) {
	return mr.storage.LookupNextCommitResults(ver)
}

type handler func(ctx context.Context) (interface{}, error)

func (mr *RaftMetadataRepository) withTelemetry(ctx context.Context, name string, h handler) (interface{}, error) {
	st := time.Now()
	rsp, err := h(ctx)
	mr.tmStub.mb.Records(name).Record(ctx, float64(time.Since(st).Nanoseconds())/float64(time.Millisecond))
	return rsp, err
}
