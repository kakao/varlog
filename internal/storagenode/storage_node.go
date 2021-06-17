package storagenode

import (
	"context"
	stderrors "errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/storagenode/executor"
	"github.com/kakao/varlog/internal/storagenode/id"
	"github.com/kakao/varlog/internal/storagenode/logio"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/internal/storagenode/replication"
	"github.com/kakao/varlog/internal/storagenode/reportcommitter"
	"github.com/kakao/varlog/internal/storagenode/rpcserver"
	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/internal/storagenode/timestamper"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/util/netutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

var (
	errNoLogStream = stderrors.New("storagenode: no such log stream")
)

type storageNodeState int

const (
	storageNodeInit storageNodeState = iota
	storageNodeRunning
	storageNodeClosed
)

type StorageNode struct {
	config
	muAddr sync.RWMutex

	listener     net.Listener
	rpcServer    *grpc.Server
	healthServer *health.Server
	pprofServer  pprof.Server
	servers      errgroup.Group

	stopper struct {
		state storageNodeState
		mu    sync.RWMutex
	}

	lsr reportcommitter.Reporter

	executors struct {
		es map[types.LogStreamID]executor.Executor
		mu sync.RWMutex
	}

	storageNodePaths set.Set

	// options *Options
	tsp timestamper.Timestamper
}

var _ id.StorageNodeIDGetter = (*StorageNode)(nil)
var _ id.ClusterIDGetter = (*StorageNode)(nil)
var _ replication.Getter = (*StorageNode)(nil)
var _ reportcommitter.Getter = (*StorageNode)(nil)
var _ logio.Getter = (*StorageNode)(nil)
var _ fmt.Stringer = (*StorageNode)(nil)

func New(ctx context.Context, opts ...Option) (*StorageNode, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	sn := &StorageNode{
		config:           *cfg,
		storageNodePaths: set.New(len(cfg.volumes)),
		tsp:              timestamper.New(),
	}
	sn.executors.es = make(map[types.LogStreamID]executor.Executor)
	sn.pprofServer = pprof.New(sn.pprofOpts...)

	/*
		tmStub, err := newTelemetryStub(ctx, options.TelemetryOptions.CollectorName, options.StorageNodeID, options.TelemetryOptions.CollectorEndpoint)
		if err != nil {
			return nil, err
		}
		sn := &StorageNode{
			clusterID:        options.ClusterID,
			storageNodeID:    options.StorageNodeID,
			lseMap:           make(map[types.LogStreamID]Executor),
			tst:              NewTimestamper(),
			storageNodePaths: set.New(len(options.Volumes)),
			options:          options,
			logger:           options.Logger,
			sw:               stopwaiter.New(),
			tmStub:           tmStub,
			pprofServer:      newPprofServer(options.PProfServerConfig),
		}
	*/
	sn.logger = sn.logger.Named("storagenode").With(
		zap.Uint32("cid", uint32(sn.cid)),
		zap.Uint32("snid", uint32(sn.snid)),
	)

	for v := range sn.volumes {
		vol := v.(Volume)
		snPath, err := vol.CreateStorageNodePath(sn.cid, sn.snid)
		if err != nil {
			return nil, err
		}
		sn.storageNodePaths.Add(snPath)
	}
	if len(sn.storageNodePaths) == 0 {
		return nil, errors.New("storagenode: no valid storage node path")
	}

	lsrOpts := []reportcommitter.Option{
		reportcommitter.WithStorageNodeIDGetter(sn),
		reportcommitter.WithReportCommitterGetter(sn),
		// TODO: WithLogger
	}
	sn.lsr = reportcommitter.New(lsrOpts...)
	sn.rpcServer = grpc.NewServer()
	sn.healthServer = health.NewServer()

	// Listener
	lis, err := net.Listen("tcp", sn.listenAddress)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sn.listener = lis
	if sn.advertiseAddress == "" {
		addrs, _ := netutil.GetListenerAddrs(lis.Addr())
		sn.muAddr.Lock()
		sn.advertiseAddress = addrs[0]
		sn.muAddr.Unlock()
	}

	// log stream path
	logStreamPaths := set.New(0)
	for v := range sn.volumes {
		vol := v.(Volume)
		paths := vol.ReadLogStreamPaths(sn.cid, sn.snid)
		for _, path := range paths {
			if logStreamPaths.Contains(path) {
				return nil, errors.Errorf("storagenode: duplicated log stream path (%s)", path)
			}
			logStreamPaths.Add(path)
		}
	}

	for logStreamPathIf := range logStreamPaths {
		logStreamPath := logStreamPathIf.(string)
		_, _, _, logStreamID, err := ParseLogStreamPath(logStreamPath)
		if err != nil {
			return nil, err
		}

		strg, err := sn.createStorage(context.Background(), logStreamPath)
		if err != nil {
			return nil, err
		}
		if err := sn.startLogStream(context.Background(), logStreamID, strg); err != nil {
			return nil, err
		}
	}

	// services
	grpc_health_v1.RegisterHealthServer(sn.rpcServer, sn.healthServer)
	rpcserver.RegisterRPCServer(sn.rpcServer,
		reportcommitter.NewServer(sn.lsr),
		logio.NewServer(
			logio.WithStorageNodeIDGetter(sn),
			logio.WithReadWriterGetter(sn),
		),
		replication.NewServer(
			replication.WithStorageNodeIDGetter(sn),
			replication.WithLogReplicatorGetter(sn),
			replication.WithPipelineQueueSize(1),
			replication.WithLogger(sn.logger),
		),
		NewServer(WithStorageNode(sn)),
	)

	return sn, nil
}

func (sn *StorageNode) Run() error {
	sn.stopper.mu.Lock()
	switch sn.stopper.state {
	case storageNodeRunning:
		sn.stopper.mu.Unlock()
		return nil
	case storageNodeClosed:
		sn.stopper.mu.Unlock()
		return errors.WithStack(verrors.ErrClosed)
	case storageNodeInit:
		sn.stopper.state = storageNodeRunning
	}

	sn.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// mux
	mux := cmux.New(sn.listener)
	httpL := mux.Match(cmux.HTTP1Fast())
	grpcL := mux.Match(cmux.Any())

	// RPC Server
	sn.servers.Go(func() error {
		return errors.WithStack(sn.rpcServer.Serve(grpcL))
	})
	sn.servers.Go(func() error {
		if err := sn.pprofServer.Run(httpL); !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, cmux.ErrListenerClosed) {
			return err
		}
		return nil
	})
	sn.servers.Go(func() error {
		if err := mux.Serve(); err != nil && !strings.Contains(err.Error(), "use of closed") {
			return errors.WithStack(err)
		}
		return nil
	})

	sn.stopper.mu.Unlock()
	sn.logger.Info("start")
	return sn.servers.Wait()
}

func (sn *StorageNode) Close() {
	sn.stopper.mu.Lock()
	defer sn.stopper.mu.Unlock()

	switch sn.stopper.state {
	case storageNodeClosed:
		return
	}
	sn.stopper.state = storageNodeClosed

	sn.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Reporter
	sn.lsr.Close()

	// LogStreamExecutors
	for _, lse := range sn.logStreamExecutors() {
		lse.Close()
	}

	// RPC Server
	// FIXME (jun): Use GracefulStop
	sn.rpcServer.Stop()

	// TODO: set close timeout
	// sn.tmStub.close(context.TODO())

	// TODO: Use close timeout
	sn.pprofServer.Close(context.TODO())
	sn.logger.Info("stop")
}

func (sn *StorageNode) ClusterID() types.ClusterID {
	return sn.cid
}

func (sn *StorageNode) StorageNodeID() types.StorageNodeID {
	return sn.snid
}

// GetMeGetMetadata implements the Server GetMetadata method.
func (sn *StorageNode) GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	ctx, span := sn.tmStub.StartSpan(ctx, "storagenode.GetMetadata")
	defer span.End()

	sn.muAddr.RLock()
	addr := sn.advertiseAddress
	sn.muAddr.RUnlock()

	snmeta := &varlogpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.cid,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNodeID: sn.snid,
			Address:       addr,
			Status:        varlogpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping,
		},
		CreatedTime: sn.tsp.Created(),
		UpdatedTime: sn.tsp.LastUpdated(),
	}
	for snPathIf := range sn.storageNodePaths {
		snPath := snPathIf.(string)
		snmeta.StorageNode.Storages = append(snmeta.StorageNode.Storages, &varlogpb.StorageDescriptor{
			Path:  snPath,
			Used:  0,
			Total: 0,
		})
	}

	snmeta.LogStreams = sn.logStreamMetadataDescriptors(ctx)
	return snmeta, nil
}

func (sn *StorageNode) logStreamMetadataDescriptors(ctx context.Context) []varlogpb.LogStreamMetadataDescriptor {
	ctx, span := sn.tmStub.StartSpan(ctx, "storagenode.logStreamMetadataDescriptors")
	defer span.End()

	tick := time.Now()
	lseList := sn.logStreamExecutors()
	span.SetAttributes(attribute.Int64("get_log_stream_executors_ms", time.Since(tick).Milliseconds()))

	if len(lseList) == 0 {
		return nil
	}
	lsmetas := make([]varlogpb.LogStreamMetadataDescriptor, 0, len(lseList))
	for _, lse := range lseList {
		lsmetas = append(lsmetas, lse.Metadata())
	}
	return lsmetas
}

// AddLogStream implements the Server AddLogStream method.
func (sn *StorageNode) AddLogStream(ctx context.Context, logStreamID types.LogStreamID, storageNodePath string) (string, error) {
	logStreamPath, err := sn.addLogStream(ctx, logStreamID, storageNodePath)
	if err != nil {
		return "", err
	}

	return logStreamPath, nil
}

func (sn *StorageNode) addLogStream(ctx context.Context, logStreamID types.LogStreamID, storageNodePath string) (string, error) {
	if !sn.storageNodePaths.Contains(storageNodePath) {
		return "", errors.New("storagenode: no such storage path")
	}

	lsPath, err := CreateLogStreamPath(storageNodePath, logStreamID)
	if err != nil {
		return "", err
	}

	sn.executors.mu.Lock()
	defer sn.executors.mu.Unlock()

	if _, ok := sn.executors.es[logStreamID]; ok {
		return "", errors.New("storagenode: log stream already exists")
	}

	strg, err := sn.createStorage(ctx, lsPath)
	if err != nil {
		return "", err
	}

	if err := sn.startLogStream(ctx, logStreamID, strg); err != nil {
		return "", err
	}
	return lsPath, nil
}

func (sn *StorageNode) createStorage(ctx context.Context, logStreamPath string) (storage.Storage, error) {
	ctx, span := sn.tmStub.StartSpan(ctx, "storagenode/StorageNode.createStorage")
	defer span.End()

	opts := append(sn.storageOpts, storage.WithPath(logStreamPath), storage.WithLogger(sn.logger))
	strg, err := storage.NewStorage(opts...)
	if err != nil {
		span.RecordError(err)
	}
	return strg, err
}

func (sn *StorageNode) startLogStream(ctx context.Context, logStreamID types.LogStreamID, storage storage.Storage) (err error) {
	ctx, span := sn.tmStub.StartSpan(ctx, "storagenode/StorageNode.startLogStream")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	opts := append(sn.executorOpts,
		executor.WithStorage(storage),
		executor.WithStorageNodeID(sn.snid),
		executor.WithLogStreamID(logStreamID),
		executor.WithLogger(sn.logger),
	)
	lse, err := executor.New(opts...)
	if err != nil {
		return err
	}
	sn.tsp.Touch()
	sn.executors.es[logStreamID] = lse
	return nil
}

// RemoveLogStream implements the Server RemoveLogStream method.
func (sn *StorageNode) RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	ctx, span := sn.tmStub.StartSpan(ctx, "storagenode/StorageNode.RemoveLogStream")
	defer span.End()

	sn.executors.mu.Lock()
	defer sn.executors.mu.Unlock()

	lse, ok := sn.executors.es[logStreamID]
	if !ok {
		return verrors.ErrNotExist
	}
	delete(sn.executors.es, logStreamID)

	tick := time.Now()
	lse.Close()
	closeDuration := time.Since(tick)

	tick = time.Now()
	// TODO (jun): Is removing data path optional or default behavior?
	if err := os.RemoveAll(lse.Path()); err != nil {
		sn.logger.Warn("error while removing log stream path")
	}
	removeDuration := time.Since(tick)
	sn.tsp.Touch()

	span.SetAttributes(attribute.Int64("close_duration_ms", closeDuration.Milliseconds()),
		attribute.Int64("remove_duration_ms", removeDuration.Milliseconds()))
	return nil
}

// Seal implements the Server Seal method.
func (sn *StorageNode) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	lse, ok := sn.logStreamExecutor(logStreamID)
	if !ok {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, errors.WithStack(errNoLogStream)
	}
	// TODO: check err
	status, hwm, _ := lse.Seal(ctx, lastCommittedGLSN)

	sn.tsp.Touch()
	return status, hwm, nil
}

// Unseal implements the Server Unseal method.
func (sn *StorageNode) Unseal(ctx context.Context, logStreamID types.LogStreamID, replicas []snpb.Replica) error {
	lse, ok := sn.logStreamExecutor(logStreamID)
	if !ok {
		return errors.WithStack(errNoLogStream)
	}
	sn.tsp.Touch()
	return lse.Unseal(ctx, replicas)
}

func (sn *StorageNode) Sync(ctx context.Context, logStreamID types.LogStreamID, replica snpb.Replica, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	lse, ok := sn.logStreamExecutor(logStreamID)
	if !ok {
		return nil, errors.WithStack(errNoLogStream)
	}
	sts, err := lse.Sync(ctx, replica, lastGLSN)
	if err != nil {
		return nil, err
	}
	return &snpb.SyncStatus{
		State:   sts.State,
		First:   sts.First,
		Last:    sts.Last,
		Current: sts.Current,
	}, nil
}

func (sn *StorageNode) GetPrevCommitInfo(ctx context.Context, hwm types.GLSN) ([]*snpb.LogStreamCommitInfo, error) {
	es := sn.logStreamExecutors()
	infos := make([]*snpb.LogStreamCommitInfo, len(es))
	grp, ctx := errgroup.WithContext(ctx)
	for i := range es {
		idx := i
		grp.Go(func() (err error) {
			infos[idx], err = es[idx].GetPrevCommitInfo(hwm)
			return err
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	return infos, nil
}

func (sn *StorageNode) logStreamExecutor(logStreamID types.LogStreamID) (executor.Executor, bool) {
	sn.executors.mu.RLock()
	defer sn.executors.mu.RUnlock()
	lse, ok := sn.executors.es[logStreamID]
	return lse, ok
}

func (sn *StorageNode) logStreamExecutors() []executor.Executor {
	sn.executors.mu.RLock()
	defer sn.executors.mu.RUnlock()
	ret := make([]executor.Executor, 0, len(sn.executors.es))
	for _, lse := range sn.executors.es {
		ret = append(ret, lse)
	}
	return ret
}

func (sn *StorageNode) verifyClusterID(cid types.ClusterID) bool {
	return sn.cid == cid
}

func (sn *StorageNode) verifyStorageNodeID(snid types.StorageNodeID) bool {
	return sn.snid == snid
}

func (sn *StorageNode) ReportCommitter(logStreamID types.LogStreamID) (reportcommitter.ReportCommitter, bool) {
	return sn.logStreamExecutor(logStreamID)
}

func (sn *StorageNode) ReportCommitters() []reportcommitter.ReportCommitter {
	es := sn.logStreamExecutors()
	ret := make([]reportcommitter.ReportCommitter, 0, len(es))
	for _, e := range es {
		ret = append(ret, e)
	}
	return ret
}

func (sn *StorageNode) Replicator(logStreamID types.LogStreamID) (replication.Replicator, bool) {
	return sn.logStreamExecutor(logStreamID)
}

func (sn *StorageNode) Replicators() []replication.Replicator {
	es := sn.logStreamExecutors()
	ret := make([]replication.Replicator, 0, len(es))
	for _, e := range es {
		ret = append(ret, e)
	}
	return ret
}

func (sn *StorageNode) ReadWriter(logStreamID types.LogStreamID) (logio.ReadWriter, bool) {
	return sn.logStreamExecutor(logStreamID)
}

func (sn *StorageNode) ReadWriters() []logio.ReadWriter {
	es := sn.logStreamExecutors()
	ret := make([]logio.ReadWriter, 0, len(es))
	for _, e := range es {
		ret = append(ret, e)
	}
	return ret
}

func (sn *StorageNode) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "storagenode (cid=%d, snid=%d)", sn.ClusterID(), sn.StorageNodeID())
	return sb.String()
}
