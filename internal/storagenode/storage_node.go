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
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
	"go.uber.org/multierr"
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
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/internal/storagenode/timestamper"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/util/netutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

var (
	errNoLogStream       = stderrors.New("storagenode: no such log stream")
	errNotReadyLogStream = stderrors.New("storagenode: not ready log stream")
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

	executors             *sync.Map // map[types.LogStreamID]executor.Executor
	estimatedNumExecutors int64

	storageNodePaths set.Set

	tsp timestamper.Timestamper

	tmStub *telemetry.TelemetryStub
}

var _ id.StorageNodeIDGetter = (*StorageNode)(nil)
var _ id.ClusterIDGetter = (*StorageNode)(nil)
var _ replication.Getter = (*StorageNode)(nil)
var _ reportcommitter.Getter = (*StorageNode)(nil)
var _ logio.Getter = (*StorageNode)(nil)
var _ fmt.Stringer = (*StorageNode)(nil)
var _ telemetry.Measurable = (*StorageNode)(nil)

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
	sn.executors = new(sync.Map)
	sn.pprofServer = pprof.New(sn.pprofOpts...)

	if len(sn.telemetryEndpoint) == 0 {
		sn.tmStub = telemetry.NewNopTelmetryStub()
	} else {
		sn.tmStub, err = telemetry.NewTelemetryStub(ctx, "otel", sn.snid, sn.telemetryEndpoint)
		if err != nil {
			return nil, err
		}
	}

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
		reportcommitter.NewServer(sn.lsr, sn),
		logio.NewServer(
			logio.WithStorageNodeIDGetter(sn),
			logio.WithReadWriterGetter(sn),
			logio.WithMeasurable(sn),
			logio.WithLogger(sn.logger),
		),
		replication.NewServer(
			replication.WithStorageNodeIDGetter(sn),
			replication.WithLogReplicatorGetter(sn),
			replication.WithPipelineQueueSize(1),
			replication.WithMeasurable(sn),
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
	sn.forEachExecutors(func(_ types.LogStreamID, extor executor.Executor) {
		extor.Close()
	})

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
	lsmetas := make([]varlogpb.LogStreamMetadataDescriptor, 0, sn.estimatedNumberOfExecutors())
	sn.forEachExecutors(func(_ types.LogStreamID, extor executor.Executor) {
		lsmetas = append(lsmetas, extor.Metadata())
	})
	return lsmetas
}

// AddLogStream implements the Server AddLogStream method.
func (sn *StorageNode) AddLogStream(ctx context.Context, logStreamID types.LogStreamID, storageNodePath string) (string, error) {
	logStreamPath, err := sn.addLogStream(ctx, logStreamID, storageNodePath)
	if err != nil {
		return "", err
	}

	atomic.AddInt64(&sn.estimatedNumExecutors, 1)
	return logStreamPath, nil
}

func (sn *StorageNode) addLogStream(ctx context.Context, logStreamID types.LogStreamID, storageNodePath string) (lsPath string, err error) {
	if !sn.storageNodePaths.Contains(storageNodePath) {
		return "", errors.New("storagenode: no such storage path")
	}

	lsPath, err = CreateLogStreamPath(storageNodePath, logStreamID)
	if err != nil {
		return "", err
	}

	_, loaded := sn.executors.LoadOrStore(logStreamID, executor.Executor(nil))
	if loaded {
		return "", errors.New("storagenode: log stream already exists")
	}

	defer func() {
		if err != nil {
			sn.executors.Delete(logStreamID)
		}
	}()

	strg, err := sn.createStorage(ctx, lsPath)
	if err != nil {
		return "", err
	}
	err = sn.startLogStream(ctx, logStreamID, strg)
	if err != nil {
		return "", err
	}
	return lsPath, nil
}

func (sn *StorageNode) createStorage(ctx context.Context, logStreamPath string) (storage.Storage, error) {
	opts := append(sn.storageOpts, storage.WithPath(logStreamPath), storage.WithLogger(sn.logger))
	return storage.NewStorage(opts...)
}

func (sn *StorageNode) startLogStream(ctx context.Context, logStreamID types.LogStreamID, storage storage.Storage) (err error) {
	opts := append(sn.executorOpts,
		executor.WithStorage(storage),
		executor.WithStorageNodeID(sn.snid),
		executor.WithLogStreamID(logStreamID),
		executor.WithMeasurable(sn),
		executor.WithLogger(sn.logger),
	)
	lse, err := executor.New(opts...)
	if err != nil {
		return err
	}
	sn.tsp.Touch()
	sn.executors.Store(logStreamID, lse)
	return nil
}

// RemoveLogStream implements the Server RemoveLogStream method.
func (sn *StorageNode) RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	ifaceExecutor, loaded := sn.executors.LoadAndDelete(logStreamID)
	if !loaded {
		return verrors.ErrNotExist
	}

	lse := ifaceExecutor.(executor.Executor)

	lse.Close()

	// TODO (jun): Is removing data path optional or default behavior?
	if err := os.RemoveAll(lse.Path()); err != nil {
		sn.logger.Warn("error while removing log stream path")
	}
	sn.tsp.Touch()

	atomic.AddInt64(&sn.estimatedNumExecutors, -1)
	return nil
}

func (sn *StorageNode) lookupExecutor(logStreamID types.LogStreamID) (executor.Executor, error) {
	ifaceExecutor, ok := sn.executors.Load(logStreamID)
	if !ok {
		return nil, errors.WithStack(errNoLogStream)
	}
	if ifaceExecutor == nil {
		return nil, errors.WithStack(errNotReadyLogStream)
	}
	return ifaceExecutor.(executor.Executor), nil
}

// Seal implements the Server Seal method.
func (sn *StorageNode) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	lse, err := sn.lookupExecutor(logStreamID)
	if err != nil {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, err
	}

	// TODO: check err
	status, hwm, _ := lse.Seal(ctx, lastCommittedGLSN)

	sn.tsp.Touch()
	return status, hwm, nil
}

// Unseal implements the Server Unseal method.
func (sn *StorageNode) Unseal(ctx context.Context, logStreamID types.LogStreamID, replicas []snpb.Replica) error {
	lse, err := sn.lookupExecutor(logStreamID)
	if err != nil {
		return err
	}

	sn.tsp.Touch()
	return lse.Unseal(ctx, replicas)
}

func (sn *StorageNode) Sync(ctx context.Context, logStreamID types.LogStreamID, replica snpb.Replica) (*snpb.SyncStatus, error) {
	lse, err := sn.lookupExecutor(logStreamID)
	if err != nil {
		return nil, err
	}

	sts, err := lse.Sync(ctx, replica)
	return sts, err
}

func (sn *StorageNode) GetPrevCommitInfo(ctx context.Context, hwm types.GLSN) (infos []*snpb.LogStreamCommitInfo, err error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	infos = make([]*snpb.LogStreamCommitInfo, 0, sn.estimatedNumberOfExecutors())

	sn.forEachExecutors(func(_ types.LogStreamID, extor executor.Executor) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, cerr := extor.GetPrevCommitInfo(hwm)
			mu.Lock()
			infos = append(infos, info)
			err = multierr.Append(err, cerr)
			defer mu.Unlock()
		}()
	})
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return infos, nil
}

func (sn *StorageNode) Executors() *sync.Map {
	return sn.executors
}

func (sn *StorageNode) verifyClusterID(cid types.ClusterID) bool {
	return sn.cid == cid
}

func (sn *StorageNode) verifyStorageNodeID(snid types.StorageNodeID) bool {
	return sn.snid == snid
}

func (sn *StorageNode) ReportCommitter(logStreamID types.LogStreamID) (reportcommitter.ReportCommitter, bool) {
	extor, err := sn.lookupExecutor(logStreamID)
	if err != nil {
		return nil, false
	}
	return extor, true
}

func (sn *StorageNode) forEachExecutors(f func(types.LogStreamID, executor.Executor)) {
	sn.executors.Range(func(k, v interface{}) bool {
		// NOTE: if value interface is nil, the executor is not ready.
		if v == nil {
			return true
		}
		extor := v.(executor.Executor)
		f(k.(types.LogStreamID), extor)
		return true
	})
}

func (sn *StorageNode) estimatedNumberOfExecutors() int {
	return int(atomic.LoadInt64(&sn.estimatedNumExecutors))
}

func (sn *StorageNode) GetReports(rsp *snpb.GetReportResponse, f func(reportcommitter.ReportCommitter, *snpb.GetReportResponse)) {
	sn.forEachExecutors(func(_ types.LogStreamID, lse executor.Executor) {
		f(lse, rsp)
	})
}

func (sn *StorageNode) Replicator(logStreamID types.LogStreamID) (replication.Replicator, bool) {
	extor, err := sn.lookupExecutor(logStreamID)
	if err != nil {
		return nil, false
	}
	return extor, true
}

func (sn *StorageNode) ReadWriter(logStreamID types.LogStreamID) (logio.ReadWriter, bool) {
	extor, err := sn.lookupExecutor(logStreamID)
	if err != nil {
		return nil, false
	}
	return extor, true
}

func (sn *StorageNode) ForEachReadWriters(f func(logio.ReadWriter)) {
	sn.forEachExecutors(func(_ types.LogStreamID, extor executor.Executor) {
		f(extor)
	})
}

func (sn *StorageNode) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "storagenode (cid=%d, snid=%d)", sn.ClusterID(), sn.StorageNodeID())
	return sb.String()
}

func (sn *StorageNode) Stub() *telemetry.TelemetryStub {
	return sn.tmStub
}
