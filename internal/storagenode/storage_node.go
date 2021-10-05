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
	"github.com/kakao/varlog/internal/storagenode/executorsmap"
	"github.com/kakao/varlog/internal/storagenode/id"
	"github.com/kakao/varlog/internal/storagenode/logio"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/internal/storagenode/replication"
	"github.com/kakao/varlog/internal/storagenode/reportcommitter"
	"github.com/kakao/varlog/internal/storagenode/rpcserver"
	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/internal/storagenode/timestamper"
	"github.com/kakao/varlog/internal/storagenode/volume"
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

// StorageNode is a struct representing data and methods to run the storage node.
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

	executors             *executorsmap.ExecutorsMap
	estimatedNumExecutors int64

	storageNodePaths set.Set

	tsp timestamper.Timestamper

	tmStub *telemetry.Stub
}

var _ id.StorageNodeIDGetter = (*StorageNode)(nil)
var _ id.ClusterIDGetter = (*StorageNode)(nil)
var _ replication.Getter = (*StorageNode)(nil)
var _ reportcommitter.Getter = (*StorageNode)(nil)
var _ logio.Getter = (*StorageNode)(nil)
var _ fmt.Stringer = (*StorageNode)(nil)
var _ telemetry.Measurable = (*StorageNode)(nil)

// New creates a StorageNode configured by the opts.
// TODO: The argument ctx can be removed.
func New(ctx context.Context, opts ...Option) (*StorageNode, error) {
	const hintNumExecutors = 32

	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	sn := &StorageNode{
		config:           *cfg,
		storageNodePaths: set.New(len(cfg.volumes)),
		tsp:              timestamper.New(),
		executors:        executorsmap.New(hintNumExecutors),
	}
	sn.pprofServer = pprof.New(sn.pprofOpts...)

	if len(sn.telemetryEndpoint) == 0 {
		sn.tmStub = telemetry.NewNopTelmetryStub()
	} else {
		sn.tmStub, err = telemetry.NewTelemetryStub(ctx, "otel", sn.storageNodeID, sn.telemetryEndpoint)
		if err != nil {
			return nil, err
		}
	}

	sn.logger = sn.logger.Named("storagenode").With(
		zap.Uint32("cid", uint32(sn.clusterID)),
		zap.Int32("snid", int32(sn.storageNodeID)),
	)

	for v := range sn.volumes {
		vol, ok := v.(volume.Volume)
		if !ok {
			continue
		}
		snPath, err := volume.CreateStorageNodePath(vol, sn.clusterID, sn.storageNodeID)
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
		vol, ok := v.(volume.Volume)
		if !ok {
			continue
		}
		paths := vol.ReadLogStreamPaths(sn.clusterID, sn.storageNodeID)
		for _, path := range paths {
			if logStreamPaths.Contains(path) {
				return nil, errors.Errorf("storagenode: duplicated log stream path (%s)", path)
			}
			logStreamPaths.Add(path)
		}
	}

	for logStreamPathIf := range logStreamPaths {
		logStreamPath, ok := logStreamPathIf.(string)
		if !ok {
			continue
		}
		_, _, _, topicID, logStreamID, err := volume.ParseLogStreamPath(logStreamPath)
		if err != nil {
			return nil, err
		}

		strg, err := sn.createStorage(logStreamPath)
		if err != nil {
			return nil, err
		}
		if err := sn.startLogStream(topicID, logStreamID, strg); err != nil {
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
		newServer(withStorageNode(sn)),
	)

	return sn, nil
}

// Run starts storage node.
// Run returns when the storage node fails with fatal errors.
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

	sn.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	sn.stopper.mu.Unlock()
	sn.logger.Info("start")
	return sn.servers.Wait()
}

// Close closes the storage node.
func (sn *StorageNode) Close() {
	sn.stopper.mu.Lock()
	defer sn.stopper.mu.Unlock()

	if sn.stopper.state == storageNodeClosed {
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

// ClusterID returns the ClusterID of the storage node.
func (sn *StorageNode) ClusterID() types.ClusterID {
	return sn.clusterID
}

// StorageNodeID returns the StorageNodeID of the storage node.
func (sn *StorageNode) StorageNodeID() types.StorageNodeID {
	return sn.storageNodeID
}

// GetMetadata returns metadata of the storage node and the log stream replicas.
func (sn *StorageNode) GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	_, span := sn.tmStub.StartSpan(ctx, "storagenode.GetMetadata")
	defer span.End()

	sn.muAddr.RLock()
	addr := sn.advertiseAddress
	sn.muAddr.RUnlock()

	snmeta := &varlogpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.clusterID,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: sn.storageNodeID,
				Address:       addr,
			},
			Status: varlogpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping,
		},
		CreatedTime: sn.tsp.Created(),
		UpdatedTime: sn.tsp.LastUpdated(),
	}
	for snPathIf := range sn.storageNodePaths {
		snPath, ok := snPathIf.(string)
		if !ok {
			continue
		}
		snmeta.StorageNode.Storages = append(snmeta.StorageNode.Storages, &varlogpb.StorageDescriptor{
			Path:  snPath,
			Used:  0,
			Total: 0,
		})
	}

	snmeta.LogStreams = sn.logStreamMetadataDescriptors()
	return snmeta, nil
}

func (sn *StorageNode) logStreamMetadataDescriptors() []varlogpb.LogStreamMetadataDescriptor {
	lsmetas := make([]varlogpb.LogStreamMetadataDescriptor, 0, sn.estimatedNumberOfExecutors())
	sn.forEachExecutors(func(_ types.LogStreamID, extor executor.Executor) {
		lsmetas = append(lsmetas, extor.Metadata())
	})
	return lsmetas
}

// AddLogStream implements the Server AddLogStream method.
func (sn *StorageNode) AddLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, storageNodePath string) (string, error) {
	logStreamPath, err := sn.addLogStream(topicID, logStreamID, storageNodePath)
	if err != nil {
		return "", err
	}

	atomic.AddInt64(&sn.estimatedNumExecutors, 1)
	return logStreamPath, nil
}

func (sn *StorageNode) addLogStream(topicID types.TopicID, logStreamID types.LogStreamID, storageNodePath string) (lsPath string, err error) {
	if !sn.storageNodePaths.Contains(storageNodePath) {
		return "", errors.New("storagenode: no such storage path")
	}

	lsPath, err = volume.CreateLogStreamPath(storageNodePath, topicID, logStreamID)
	if err != nil {
		return "", err
	}

	_, loaded := sn.executors.LoadOrStore(topicID, logStreamID, executor.Executor(nil))
	if loaded {
		return "", errors.New("storagenode: log stream already exists")
	}

	defer func() {
		if err != nil {
			_, _ = sn.executors.LoadAndDelete(topicID, logStreamID)
		}
	}()

	strg, err := sn.createStorage(lsPath)
	if err != nil {
		return "", err
	}
	err = sn.startLogStream(topicID, logStreamID, strg)
	if err != nil {
		return "", err
	}
	return lsPath, nil
}

func (sn *StorageNode) createStorage(logStreamPath string) (storage.Storage, error) {
	return storage.NewStorage(append(
		sn.storageOpts,
		storage.WithPath(logStreamPath),
		storage.WithLogger(sn.logger),
	)...)
}

func (sn *StorageNode) startLogStream(topicID types.TopicID, logStreamID types.LogStreamID, storage storage.Storage) (err error) {
	lse, err := executor.New(append(sn.executorOpts,
		executor.WithStorage(storage),
		executor.WithStorageNodeID(sn.storageNodeID),
		executor.WithTopicID(topicID),
		executor.WithLogStreamID(logStreamID),
		executor.WithMeasurable(sn),
		executor.WithLogger(sn.logger),
	)...)
	if err != nil {
		return err
	}
	sn.tsp.Touch()
	return sn.executors.Store(topicID, logStreamID, lse)
}

func (sn *StorageNode) removeLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error {
	lse, loaded := sn.executors.LoadAndDelete(topicID, logStreamID)
	if !loaded {
		return verrors.ErrNotExist
	}

	lse.Close()

	// TODO (jun): Is removing data path optional or default behavior?
	if err := os.RemoveAll(lse.Path()); err != nil {
		sn.logger.Warn("error while removing log stream path")
	}
	sn.tsp.Touch()

	atomic.AddInt64(&sn.estimatedNumExecutors, -1)
	return nil
}

func (sn *StorageNode) lookupExecutor(topicID types.TopicID, logStreamID types.LogStreamID) (executor.Executor, error) {
	extor, ok := sn.executors.Load(topicID, logStreamID)
	if !ok {
		return nil, errors.WithStack(errNoLogStream)
	}
	if extor == nil {
		return nil, errors.WithStack(errNotReadyLogStream)
	}
	return extor, nil
}

// Seal implements the Server Seal method.
func (sn *StorageNode) Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	lse, err := sn.lookupExecutor(topicID, logStreamID)
	if err != nil {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, err
	}

	// TODO: check err
	status, hwm, _ := lse.Seal(ctx, lastCommittedGLSN)

	sn.tsp.Touch()
	return status, hwm, nil
}

func (sn *StorageNode) unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, replicas []varlogpb.Replica) error {
	lse, err := sn.lookupExecutor(topicID, logStreamID)
	if err != nil {
		return err
	}

	sn.tsp.Touch()
	return lse.Unseal(ctx, replicas)
}

func (sn *StorageNode) sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, replica varlogpb.Replica) (*snpb.SyncStatus, error) {
	lse, err := sn.lookupExecutor(topicID, logStreamID)
	if err != nil {
		return nil, err
	}

	sts, err := lse.Sync(ctx, replica)
	return sts, err
}

func (sn *StorageNode) getPrevCommitInfo(ctx context.Context, version types.Version) (infos []*snpb.LogStreamCommitInfo, err error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	infos = make([]*snpb.LogStreamCommitInfo, 0, sn.estimatedNumberOfExecutors())

	sn.forEachExecutors(func(_ types.LogStreamID, extor executor.Executor) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, cerr := extor.GetPrevCommitInfo(version)
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

// ReportCommitter implements `internal/storagenode/reportcommitter.Getter.ReportCommitter`.
func (sn *StorageNode) ReportCommitter(topicID types.TopicID, logStreamID types.LogStreamID) (reportcommitter.ReportCommitter, bool) {
	extor, err := sn.lookupExecutor(topicID, logStreamID)
	if err != nil {
		return nil, false
	}
	return extor, true
}

func (sn *StorageNode) forEachExecutors(f func(types.LogStreamID, executor.Executor)) {
	sn.executors.Range(func(lsid types.LogStreamID, extor executor.Executor) bool {
		// NOTE: if value interface is nil, the executor is not ready.
		if extor != nil {
			f(lsid, extor)
		}
		return true
	})
}

func (sn *StorageNode) estimatedNumberOfExecutors() int {
	return int(atomic.LoadInt64(&sn.estimatedNumExecutors))
}

// GetReports implements `internal/storagenode/reportcommitter.Getter.GetReports`.
func (sn *StorageNode) GetReports(rsp *snpb.GetReportResponse, f func(reportcommitter.ReportCommitter, *snpb.GetReportResponse)) {
	sn.forEachExecutors(func(_ types.LogStreamID, lse executor.Executor) {
		f(lse, rsp)
	})
}

// Replicator implements `internal/storagenode/replication.Getter.Replicator`.
func (sn *StorageNode) Replicator(topicID types.TopicID, logStreamID types.LogStreamID) (replication.Replicator, bool) {
	extor, err := sn.lookupExecutor(topicID, logStreamID)
	if err != nil {
		return nil, false
	}
	return extor, true
}

// ReadWriter implements `internal/storagenode/logio.Getter.ReadWriter`.
func (sn *StorageNode) ReadWriter(topicID types.TopicID, logStreamID types.LogStreamID) (logio.ReadWriter, bool) {
	extor, err := sn.lookupExecutor(topicID, logStreamID)
	if err != nil {
		return nil, false
	}
	return extor, true
}

// ForEachReadWriters implements `internal/storagenode/logio.Getter.ForEachReadWriters`.
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

// Stub implements `internal/storagenode/telemetry.Measurable.Stub`.
func (sn *StorageNode) Stub() *telemetry.Stub {
	return sn.tmStub
}
