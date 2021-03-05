package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode -package storagenode -destination storage_node_mock.go . Management,LogStreamExecutorGetter
import (
	"context"
	stderrors "errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/label"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/pkg/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner/stopwaiter"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

var (
	errNoLogStream = stderrors.New("storagenode: no such log stream")
)

// Management is the interface that wraps methods for managing StorageNode.
type Management interface {
	ClusterID() types.ClusterID

	StorageNodeID() types.StorageNodeID

	// GetMetadata returns metadata of StorageNode. The metadata contains
	// configurations and statistics for StorageNode.
	GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error)

	// AddLogStream adds a new LogStream to StorageNode.
	AddLogStream(ctx context.Context, logStreamID types.LogStreamID, storageNodePath string) (string, error)

	// RemoveLogStream removes a LogStream from StorageNode.
	RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error

	// Seal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusSealing or LogStreamStatusSealed.
	Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error)

	// Unseal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusRunning.
	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	Sync(ctx context.Context, logStreamID types.LogStreamID, replica Replica, lastGLSN types.GLSN) (*snpb.SyncStatus, error)
}

type LogStreamExecutorGetter interface {
	GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool)
	GetLogStreamExecutors() []LogStreamExecutor
}

type StorageNode struct {
	clusterID     types.ClusterID
	storageNodeID types.StorageNodeID

	server        *grpc.Server
	healthServer  *health.Server
	advertiseAddr string

	pprofServer *pprofServer

	running   bool
	muRunning sync.Mutex
	sw        *stopwaiter.StopWaiter

	lseMtx sync.RWMutex
	lseMap map[types.LogStreamID]LogStreamExecutor

	lsr LogStreamReporter

	storageNodePaths set.Set

	options *Options
	tst     Timestamper

	tmStub *telemetryStub

	logger *zap.Logger
}

func NewStorageNode(ctx context.Context, options *Options) (*StorageNode, error) {
	if err := options.Valid(); err != nil {
		return nil, err
	}

	tmStub, err := newTelemetryStub(ctx, options.TelemetryOptions.CollectorName, options.StorageNodeID, options.TelemetryOptions.CollectorEndpoint)
	if err != nil {
		return nil, err
	}
	sn := &StorageNode{
		clusterID:        options.ClusterID,
		storageNodeID:    options.StorageNodeID,
		lseMap:           make(map[types.LogStreamID]LogStreamExecutor),
		tst:              NewTimestamper(),
		storageNodePaths: set.New(len(options.Volumes)),
		options:          options,
		logger:           options.Logger,
		sw:               stopwaiter.New(),
		tmStub:           tmStub,
		pprofServer:      newPprofServer(options.PProfServerConfig),
	}
	if sn.logger == nil {
		sn.logger = zap.NewNop()
	}
	sn.logger = sn.logger.Named("storagenode").With(
		zap.Uint32("cid", uint32(sn.clusterID)),
		zap.Uint32("snid", uint32(sn.storageNodeID)),
	)

	for volume := range options.Volumes {
		snPath, err := volume.CreateStorageNodePath(sn.clusterID, sn.storageNodeID)
		if err != nil {
			return nil, err
		}
		sn.storageNodePaths.Add(snPath)
	}

	if len(sn.storageNodePaths) == 0 {
		return nil, errors.New("storagenode: no valid storage node path")
	}

	sn.lsr = NewLogStreamReporter(sn.logger, sn.storageNodeID, sn, sn.tmStub, &options.LogStreamReporterOptions)
	sn.server = grpc.NewServer()

	sn.healthServer = health.NewServer()
	grpc_health_v1.RegisterHealthServer(sn.server, sn.healthServer)

	NewLogStreamReporterService(sn.lsr, sn.tmStub, sn.logger).Register(sn.server)
	NewLogIOService(sn.storageNodeID, sn, sn.tmStub, sn.logger).Register(sn.server)
	NewManagementService(sn, sn.tmStub, sn.logger).Register(sn.server)
	NewReplicatorService(sn.storageNodeID, sn, sn.tmStub, sn.logger).Register(sn.server)

	return sn, nil
}

func (sn *StorageNode) Run() error {
	sn.muRunning.Lock()
	defer sn.muRunning.Unlock()

	if sn.running {
		return nil
	}
	sn.running = true

	// LogStreamReporter
	if err := sn.lsr.Run(context.Background()); err != nil {
		return err
	}

	// Listener
	lis, err := net.Listen("tcp", sn.options.ListenAddress)
	if err != nil {
		return errors.Wrapf(err, "storagenode")
	}
	sn.advertiseAddr = sn.options.AdvertiseAddress
	if sn.advertiseAddr == "" {
		addrs, _ := netutil.GetListenerAddrs(lis.Addr())
		sn.advertiseAddr = addrs[0]
	}

	// mux
	mux := cmux.New(lis)
	httpL := mux.Match(cmux.HTTP1Fast())
	grpcL := mux.Match(cmux.Any())

	// RPC Server
	go func() {
		if err := sn.server.Serve(grpcL); err != nil {
			sn.logger.Error("rpc server error", zap.Error(err))
			sn.Close()
		}
	}()

	go func() {
		if err := sn.pprofServer.run(httpL); err != nil {
			sn.logger.Error("pprof server error", zap.Error(err))
			sn.Close()
		}
	}()

	go func() {
		if err := mux.Serve(); err != nil {
			sn.logger.Error("mux error", zap.Error(err))
			sn.Close()
		}
	}()

	logStreamPaths := set.New(0)
	for volume := range sn.options.Volumes {
		paths := volume.ReadLogStreamPaths(sn.clusterID, sn.storageNodeID)
		for _, path := range paths {
			if logStreamPaths.Contains(path) {
				return errors.Errorf("storagenode: duplicated log stream path (%s)", path)
			}
			logStreamPaths.Add(path)
		}
	}

	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()
	for logStreamPathIf := range logStreamPaths {
		logStreamPath := logStreamPathIf.(string)
		_, _, _, logStreamID, err := ParseLogStreamPath(logStreamPath)
		if err != nil {
			return err
		}

		storage, err := sn.createStorage(context.Background(), logStreamPath)
		if err != nil {
			return err
		}
		if err := sn.startLogStream(context.Background(), logStreamID, storage); err != nil {
			return err
		}
	}

	sn.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	sn.logger.Info("start")
	return nil
}

func (sn *StorageNode) Close() {
	sn.muRunning.Lock()
	defer sn.muRunning.Unlock()

	sn.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	if !sn.running {
		return
	}
	sn.running = false

	// LogStreamReporter
	sn.lsr.Close()

	// LogStreamExecutors
	for _, lse := range sn.GetLogStreamExecutors() {
		lse.Close()
	}

	// RPC Server
	// FIXME (jun): Use GracefulStop
	sn.server.Stop()

	// TODO: set close timeout
	sn.tmStub.close(context.TODO())

	// TODO: Use close timeout
	sn.pprofServer.close(context.TODO())

	sn.sw.Stop()
	sn.logger.Info("stop")
}

func (sn *StorageNode) Wait() {
	sn.sw.Wait()
}

func (sn *StorageNode) ClusterID() types.ClusterID {
	return sn.clusterID
}

func (sn *StorageNode) StorageNodeID() types.StorageNodeID {
	return sn.storageNodeID
}

// GetMeGetMetadata implements the Management GetMetadata method.
func (sn *StorageNode) GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	ctx, span := sn.tmStub.startSpan(ctx, "storagenode.GetMetadata")
	defer span.End()

	snmeta := &varlogpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.clusterID,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNodeID: sn.storageNodeID,
			Address:       sn.advertiseAddr,
			Status:        varlogpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping,
		},
		CreatedTime: sn.tst.Created(),
		UpdatedTime: sn.tst.LastUpdated(),
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
	ctx, span := sn.tmStub.startSpan(ctx, "storagenode.logStreamMetadataDescriptors")
	defer span.End()

	tick := time.Now()
	lseList := sn.GetLogStreamExecutors()
	span.SetAttributes(label.Int64("get_log_stream_executors_ms", time.Since(tick).Milliseconds()))

	if len(lseList) == 0 {
		return nil
	}
	lsmetas := make([]varlogpb.LogStreamMetadataDescriptor, len(lseList))
	for i, lse := range lseList {
		lsmetas[i] = varlogpb.LogStreamMetadataDescriptor{
			StorageNodeID: sn.storageNodeID,
			LogStreamID:   lse.LogStreamID(),
			Status:        lse.Status(),
			HighWatermark: lse.HighWatermark(),
			Path:          lse.Path(),
			CreatedTime:   lse.Created(),
			UpdatedTime:   lse.LastUpdated(),
		}
	}
	return lsmetas
}

// AddLogStream implements the Management AddLogStream method.
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

	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()

	if _, ok := sn.lseMap[logStreamID]; ok {
		return "", errors.New("storagenode: log stream already exists")
	}

	storage, err := sn.createStorage(ctx, lsPath)
	if err != nil {
		return "", err
	}

	if err := sn.startLogStream(ctx, logStreamID, storage); err != nil {
		return "", err
	}
	return lsPath, nil
}

func (sn *StorageNode) createStorage(ctx context.Context, logStreamPath string) (Storage, error) {
	ctx, span := sn.tmStub.startSpan(ctx, "storagenode/StorageNode.createStorage")
	defer span.End()

	snOpts := []StorageOption{
		WithPath(logStreamPath),
		WithLogger(sn.logger),
	}
	if sn.options.StorageOptions.EnableWriteFsync {
		snOpts = append(snOpts, WithEnableWriteFsync())
	}
	if sn.options.StorageOptions.EnableCommitFsync {
		snOpts = append(snOpts, WithEnableCommitFsync())
	}
	if sn.options.StorageOptions.EnableCommitContextFsync {
		snOpts = append(snOpts, WithEnableCommitContextFsync())
	}
	if sn.options.StorageOptions.EnableDeleteCommittedFsync {
		snOpts = append(snOpts, WithEnableDeleteCommittedFsync())
	}
	if sn.options.StorageOptions.DisableDeleteUncommittedFsync {
		snOpts = append(snOpts, WithDisableDeleteUncommittedFsync())
	}
	storage, err := NewStorage(sn.options.StorageOptions.Name, snOpts...)
	if err != nil {
		span.RecordError(err)
	}
	return storage, err
}

func (sn *StorageNode) startLogStream(ctx context.Context, logStreamID types.LogStreamID, storage Storage) (err error) {
	ctx, span := sn.tmStub.startSpan(ctx, "storagenode/StorageNode.startLogStream")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	lse, err := NewLogStreamExecutor(sn.logger, sn.storageNodeID, logStreamID, storage, sn.tmStub, &sn.options.LogStreamExecutorOptions)
	if err != nil {
		return err
	}
	if err = lse.Run(ctx); err != nil {
		lse.Close()
		return err
	}
	sn.tst.Touch()
	sn.lseMap[logStreamID] = lse
	return nil
}

// RemoveLogStream implements the Management RemoveLogStream method.
func (sn *StorageNode) RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	ctx, span := sn.tmStub.startSpan(ctx, "storagenode/StorageNode.RemoveLogStream")
	defer span.End()

	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()

	lse, ok := sn.lseMap[logStreamID]
	if !ok {
		return verrors.ErrNotExist
	}
	delete(sn.lseMap, logStreamID)

	tick := time.Now()
	lse.Close()
	closeDuration := time.Since(tick)

	tick = time.Now()
	// TODO (jun): Is removing data path optional or default behavior?
	if err := os.RemoveAll(lse.Path()); err != nil {
		sn.logger.Warn("error while removing log stream path")
	}
	removeDuration := time.Since(tick)
	sn.tst.Touch()

	span.SetAttributes(label.Int64("close_duration_ms", closeDuration.Milliseconds()),
		label.Int64("remove_duration_ms", removeDuration.Milliseconds()))
	return nil
}

// Seal implements the Management Seal method.
func (sn *StorageNode) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, errors.WithStack(errNoLogStream)
	}
	status, hwm := lse.Seal(lastCommittedGLSN)
	sn.tst.Touch()
	return status, hwm, nil
}

// Unseal implements the Management Unseal method.
func (sn *StorageNode) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return errors.WithStack(errNoLogStream)
	}
	sn.tst.Touch()
	return lse.Unseal()
}

func (sn *StorageNode) Sync(ctx context.Context, logStreamID types.LogStreamID, replica Replica, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
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

func (sn *StorageNode) GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
	sn.lseMtx.RLock()
	defer sn.lseMtx.RUnlock()
	lse, ok := sn.lseMap[logStreamID]
	return lse, ok
}

func (sn *StorageNode) GetLogStreamExecutors() []LogStreamExecutor {
	sn.lseMtx.RLock()
	ret := make([]LogStreamExecutor, 0, len(sn.lseMap))
	for _, lse := range sn.lseMap {
		ret = append(ret, lse)
	}
	sn.lseMtx.RUnlock()
	return ret
}

func (sn *StorageNode) verifyClusterID(cid types.ClusterID) bool {
	return sn.clusterID == cid
}

func (sn *StorageNode) verifyStorageNodeID(snid types.StorageNodeID) bool {
	return sn.storageNodeID == snid
}
