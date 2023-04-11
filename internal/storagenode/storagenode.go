package storagenode

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/status"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/storage"
	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/internal/storagenode/executorsmap"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/internal/storagenode/volume"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/fputil"
	"github.com/kakao/varlog/pkg/util/netutil"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	hintNumExecutors        = 32
	singleflightKeyMetadata = "metadata"
)

type StorageNode struct {
	config
	ballast []byte
	snPaths []string

	executors *executorsmap.ExecutorsMap

	mu           sync.RWMutex
	lis          net.Listener
	server       *grpc.Server
	healthServer *health.Server
	closed       bool
	closedC      chan struct{}

	mux cmux.CMux

	pprofServer *pprof.Server
	// FIXME: some metadata should be stored into storage.

	metrics *telemetry.Metrics

	sf singleflight.Group

	startTime time.Time

	limits struct {
		logStreamReplicasCount atomic.Int32
	}
}

func NewStorageNode(opts ...Option) (*StorageNode, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	metrics, err := telemetry.RegisterMetrics(global.Meter("varlogsn"), cfg.snid)
	if err != nil {
		return nil, err
	}

	snPaths, err := volume.CreateStorageNodePaths(cfg.volumes, cfg.cid, cfg.snid)
	if err != nil {
		return nil, err
	}
	if len(snPaths) == 0 {
		return nil, errors.New("storage node: no valid storage node path")
	}

	dataDirs, err := volume.ReadVolumes(cfg.volumes)
	if err != nil {
		return nil, err
	}
	dataDirs = filterValidDataDirectories(dataDirs, cfg.cid, cfg.snid, cfg.logger)

	grpcServer := grpc.NewServer(
		grpc.ReadBufferSize(int(cfg.grpcServerReadBufferSize)),
		grpc.WriteBufferSize(int(cfg.grpcServerWriteBufferSize)),
		grpc.MaxRecvMsgSize(int(cfg.grpcServerMaxRecvMsgSize)),
		grpc.UnaryInterceptor(
			func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
				start := time.Now()
				resp, err := handler(ctx, req)
				if err != nil {
					duration := time.Since(start)
					cfg.logger.Error(info.FullMethod,
						zap.Stringer("code", status.Code(err)),
						zap.Int64("duration", duration.Microseconds()),
						zap.Stringer("request", req.(fmt.Stringer)),
						zap.Error(err),
					)
				}
				return resp, err
			},
		),
	)

	sn := &StorageNode{
		config:       cfg,
		executors:    executorsmap.New(hintNumExecutors),
		server:       grpcServer,
		healthServer: health.NewServer(),
		closedC:      make(chan struct{}),
		snPaths:      snPaths,
		pprofServer:  pprof.New(cfg.pprofOpts...),
		metrics:      metrics,
		startTime:    time.Now().UTC(),
	}
	if sn.ballastSize > 0 {
		sn.ballast = make([]byte, sn.ballastSize)
	}
	if err := sn.loadLogStreamReplicas(dataDirs); err != nil {
		return nil, err
	}

	return sn, nil
}

func filterValidDataDirectories(dataDirs []volume.DataDir, cid types.ClusterID, snid types.StorageNodeID, logger *zap.Logger) []volume.DataDir {
	ret := make([]volume.DataDir, 0, len(dataDirs))
	for _, dd := range dataDirs {
		if err := dd.Valid(cid, snid); err != nil {
			logger.Info("ignore incorrect data directory", zap.String("dir", dd.String()))
			continue
		}
		ret = append(ret, dd)
	}
	return ret
}

func (sn *StorageNode) loadLogStreamReplicas(dataDirs []volume.DataDir) error {
	var g errgroup.Group
	for i := range dataDirs {
		dataDir := dataDirs[i]
		g.Go(func() error {
			_, err := sn.runLogStreamReplica(context.Background(), dataDir.TopicID, dataDir.LogStreamID, dataDir.String())
			return err
		})
	}
	return g.Wait()
}

func (sn *StorageNode) Serve() error {
	sn.mu.Lock()
	if sn.lis != nil {
		sn.mu.Unlock()
		return errors.New("storage node: already serving")
	}
	lis, err := net.Listen("tcp", sn.listen)
	if err != nil {
		sn.mu.Unlock()
		return err
	}
	sn.lis = lis
	if len(sn.advertise) == 0 {
		addrs, _ := netutil.GetListenerAddrs(lis.Addr())
		sn.advertise = addrs[0]
	}

	snpb.RegisterManagementServer(sn.server, &adminServer{sn})
	snpb.RegisterLogStreamReporterServer(sn.server, &reportCommitServer{sn})
	snpb.RegisterLogIOServer(sn.server, &logServer{sn})
	snpb.RegisterReplicatorServer(sn.server, &replicationServer{
		sn:     sn,
		logger: sn.logger.Named("replicate server"),
	})
	grpc_health_v1.RegisterHealthServer(sn.server, sn.healthServer)
	sn.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	sn.logger.Info("serving",
		zap.String("listen", sn.listen),
		zap.Int64("ballast", sn.ballastSize),
		zap.Int64("grpcServerReadBufferSize", sn.grpcServerReadBufferSize),
		zap.Int64("grpcServerWriteBufferSize", sn.grpcServerWriteBufferSize),
		zap.Int64("grpcServerMaxRecvMsgSize", sn.grpcServerMaxRecvMsgSize),
		zap.Int64("grpcReplicateClientReadBufferSize", sn.replicateClientReadBufferSize),
		zap.Int64("grpcReplicateClientWriteBufferSize", sn.replicateClientWriteBufferSize),
	)
	sn.mu.Unlock()

	sn.mux = cmux.New(sn.lis)
	httpL := sn.mux.Match(cmux.HTTP1Fast())
	grpcL := sn.mux.Match(cmux.Any())

	var g errgroup.Group
	g.Go(func() error {
		return sn.server.Serve(grpcL)
	})
	g.Go(func() error {
		err := sn.pprofServer.Run(httpL)
		if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, cmux.ErrListenerClosed) {
			return nil
		}
		return err
	})
	g.Go(func() error {
		err := sn.mux.Serve()
		if err != nil && !strings.Contains(err.Error(), "use of closed") {
			return err
		}
		return nil
	})

	return g.Wait()
}

func (sn *StorageNode) Close() (err error) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	if sn.closed {
		return nil
	}
	sn.closed = true
	close(sn.closedC)

	sn.mux.Close()
	sn.pprofServer.Close(context.Background())
	sn.healthServer.Shutdown()
	sn.executors.Range(func(_ types.LogStreamID, _ types.TopicID, lse *logstream.Executor) bool {
		err = multierr.Append(err, lse.Close())
		return true
	})
	sn.server.Stop() // TODO: sn.server.GracefulStop() -> need not to use mutex
	sn.logger.Info("closed")
	return err
}

func (sn *StorageNode) getMetadata(_ context.Context) (*snpb.StorageNodeMetadataDescriptor, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return nil, snerrors.ErrClosed
	}

	ret, _, _ := sn.sf.Do(singleflightKeyMetadata, func() (interface{}, error) {
		snmeta := &snpb.StorageNodeMetadataDescriptor{
			ClusterID: sn.cid,
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: sn.snid,
				Address:       sn.advertise,
			},
			Status:    varlogpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping,
			StartTime: sn.startTime,
		}

		for _, path := range sn.snPaths {
			all, used, _ := fputil.DiskSize(path)
			snmeta.Storages = append(snmeta.Storages,
				varlogpb.StorageDescriptor{
					Path:  path,
					Used:  used,
					Total: all,
				},
			)
		}

		sn.executors.Range(func(_ types.LogStreamID, _ types.TopicID, lse *logstream.Executor) bool {
			if lsmd, err := lse.Metadata(); err == nil {
				snmeta.LogStreamReplicas = append(snmeta.LogStreamReplicas, lsmd)
			}
			return true
		})

		return snmeta, nil
	})
	return ret.(*snpb.StorageNodeMetadataDescriptor), nil
}

func (sn *StorageNode) addLogStreamReplica(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, snPath string) (snpb.LogStreamReplicaMetadataDescriptor, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return snpb.LogStreamReplicaMetadataDescriptor{}, snerrors.ErrClosed
	}

	lsDirName := volume.LogStreamDirName(tpid, lsid)
	lsPath := path.Join(snPath, lsDirName)
	// If flag `--experimental-storage-seprate-db` is turned on, the data
	// directory should be created first because it might not exist currently.
	// The storage engine will create subdirectories within the data directory
	// while the flag is set.
	// If the directory already exists, the error can be ignored silently.
	_ = os.Mkdir(lsPath, volume.VolumeFileMode)

	lse, err := sn.runLogStreamReplica(ctx, tpid, lsid, lsPath)
	if err != nil {
		return snpb.LogStreamReplicaMetadataDescriptor{}, err
	}

	return lse.Metadata()
}

func (sn *StorageNode) runLogStreamReplica(_ context.Context, tpid types.TopicID, lsid types.LogStreamID, lsPath string) (*logstream.Executor, error) {
	if added := sn.limits.logStreamReplicasCount.Add(1); sn.maxLogStreamReplicasCount >= 0 && added > sn.maxLogStreamReplicasCount {
		sn.limits.logStreamReplicasCount.Add(-1)
		return nil, snerrors.ErrTooManyReplicas
	}

	lsm, err := telemetry.RegisterLogStreamMetrics(sn.metrics, lsid)
	if err != nil {
		return nil, err
	}

	stgOpts := make([]storage.Option, len(sn.defaultStorageOptions))
	copy(stgOpts, sn.defaultStorageOptions)
	stgOpts = append(stgOpts,
		storage.WithPath(lsPath),
		storage.WithLogger(sn.logger.Named("storage").With(zap.String("path", lsPath))),
	)
	stg, err := storage.New(stgOpts...)
	if err != nil {
		return nil, err
	}

	lseOpts := make([]logstream.ExecutorOption, len(sn.defaultLogStreamExecutorOptions))
	copy(lseOpts, sn.defaultLogStreamExecutorOptions)
	lseOpts = append(lseOpts,
		logstream.WithStorageNodeID(sn.snid),
		logstream.WithTopicID(tpid),
		logstream.WithLogStreamID(lsid),
		logstream.WithLogger(sn.logger),
		logstream.WithStorage(stg),
		logstream.WithReplicateClientGRPCOptions(
			grpc.WithReadBufferSize(int(sn.replicateClientReadBufferSize)),
			grpc.WithWriteBufferSize(int(sn.replicateClientWriteBufferSize)),
		),
		logstream.WithLogStreamMetrics(lsm),
	)

	lse, err := logstream.NewExecutor(lseOpts...)
	if err != nil {
		return nil, err
	}

	if _, loaded := sn.executors.LoadOrStore(tpid, lsid, lse); loaded {
		_ = lse.Close()
		return nil, errors.New("storage node: logstream already exists")
	}
	return lse, nil
}

func (sn *StorageNode) removeLogStreamReplica(_ context.Context, tpid types.TopicID, lsid types.LogStreamID) error {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return snerrors.ErrClosed
	}

	lse, loaded := sn.executors.LoadAndDelete(tpid, lsid)
	if !loaded {
		return snerrors.ErrNotExist
	}
	if err := lse.Close(); err != nil {
		sn.logger.Warn("error while closing log stream replica")
	}
	telemetry.UnregisterLogStreamMetrics(sn.metrics, lsid)
	if err := os.RemoveAll(lse.Path()); err != nil {
		sn.logger.Warn("error while removing log stream path")
	}
	sn.limits.logStreamReplicasCount.Add(-1)
	return nil
}

func (sn *StorageNode) seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, snerrors.ErrClosed
	}

	lse, loaded := sn.executors.Load(tpid, lsid)
	if !loaded {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, snerrors.ErrNotExist
	}

	return lse.Seal(ctx, lastCommittedGLSN)
}

func (sn *StorageNode) unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, replicas []varlogpb.LogStreamReplica) error {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return snerrors.ErrClosed
	}

	lse, loaded := sn.executors.Load(tpid, lsid)
	if !loaded {
		return snerrors.ErrNotExist
	}

	return lse.Unseal(ctx, replicas)
}

func (sn *StorageNode) sync(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, dst varlogpb.LogStreamReplica) (*snpb.SyncStatus, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return nil, snerrors.ErrClosed
	}

	lse, loaded := sn.executors.Load(tpid, lsid)
	if !loaded {
		return nil, snerrors.ErrNotExist
	}

	return lse.Sync(ctx, dst)
}

func (sn *StorageNode) trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) map[types.LogStreamID]string {
	ret := make(map[types.LogStreamID]string)
	sn.executors.Range(func(lsid types.LogStreamID, tpid types.TopicID, lse *logstream.Executor) bool {
		if topicID != tpid {
			return true
		}
		var msg string
		if err := lse.Trim(ctx, lastGLSN); err != nil {
			msg = err.Error()
		}
		ret[lsid] = msg
		return true
	})
	return ret
}
