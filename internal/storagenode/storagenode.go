package storagenode

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode/executorsmap"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/netutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const hintNumExecutors = 32

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

	pprofServer *pprof.Server
	// FIXME: some metadata should be stored into storage.

	metrics *telemetry.Metrics
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

	snPaths, err := createStorageNodePaths(cfg.volumes, cfg.cid, cfg.snid)
	if err != nil {
		return nil, err
	}
	if len(snPaths) == 0 {
		return nil, errors.New("storage node: no valid storage node path")
	}

	sn := &StorageNode{
		config:    cfg,
		executors: executorsmap.New(hintNumExecutors),
		server: grpc.NewServer(
			grpc.ReadBufferSize(int(cfg.grpcServerReadBufferSize)),
			grpc.WriteBufferSize(int(cfg.grpcServerWriteBufferSize)),
			grpc.MaxRecvMsgSize(int(cfg.grpcServerMaxRecvMsgSize)),
		),
		healthServer: health.NewServer(),
		closedC:      make(chan struct{}),
		snPaths:      snPaths,
		pprofServer:  pprof.New(cfg.pprofOpts...),
		metrics:      metrics,
	}
	if sn.ballastSize > 0 {
		sn.ballast = make([]byte, sn.ballastSize)
	}
	if err := sn.loadLogStreamReplicas(); err != nil {
		return nil, err
	}

	return sn, nil
}

func (sn *StorageNode) loadLogStreamReplicas() error {
	lsPaths := readLogStreamPaths(sn.snPaths)
	for _, lsPath := range lsPaths {
		_, _, tpid, lsid, err := parseLogStreamPath(lsPath)
		if err != nil {
			return err
		}
		if err := sn.runLogStreamReplica(context.Background(), tpid, lsid, lsPath); err != nil {
			return err
		}
	}
	return nil
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

	mux := cmux.New(sn.lis)
	httpL := mux.Match(cmux.HTTP1Fast())
	grpcL := mux.Match(cmux.Any())

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
		err := mux.Serve()
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
	sn.healthServer.Shutdown()

	sn.executors.Range(func(_ types.LogStreamID, _ types.TopicID, lse *logstream.Executor) bool {
		err = multierr.Append(err, lse.Close())
		return true
	})
	sn.server.Stop() // TODO: sn.server.GracefulStop() -> need not to use mutex
	sn.logger.Info("closed")
	return err
}

func (sn *StorageNode) getMetadata(_ context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return nil, errors.New("storage node: closed")
	}

	snmeta := &varlogpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.cid,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: sn.snid,
				Address:       sn.advertise,
			},
			Status: varlogpb.StorageNodeStatusRunning, // TODO (jun), Ready, Running, Stopping,
		},
	}

	for i := range sn.snPaths {
		snmeta.StorageNode.Storages = append(snmeta.StorageNode.Storages,
			&varlogpb.StorageDescriptor{
				Path: sn.snPaths[i],
			},
		)
	}

	sn.executors.Range(func(_ types.LogStreamID, _ types.TopicID, lse *logstream.Executor) bool {
		if lsmd, err := lse.Metadata(); err == nil {
			snmeta.LogStreams = append(snmeta.LogStreams, lsmd)
		}
		return true
	})

	return snmeta, nil
}

func (sn *StorageNode) addLogStreamReplica(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, snPath string) (string, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return "", errors.New("storage node: closed")
	}

	lsDirName := logStreamDirName(tpid, lsid)
	lsPath := path.Join(snPath, lsDirName)

	return lsPath, sn.runLogStreamReplica(ctx, tpid, lsid, lsPath)
}

func (sn *StorageNode) runLogStreamReplica(_ context.Context, tpid types.TopicID, lsid types.LogStreamID, lsPath string) error {
	lsm, err := telemetry.RegisterLogStreamMetrics(sn.metrics, lsid)
	if err != nil {
		return err
	}

	stg, err := storage.New(append(
		sn.defaultStorageOptions,
		storage.WithPath(lsPath),
		storage.WithLogger(sn.logger.Named("storage").With(zap.String("path", lsPath))),
	)...)
	if err != nil {
		return err
	}

	lse, err := logstream.NewExecutor(append(
		sn.defaultLogStreamExecutorOptions,
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
	)...)
	if err != nil {
		return err
	}

	if _, loaded := sn.executors.LoadOrStore(tpid, lsid, lse); loaded {
		_ = lse.Close()
		return errors.New("storage node: logstream already exists")
	}
	return nil
}

func (sn *StorageNode) removeLogStreamReplica(_ context.Context, tpid types.TopicID, lsid types.LogStreamID) error {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return errors.New("storage node: closed")
	}

	lse, loaded := sn.executors.LoadAndDelete(tpid, lsid)
	if !loaded {
		return verrors.ErrNotExist
	}
	_ = lse.Close()
	telemetry.UnregisterLogStreamMetrics(sn.metrics, lsid)
	// TODO (jun): Is removing data path optional or default behavior?
	// if err := os.RemoveAll(lse.Path()); err != nil {
	//	sn.logger.Warn("error while removing log stream path")
	// }
	return nil
}

func (sn *StorageNode) seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, errors.New("storage node: closed")
	}

	lse, loaded := sn.executors.Load(tpid, lsid)
	if !loaded {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, errors.New("storage node: no log stream")
	}

	return lse.Seal(ctx, lastCommittedGLSN)
}

func (sn *StorageNode) unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, replicas []varlogpb.Replica) error {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return errors.New("storage node: closed")
	}

	lse, loaded := sn.executors.Load(tpid, lsid)
	if !loaded {
		return errors.New("storage node: no log stream")
	}

	return lse.Unseal(ctx, replicas)
}

func (sn *StorageNode) sync(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, dst varlogpb.Replica) (*snpb.SyncStatus, error) {
	sn.mu.RLock()
	defer sn.mu.RUnlock()
	if sn.closed {
		return nil, errors.New("storage node: closed")
	}

	lse, loaded := sn.executors.Load(tpid, lsid)
	if !loaded {
		return nil, errors.New("storage node: no log stream")
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