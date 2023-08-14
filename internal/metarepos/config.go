package metarepos

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/netutil"
)

const (
	DefaultClusterID                        = types.ClusterID(1)
	DefaultRPCBindAddress                   = "0.0.0.0:9092"
	DefaultDebugAddress                     = "0.0.0.0:9099"
	DefaultRaftPort                         = 10000
	DefaultSnapshotCount             uint64 = 10000
	DefaultSnapshotCatchUpCount      uint64 = 10000
	DefaultSnapshotPurgeCount        uint   = 10
	DefaultWalPurgeCount             uint   = 10
	DefaultLogReplicationFactor      int    = 1
	DefaultProposeTimeout                   = 100 * time.Millisecond
	DefaultRaftTick                         = 100 * time.Millisecond
	DefaultRPCTimeout                       = 100 * time.Millisecond
	DefaultCommitTick                       = 1 * time.Millisecond
	DefaultPromoteTick                      = 100 * time.Millisecond
	DefaultRaftDir                   string = "raftdata"
	DefaultLogDir                    string = "log"
	DefaultTelemetryCollectorName    string = "nop"
	DefaultTelmetryCollectorEndpoint string = "localhost:55680"

	DefaultReportCommitterReadBufferSize  = 32 * 1024 // 32KB
	DefaultReportCommitterWriteBufferSize = 32 * 1024 // 32KB

	DefaultMaxTopicsCount             = -1
	DefaultMaxLogStreamsCountPerTopic = -1

	UnusedRequestIndex uint64 = 0
)

var (
	DefaultRaftAddress = makeDefaultRaftAddress()
)

func makeDefaultRaftAddress() string {
	ips, _ := netutil.AdvertisableIPs()
	if len(ips) > 0 {
		return "http://" + net.JoinHostPort(ips[0].String(), strconv.Itoa(DefaultRaftPort))
	}
	return ""
}

type raftConfig struct {
	nodeID            types.NodeID // node ID for raft
	join              bool         // node is joining an existing cluster
	snapCount         uint64
	snapCatchUpCount  uint64
	maxSnapPurgeCount uint
	maxWalPurgeCount  uint
	raftTick          time.Duration // raft tick
	raftDir           string
	peers             []string // raft bootstrap peer URLs
}

type config struct {
	raftConfig
	clusterID                      types.ClusterID
	rpcAddr                        string
	raftAddr                       string
	debugAddr                      string
	replicationFactor              int
	raftProposeTimeout             time.Duration
	rpcTimeout                     time.Duration
	commitTick                     time.Duration
	promoteTick                    time.Duration
	reporterClientFac              ReporterClientFactory
	reportCommitterReadBufferSize  int
	reportCommitterWriteBufferSize int
	maxTopicsCount                 int32
	maxLogStreamsCountPerTopic     int32
	telemetryCollectorName         string
	telemetryCollectorEndpoint     string
	logger                         *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		raftConfig: raftConfig{
			snapCount:         DefaultSnapshotCount,
			snapCatchUpCount:  DefaultSnapshotCatchUpCount,
			maxSnapPurgeCount: DefaultSnapshotPurgeCount,
			maxWalPurgeCount:  DefaultWalPurgeCount,
			raftTick:          DefaultRaftTick,
			raftDir:           DefaultRaftDir,
		},
		clusterID:                      DefaultClusterID,
		rpcAddr:                        DefaultRPCBindAddress,
		raftAddr:                       DefaultRaftAddress,
		debugAddr:                      DefaultDebugAddress,
		replicationFactor:              DefaultLogReplicationFactor,
		raftProposeTimeout:             DefaultProposeTimeout,
		rpcTimeout:                     DefaultRPCTimeout,
		commitTick:                     DefaultCommitTick,
		promoteTick:                    DefaultPromoteTick,
		reportCommitterReadBufferSize:  DefaultReportCommitterReadBufferSize,
		reportCommitterWriteBufferSize: DefaultReportCommitterWriteBufferSize,
		maxTopicsCount:                 DefaultMaxTopicsCount,
		maxLogStreamsCountPerTopic:     DefaultMaxLogStreamsCountPerTopic,
		telemetryCollectorName:         DefaultTelemetryCollectorName,
		telemetryCollectorEndpoint:     DefaultTelmetryCollectorEndpoint,
		logger:                         zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	cfg.ensureDefault()

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	cfg.logger = cfg.logger.Named("mr").With(
		zap.Uint32("cid", uint32(cfg.clusterID)),
		zap.Uint64("nodeid", uint64(cfg.nodeID)),
	)

	return cfg, nil
}

func (cfg *config) ensureDefault() {
	cfg.nodeID = types.NewNodeIDFromURL(cfg.raftAddr)

	if cfg.reporterClientFac == nil {
		cfg.reporterClientFac = NewReporterClientFactory(
			grpc.WithReadBufferSize(cfg.reportCommitterReadBufferSize),
			grpc.WithWriteBufferSize(cfg.reportCommitterWriteBufferSize),
		)
	}

	// FIXME(pharrell): Is this good or not? - add the missing local address in peers
	cfg.ensurePeers()

	if cfg.snapCount == 0 {
		cfg.snapCount = DefaultSnapshotCount
	}

	if cfg.snapCatchUpCount == 0 {
		cfg.snapCatchUpCount = DefaultSnapshotCatchUpCount
	}

	if cfg.raftTick == time.Duration(0) {
		cfg.raftTick = DefaultRaftTick
	}

	if cfg.raftProposeTimeout == time.Duration(0) {
		cfg.raftProposeTimeout = DefaultProposeTimeout
	}

	if cfg.rpcTimeout == time.Duration(0) {
		cfg.rpcTimeout = DefaultRPCTimeout
	}

	if cfg.commitTick == time.Duration(0) {
		cfg.commitTick = DefaultCommitTick
	}

	if cfg.promoteTick == time.Duration(0) {
		cfg.promoteTick = DefaultPromoteTick
	}

	if cfg.raftDir == "" {
		cfg.raftDir = DefaultRaftDir
	}
}

func (cfg *config) ensurePeers() {
	found := false
	for _, peer := range cfg.peers {
		if peer == cfg.raftAddr {
			found = true
			break
		}
	}
	if !found {
		cfg.peers = append(cfg.peers, cfg.raftAddr)
	}
}

func (cfg *config) validate() error {
	if cfg.nodeID == types.InvalidNodeID {
		return errors.New("invalid nodeID")
	}
	raftURL, err := url.Parse(cfg.raftAddr)
	if err != nil {
		return err
	}
	if _, _, err := net.SplitHostPort(raftURL.Host); err != nil {
		return err
	}

	if cfg.logger == nil {
		return errors.New("logger should not be nil")
	}

	if cfg.replicationFactor < 1 {
		return errors.New("NumRep should be bigger than 0")
	}

	if cfg.reporterClientFac == nil {
		return errors.New("reporterClientFac should not be nil")
	}

	return nil
}

type Option interface {
	apply(options *config)
}

type funcOption struct {
	f func(*config)
}

func newFuncOption(f func(*config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *config) {
	fo.f(cfg)
}

func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.clusterID = cid
	})
}

func WithRPCAddress(rpcAddr string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.rpcAddr = rpcAddr
	})
}

func WithRaftAddress(raftAddr string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.raftAddr = raftAddr
	})
}

func WithRaftTick(raftTick time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.raftTick = raftTick
	})
}

func WithRaftDirectory(raftDir string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.raftDir = raftDir
	})
}

func WithCommitTick(commitTick time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.commitTick = commitTick
	})
}

func JoinCluster() Option {
	return newFuncOption(func(cfg *config) {
		cfg.join = true
	})
}

func WithReplicationFactor(replicationFactor int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicationFactor = replicationFactor
	})
}

func WithSnapshotCount(snapshotCount uint64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snapCount = snapshotCount
	})
}

func WithSnapshotCatchUpCount(snapCatchUpCount uint64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snapCatchUpCount = snapCatchUpCount
	})
}

func WithMaxSnapPurgeCount(maxSnapPurgeCount uint) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxSnapPurgeCount = maxSnapPurgeCount
	})
}

func WithMaxWALPurgeCount(maxWalPurgeCount uint) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxWalPurgeCount = maxWalPurgeCount
	})
}

func WithPeers(peers ...string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.peers = peers
	})
}

func WithDebugAddress(debugAddr string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.debugAddr = debugAddr
	})
}

func WithRPCTimeout(rpcTimeout time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.rpcTimeout = rpcTimeout
	})
}

func WithReporterClientFactory(reporterClientFac ReporterClientFactory) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reporterClientFac = reporterClientFac
	})
}

func WithReportCommitterReadBufferSize(readBufferSize int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reportCommitterReadBufferSize = readBufferSize
	})
}

func WithReportCommitterWriteBufferSize(writeBufferSize int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.reportCommitterWriteBufferSize = writeBufferSize
	})
}

func WithMaxTopicsCount(maxTopicsCount int32) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxTopicsCount = maxTopicsCount
	})
}

func WithMaxLogStreamsCountPerTopic(maxLogStreamsCountPerTopic int32) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxLogStreamsCountPerTopic = maxLogStreamsCountPerTopic
	})
}

func WithTelemetryCollectorName(telemetryCollectorName string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.telemetryCollectorName = telemetryCollectorName
	})
}

func WithTelemetryCollectorEndpoint(telemetryCollectorEndpoint string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.telemetryCollectorEndpoint = telemetryCollectorEndpoint
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
