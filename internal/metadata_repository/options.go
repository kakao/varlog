package metadata_repository

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/netutil"
)

const (
	DefaultRPCBindAddress                          = "0.0.0.0:9092"
	DefaultRaftPort                                = 10000
	DefaultSnapshotCount             uint64        = 10000
	DefaultSnapshotCatchUpCount      uint64        = 10000
	DefaultLogReplicationFactor      int           = 1
	DefaultProposeTimeout            time.Duration = 100 * time.Millisecond
	DefaultRaftTick                  time.Duration = 100 * time.Millisecond
	DefaultRPCTimeout                time.Duration = 100 * time.Millisecond
	DefaultCommitTick                time.Duration = 1 * time.Millisecond
	DefaultPromoteTick               time.Duration = 100 * time.Millisecond
	DefaultRaftDir                   string        = "raftdata"
	DefaultLogDir                    string        = "log"
	DefaultTelemetryCollectorName    string        = "nop"
	DefaultTelmetryCollectorEndpoint string        = "localhost:55680"

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

type MetadataRepositoryOptions struct {
	TelemetryOptions

	RPCBindAddress     string
	RaftAddress        string
	ClusterID          types.ClusterID
	NodeID             types.NodeID
	Join               bool
	Verbose            bool
	NumRep             int
	SnapCount          uint64
	SnapCatchUpCount   uint64
	RaftTick           time.Duration
	RaftProposeTimeout time.Duration
	RPCTimeout         time.Duration
	CommitTick         time.Duration
	PromoteTick        time.Duration
	Peers              []string
	ReporterClientFac  ReporterClientFactory
	RaftDir            string
	LogDir             string
	Logger             *zap.Logger
}

type TelemetryOptions struct {
	CollectorName     string
	CollectorEndpoint string
}

func (options *MetadataRepositoryOptions) validate() error {
	raftURL, err := url.Parse(options.RaftAddress)
	if err != nil {
		return err
	}
	raftHost, _, err := net.SplitHostPort(raftURL.Host)
	if err != nil {
		return err
	}

	if options.Logger == nil {
		return errors.New("logger should not be nil")
	}

	raftIP := net.ParseIP(raftHost)
	if !raftIP.IsGlobalUnicast() || raftIP.IsLoopback() {
		options.Logger.Warn("bad RAFT address", zap.Any("addr", options.RaftAddress))
	}

	if options.NodeID == types.InvalidNodeID {
		return errors.New("invalid nodeID")
	}

	if options.RPCBindAddress == "" {
		options.RPCBindAddress = DefaultRPCBindAddress
	}

	if options.NumRep < 1 {
		return errors.New("NumRep should be bigger than 0")
	}

	if len(options.Peers) < 1 {
		return errors.New("# of PeerList should be bigger than 0")
	}

	if options.ReporterClientFac == nil {
		return errors.New("reporterClientFac should not be nil")
	}

	if options.SnapCount == 0 {
		options.SnapCount = DefaultSnapshotCount
	}

	if options.SnapCatchUpCount == 0 {
		options.SnapCatchUpCount = DefaultSnapshotCatchUpCount
	}

	if options.RaftTick == time.Duration(0) {
		options.RaftTick = DefaultRaftTick
	}

	if options.RaftProposeTimeout == time.Duration(0) {
		options.RaftProposeTimeout = DefaultProposeTimeout
	}

	if options.RPCTimeout == time.Duration(0) {
		options.RPCTimeout = DefaultRPCTimeout
	}

	if options.CommitTick == time.Duration(0) {
		options.CommitTick = DefaultCommitTick
	}

	if options.PromoteTick == time.Duration(0) {
		options.PromoteTick = DefaultPromoteTick
	}

	if options.RaftDir == "" {
		options.RaftDir = DefaultRaftDir
	}

	if options.LogDir == "" {
		options.LogDir = DefaultLogDir
	}

	return nil
}
