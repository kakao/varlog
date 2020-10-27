package metadata_repository

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"time"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/netutil"
	"go.uber.org/zap"
)

const (
	DefaultRPCBindAddress                        = "0.0.0.0:9092"
	DefaultRaftPort                              = 10000
	DefaultSnapshotCount           uint64        = 10000
	DefaultSnapshotCatchUpEntriesN uint64        = 10000
	DefaultLogReplicationFactor    int           = 1
	DefaultProposeTimeout          time.Duration = 100 * time.Millisecond
	DefaultRaftTick                time.Duration = 100 * time.Millisecond
	DefaultRPCTimeout              time.Duration = 100 * time.Millisecond

	UnusedRequestIndex uint64 = 0
)

var (
	DefaultRaftAddress = makeDefaultRaftAddress()
)

func makeDefaultRaftAddress() string {
	ips, _ := netutil.UnicastIPs()
	if len(ips) > 0 {
		return "http://" + net.JoinHostPort(ips[0].String(), strconv.Itoa(DefaultRaftPort))
	}
	return ""
}

type MetadataRepositoryOptions struct {
	RPCBindAddress     string
	RaftAddress        string
	ClusterID          types.ClusterID
	NodeID             types.NodeID
	Join               bool
	Verbose            bool
	NumRep             int
	SnapCount          uint64
	RaftTick           time.Duration
	RaftProposeTimeout time.Duration
	RPCTimeout         time.Duration
	Peers              []string
	ReporterClientFac  ReporterClientFactory
	Logger             *zap.Logger
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

	if options.Logger == nil {
		return errors.New("logger should not be nil")
	}

	if options.SnapCount == 0 {
		options.SnapCount = DefaultSnapshotCount
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

	return nil
}
