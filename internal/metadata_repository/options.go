package metadata_repository

import (
	"errors"
	"time"

	"github.com/urfave/cli/v2"
	types "github.com/kakao/varlog/pkg/varlog/types"
	"go.uber.org/zap"
)

const (
	DefaultRPCBindAddress                        = "0.0.0.0:9092"
	DefaultSnapshotCount           uint64        = 10000
	DefaultSnapshotCatchUpEntriesN uint64        = 10000
	DefaultLogReplicationFactor    int           = 1
	DefaultProposeTimeout          time.Duration = time.Second

	UnusedRequestIndex uint64 = 0
)

type MetadataRepositoryOptions struct {
	RPCBindAddress    string
	ClusterID         types.ClusterID
	NodeID            types.NodeID
	Join              bool
	Verbose           bool
	NumRep            int
	SnapCount         uint64
	PeerList          cli.StringSlice
	ReporterClientFac ReporterClientFactory
	Logger            *zap.Logger
}

func (options *MetadataRepositoryOptions) validate() error {
	if options.RPCBindAddress == "" {
		options.RPCBindAddress = DefaultRPCBindAddress
	}

	if options.NodeID == types.InvalidNodeID {
		return errors.New("invalid index")
	}

	if options.NumRep < 1 {
		return errors.New("NumRep should be bigger than 0")
	}

	if len(options.PeerList.Value()) < 1 {
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

	return nil
}
