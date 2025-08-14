package varlog

//go:generate go tool mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/varlog -package varlog -destination metadata_refresher_mock.go . MetadataRefresher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/kakao/varlog/pkg/mrc/mrconnector"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/varlogpb"
)

type Renewable interface {
	Renew(metadata *varlogpb.MetadataDescriptor)
}

type MetadataRefresher interface {
	Refresh(context.Context)
	Metadata() *varlogpb.MetadataDescriptor
	Close() error
}

// metadataRefresher fetches metadata from the metadata repository nodes via mrconnector. It also
// updates internal fields to provide metadata to its callers.
// It can provide stale metadata to callers.
type metadataRefresher struct {
	connector         mrconnector.Connector
	metadata          atomic.Value // *varlogpb.MetadataDescriptor
	allowlist         RenewableAllowlist
	replicasRetriever RenewableReplicasRetriever
	refreshInterval   time.Duration
	group             singleflight.Group
	runner            *runner.Runner
	cancel            context.CancelFunc
	logger            *zap.Logger
}

func newMetadataRefresher(
	ctx context.Context,
	connector mrconnector.Connector,
	allowlist RenewableAllowlist,
	replicasRetriever RenewableReplicasRetriever,
	refreshInterval,
	refreshTimeout time.Duration,
	logger *zap.Logger,
) (*metadataRefresher, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("metarefresher")

	mr := &metadataRefresher{
		connector:         connector,
		refreshInterval:   refreshInterval,
		allowlist:         allowlist,
		replicasRetriever: replicasRetriever,
		logger:            logger,
		runner:            runner.New("metarefresher", logger),
	}
	if err := mr.refresh(ctx); err != nil {
		return nil, fmt.Errorf("metarefresher: %w", err)
	}

	mctx, cancel := mr.runner.WithManagedCancel(context.Background())
	if err := mr.runner.RunC(mctx, mr.refresher); err != nil {
		cancel()
		mr.runner.Stop()
		return nil, err
	}
	mr.cancel = cancel
	return mr, nil
}

func (mr *metadataRefresher) Close() error {
	mr.cancel()
	mr.runner.Stop()
	if err := mr.allowlist.Close(); err != nil {
		mr.logger.Warn("error while closing allow/denylist", zap.Error(err))
	}
	return mr.connector.Close()
}

func (mr *metadataRefresher) refresher(ctx context.Context) {
	ticker := time.NewTicker(mr.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = mr.refresh(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (mr *metadataRefresher) refresh(ctx context.Context) error {
	_, err, _ := mr.group.Do("refresh", func() (interface{}, error) {
		// TODO
		// 1) Get MetadataDescriptor
		// 2) Compare underlying metadata
		// 3) Update allowlist, denylist, lsreplicas if the metadata is updated

		// TODO (jun): Use ClusterMetadataView
		// TODO: check whether ctx is usable or not
		client, err := mr.connector.Client(ctx)
		if err != nil {
			// TODO (jun): check if this is safe fix
			return nil, err
		}
		// TODO (jun): check if it needs retry? am I torching mr?
		clusmeta, err := client.GetMetadata(ctx)
		if err != nil {
			return nil, errors.Join(err, client.Close())
		}

		if clusmeta.GetAppliedIndex() == mr.getAppliedIndex() {
			return nil, nil
		}

		// update metadata
		mr.metadata.Store(clusmeta)

		// update allowlist
		mr.allowlist.Renew(clusmeta)

		// update replicasRetriever
		mr.replicasRetriever.Renew(clusmeta)
		return nil, nil
	})
	return err
}

func (mr *metadataRefresher) getAppliedIndex() uint64 {
	f := mr.metadata.Load()
	if f == nil {
		return 0
	}
	return f.(*varlogpb.MetadataDescriptor).GetAppliedIndex()
}

// TODO:: compare appliedIndex of metadata
func (mr *metadataRefresher) Refresh(ctx context.Context) {
	_ = mr.refresh(ctx)
}

func (mr *metadataRefresher) Metadata() *varlogpb.MetadataDescriptor {
	f := mr.metadata.Load()
	if f == nil {
		return nil
	}

	return f.(*varlogpb.MetadataDescriptor)
}
