package varlog

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/mrc/mrconnector"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type Renewable interface {
	Renew(metadata *varlogpb.MetadataDescriptor)
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
	runner            *runner.Runner
	cancel            context.CancelFunc
	logger            *zap.Logger
}

func newMetadataRefresher(ctx context.Context, connector mrconnector.Connector, allowlist RenewableAllowlist, replicasRetriever RenewableReplicasRetriever, refreshInterval time.Duration, logger *zap.Logger) (*metadataRefresher, error) {
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
		return nil, errors.Wrap(err, "metarefresher")
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
			mr.refresh(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (mr *metadataRefresher) refresh(ctx context.Context) error {
	// TODO
	// 1) Get MetadataDescriptor
	// 2) Compare underlying metadata
	// 3) Update allowlist, denylist, lsreplicas if the metadata is updated

	// TODO (jun): Use ClusterMetadataView
	client, err := mr.connector.Client()
	if err != nil {
		// TODO (jun): check if this is safe fix
		return err
	}
	// TODO (jun): check if it needs retry? am I torching mr?
	clusmeta, err := client.GetMetadata(ctx)
	if err != nil {
		return multierr.Append(err, client.Close())
	}

	if clusmeta.Equal(mr.metadata.Load()) {
		return nil
	}

	// update metadata
	mr.metadata.Store(clusmeta)

	// update allowlist
	mr.allowlist.Renew(clusmeta)

	// update replicasRetriever
	mr.replicasRetriever.Renew(clusmeta)
	return nil
}
