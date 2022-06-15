package snwatcher

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/admin/snwatcher -package snwatcher -destination snwatcher_mock.go . EventHandler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type EventHandler interface {
	// HandleHeartbeatTimeout handles the storage node that is timed out heartbeat deadline.
	HandleHeartbeatTimeout(context.Context, types.StorageNodeID)

	// HandleReport reports the metadata of storage node collected by the repository.
	HandleReport(context.Context, *snpb.StorageNodeMetadataDescriptor)
}

// StorageNodeWatcher watches storage nodes in a cluster.
// It finds all storage nodes by fetching metadata from
// mrmanager.ClusterMetadataView periodically.
// It, then, calls proto/snpb.GetMetadata to check the health status of all
// storage nodes.
// The storage nodes that seem to be stuck are reported by the
// HandleHeartbeatTimeout method of the EventHandler interface.
// It also reports metadata of storage nodes by using HandleReport of the
// EventHandler interface.
type StorageNodeWatcher struct {
	config

	hb map[types.StorageNodeID]time.Time // last heartbeat times
	mu sync.RWMutex

	runner *runner.Runner
	cancel context.CancelFunc
}

func New(opts ...Option) (*StorageNodeWatcher, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return &StorageNodeWatcher{
		config: cfg,
		hb:     make(map[types.StorageNodeID]time.Time),
		runner: runner.New("wt", cfg.logger),
	}, nil
}

func (snw *StorageNodeWatcher) Start() error {
	cancel, err := snw.runner.Run(snw.run)
	if err != nil {
		return err
	}
	snw.cancel = cancel
	return nil
}

func (snw *StorageNodeWatcher) Stop() error {
	snw.cancel()
	snw.runner.Stop()
	return nil
}

func (snw *StorageNodeWatcher) run(ctx context.Context) {
	ticker := time.NewTicker(snw.tick)
	defer ticker.Stop()

	reportInterval := snw.reportInterval

	for {
		select {
		case <-ticker.C:
			now := time.Now().UTC()
			if err := snw.checkHeartbeat(ctx, now); err != nil {
				snw.logger.Warn("check heartbeat", zap.Error(err))
			}
			snw.handleHeartbeatTimeout(ctx, now)

			reportInterval--
			if reportInterval > 0 {
				continue
			}
			if err := snw.report(ctx); err != nil {
				snw.logger.Warn("report", zap.Error(err))
			}
			reportInterval = snw.reportInterval
		case <-ctx.Done():
			return
		}
	}
}

func (snw *StorageNodeWatcher) checkHeartbeat(ctx context.Context, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, snw.heartbeatCheckDeadline)
	defer cancel()

	md, err := snw.cmview.ClusterMetadata(ctx)
	if err != nil {
		return fmt.Errorf("snwatcher: heartbeat: %w", err)
	}

	snw.reload(md.StorageNodes, now)

	var (
		wg   sync.WaitGroup
		n    = len(md.StorageNodes)
		errs = make([]error, n)
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			snid := md.StorageNodes[idx].StorageNodeID
			snmd, err := snw.snmgr.GetMetadata(ctx, snid)
			if err != nil {
				errs[idx] = err
				return
			}
			snw.set(snid, now)
			snw.statsRepos.Report(ctx, snmd, now)
		}(i)
	}
	wg.Wait()
	return multierr.Combine(errs...)
}

func (snw *StorageNodeWatcher) set(snid types.StorageNodeID, ts time.Time) {
	snw.mu.Lock()
	defer snw.mu.Unlock()
	snw.hb[snid] = ts
}

// reload resets the map lastTimes.
// The timestamp for pre-existing storage node is retained, on the other hand,
// the timestamp for a new one is set to the argument ts.
// If the storage node is not in the argument snds, it is removed from the
// lastTimes.
func (snw *StorageNodeWatcher) reload(snds []*varlogpb.StorageNodeDescriptor, ts time.Time) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	lastTimes := make(map[types.StorageNodeID]time.Time, len(snds))
	for _, snd := range snds {
		snid := snd.StorageNodeID
		oldts, ok := snw.hb[snid]
		if ok {
			lastTimes[snid] = oldts
			continue
		}
		lastTimes[snid] = ts
	}
	snw.hb = lastTimes
}

func (snw *StorageNodeWatcher) handleHeartbeatTimeout(ctx context.Context, now time.Time) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	for snid, ts := range snw.hb {
		if now.Sub(ts) > snw.tick*time.Duration(snw.heartbeatTimeout) {
			snw.eventHandler.HandleHeartbeatTimeout(ctx, snid)
		}
	}
}

func (snw *StorageNodeWatcher) report(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, snw.reportDeadline)
	defer cancel()

	meta, err := snw.cmview.ClusterMetadata(ctx)
	if err != nil {
		return fmt.Errorf("snwatcher: could not get cluster metadata: %w", err)
	}

	var (
		wg   sync.WaitGroup
		n    = len(meta.StorageNodes)
		errs = make([]error, n)
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			snid := meta.StorageNodes[idx].StorageNodeID
			snmd, err := snw.snmgr.GetMetadata(ctx, snid)
			if err != nil {
				errs[idx] = err
				return
			}
			snw.eventHandler.HandleReport(ctx, snmd)
		}(i)
	}
	wg.Wait()
	return multierr.Combine(errs...)
}
