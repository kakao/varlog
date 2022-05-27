package snwatcher

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/admin/snwatcher -package snwatcher -destination snwatcher_mock.go . EventHandler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type EventHandler interface {
	HandleHeartbeatTimeout(context.Context, types.StorageNodeID)
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

	hb map[types.StorageNodeID]time.Time
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
			if err := snw.heartbeat(ctx); err != nil {
				snw.logger.Warn("heartbeat", zap.Error(err))
			}

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

func (snw *StorageNodeWatcher) heartbeat(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, snw.heartbeatCheckDeadline)
	defer cancel()

	meta, err := snw.cmview.ClusterMetadata(ctx)
	if err != nil {
		return fmt.Errorf("snwatcher: could not get cluster metadata: %w", err)
	}

	snw.reload(meta.GetStorageNodes())

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
			if _, err := snw.snmgr.GetMetadata(ctx, snid); err != nil {
				errs[idx] = err
				return
			}
			snw.set(snid)
		}(i)
	}
	wg.Wait()
	snw.handleHeartbeat(ctx)
	return multierr.Combine(errs...)
}

func (snw *StorageNodeWatcher) set(snID types.StorageNodeID) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	snw.hb[snID] = time.Now()
}

func (snw *StorageNodeWatcher) reload(ss []*varlogpb.StorageNodeDescriptor) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	hb := make(map[types.StorageNodeID]time.Time)

	for _, s := range ss {
		hb[s.StorageNodeID] = time.Now()
	}

	for snID, t := range snw.hb {
		if _, ok := hb[snID]; ok {
			hb[snID] = t
		}
	}

	snw.hb = hb
}

func (snw *StorageNodeWatcher) handleHeartbeat(ctx context.Context) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	cur := time.Now()
	for snid, ts := range snw.hb {
		if cur.Sub(ts) > snw.tick*time.Duration(snw.heartbeatTimeout) {
			snw.eventHandler.HandleHeartbeatTimeout(ctx, snid)
			snw.hb[snid] = time.Now()
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
