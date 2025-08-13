package snwatcher

//go:generate go tool mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/admin/snwatcher -package snwatcher -destination snwatcher_mock.go . EventHandler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
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

	// continuous heartbeat fail cnt
	hbFailcnt map[types.StorageNodeID]int
	mu        sync.RWMutex

	runner *runner.Runner
	cancel context.CancelFunc
}

func New(opts ...Option) (*StorageNodeWatcher, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return &StorageNodeWatcher{
		config:    cfg,
		hbFailcnt: make(map[types.StorageNodeID]int),
		runner:    runner.New("wt", cfg.logger),
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
			if err := snw.checkHeartbeat(ctx); err != nil {
				snw.logger.Warn("check heartbeat", zap.Error(err))
			}
			snw.handleHeartbeatTimeout(ctx)

			reportInterval -= snw.tick
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

func (snw *StorageNodeWatcher) checkHeartbeat(ctx context.Context) error {
	md, err := snw.cmview.ClusterMetadata(ctx)
	if err != nil {
		return fmt.Errorf("snwatcher: heartbeat: %w", err)
	}

	snw.reload(md.StorageNodes)

	var (
		wg   sync.WaitGroup
		n    = len(md.StorageNodes)
		errs = make([]error, n)
	)

	mctx, cancel := context.WithTimeout(ctx, snw.heartbeatCheckDeadline)
	defer cancel()

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			snid := md.StorageNodes[idx].StorageNodeID

			snmd, err := snw.snmgr.GetMetadata(mctx, snid)
			if err != nil {
				snw.hbFail(snid)
				errs[idx] = err
				return
			}
			snw.hbReset(snid)
			snw.statsRepos.Report(mctx, snmd, time.Now().UTC())
		}(i)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (snw *StorageNodeWatcher) hbFail(snid types.StorageNodeID) {
	snw.mu.Lock()
	defer snw.mu.Unlock()
	snw.hbFailcnt[snid]++
}

func (snw *StorageNodeWatcher) hbReset(snid types.StorageNodeID) {
	snw.mu.Lock()
	defer snw.mu.Unlock()
	snw.hbFailcnt[snid] = 0
}

// reload resets the map lastTimes.
// The timestamp for pre-existing storage node is retained, on the other hand,
// the timestamp for a new one is set to the argument ts.
// If the storage node is not in the argument snds, it is removed from the
// lastTimes.
func (snw *StorageNodeWatcher) reload(snds []*varlogpb.StorageNodeDescriptor) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	lasthb := make(map[types.StorageNodeID]int, len(snds))
	for _, snd := range snds {
		snid := snd.StorageNodeID
		old, ok := snw.hbFailcnt[snid]
		if ok {
			lasthb[snid] = old
			continue
		}
		lasthb[snid] = 0
	}
	snw.hbFailcnt = lasthb
}

func (snw *StorageNodeWatcher) handleHeartbeatTimeout(ctx context.Context) {
	snw.mu.Lock()
	defer snw.mu.Unlock()

	var wg sync.WaitGroup
	for snid, cnt := range snw.hbFailcnt {
		if time.Duration(cnt)*snw.tick > snw.heartbeatTimeout {
			wg.Add(1)
			go func(ctx context.Context, snid types.StorageNodeID) {
				defer wg.Done()

				snw.eventHandler.HandleHeartbeatTimeout(ctx, snid)
			}(ctx, snid)
		}
	}
	wg.Wait()
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
	return errors.Join(errs...)
}
