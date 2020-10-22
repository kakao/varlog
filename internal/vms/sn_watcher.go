package vms

import (
	"context"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"

	"go.uber.org/zap"
)

type StorageNodeWatcher interface {
	Run() error
	Close() error
}

const WATCHER_RPC_TIMEOUT = 5 * time.Millisecond

var _ StorageNodeWatcher = (*snWatcher)(nil)

type snWatcher struct {
	WatcherOptions

	cmView    ClusterMetadataView
	snMgr     StorageNodeManager
	snHandler StorageNodeEventHandler

	hb map[types.StorageNodeID]time.Time
	mu sync.RWMutex

	runner *runner.Runner

	logger *zap.Logger
}

func NewStorageNodeWatcher(opts WatcherOptions, cmView ClusterMetadataView, snMgr StorageNodeManager, snHandler StorageNodeEventHandler, logger *zap.Logger) StorageNodeWatcher {
	return &snWatcher{
		WatcherOptions: opts,
		cmView:         cmView,
		snMgr:          snMgr,
		snHandler:      snHandler,
		hb:             make(map[types.StorageNodeID]time.Time),
		runner:         runner.New("wt", logger),
		logger:         logger,
	}
}

func (w *snWatcher) Run() error {
	if _, err := w.runner.Run(w.run); err != nil {
		return err
	}

	return nil
}

func (w *snWatcher) Close() error {
	w.runner.Stop()
	return nil
}

func (w *snWatcher) run(ctx context.Context) {
	ticker := time.NewTicker(w.Tick)
	defer ticker.Stop()

	reportInterval := w.ReportInterval

Loop:
	for {
		select {
		case <-ticker.C:
			w.heartbeat()

			reportInterval--
			if reportInterval == 0 {
				w.report()
				reportInterval = w.ReportInterval
			}

		case <-ctx.Done():
			break Loop
		}
	}
}

func (w *snWatcher) heartbeat() {
	ctx, cancel := context.WithTimeout(context.Background(), WATCHER_RPC_TIMEOUT)
	defer cancel()
	meta, err := w.cmView.ClusterMetadata(ctx)
	if err != nil {
		w.logger.Warn("snWatcher: get ClusterMetadata fail", zap.String("err", err.Error()))
		return
	}

	w.reload(meta.GetStorageNodes())

	//TODO:: make it parallel
	for _, s := range meta.GetStorageNodes() {
		ctx, cancel := context.WithTimeout(context.Background(), WATCHER_RPC_TIMEOUT)
		defer cancel()

		// NOTE: Missing a storage node triggers refreshes of storage node manager, which
		// can be a somewhat slow job.
		_, err := w.snMgr.GetMetadata(ctx, s.StorageNodeID)
		if err != nil {
			continue
		}

		w.set(s.StorageNodeID)
	}

	w.handleHeartbeat()
}

func (w *snWatcher) set(snID types.StorageNodeID) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.hb[snID] = time.Now()
}

func (w *snWatcher) reload(ss []*varlogpb.StorageNodeDescriptor) {
	w.mu.Lock()
	defer w.mu.Unlock()

	hb := make(map[types.StorageNodeID]time.Time)

	for _, s := range ss {
		hb[s.StorageNodeID] = time.Now()
	}

	for snID, t := range w.hb {
		if _, ok := hb[snID]; ok {
			hb[snID] = t
		}
	}

	w.hb = hb
}

func (w *snWatcher) handleHeartbeat() {
	w.mu.Lock()
	defer w.mu.Unlock()

	cur := time.Now()
	for snID, t := range w.hb {
		if cur.Sub(t) > w.Tick*time.Duration(w.HeartbeatTimeout) {
			w.snHandler.HandleHeartbeatTimeout(snID)
			w.hb[snID] = time.Now()
		}
	}
}

func (w *snWatcher) report() {
	ctx, cancel := context.WithTimeout(context.Background(), WATCHER_RPC_TIMEOUT)
	defer cancel()
	meta, err := w.cmView.ClusterMetadata(ctx)
	if err != nil {
		w.logger.Warn("snWatcher: get ClusterMetadata fail", zap.String("err", err.Error()))
		return
	}

	//TODO:: make it parallel
	for _, s := range meta.GetStorageNodes() {
		sc := w.snMgr.FindByStorageNodeID(s.StorageNodeID)

		ctx, cancel := context.WithTimeout(context.Background(), WATCHER_RPC_TIMEOUT)
		defer cancel()
		sn, err := sc.GetMetadata(ctx, snpb.MetadataTypeLogStreams)
		if err != nil {
			continue
		}

		w.snHandler.HandleReport(sn)
	}
}
