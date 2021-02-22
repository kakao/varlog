package vms

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/varlogpb"
)

type StorageNodeWatcher interface {
	Run() error
	Close() error
}

var _ StorageNodeWatcher = (*snWatcher)(nil)

type snWatcher struct {
	WatcherOptions

	cmView    ClusterMetadataView
	snMgr     StorageNodeManager
	snHandler StorageNodeEventHandler

	hb map[types.StorageNodeID]time.Time
	mu sync.RWMutex

	runner *runner.Runner
	cancel context.CancelFunc

	logger *zap.Logger
}

func NewStorageNodeWatcher(opts WatcherOptions, cmView ClusterMetadataView, snMgr StorageNodeManager, snHandler StorageNodeEventHandler, logger *zap.Logger) StorageNodeWatcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("snwatcher")
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
	cancel, err := w.runner.Run(w.run)
	if err != nil {
		return err
	}
	w.cancel = cancel
	return nil
}

func (w *snWatcher) Close() error {
	w.cancel()
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
			w.heartbeat(ctx)

			reportInterval--
			if reportInterval == 0 {
				w.report(ctx)
				reportInterval = w.ReportInterval
			}

		case <-ctx.Done():
			break Loop
		}
	}
}

func (w *snWatcher) heartbeat(c context.Context) {
	ctx, cancel := context.WithTimeout(c, w.RPCTimeout)
	defer cancel()
	meta, err := w.cmView.ClusterMetadata(ctx)
	if err != nil {
		w.logger.Warn("snWatcher: get ClusterMetadata fail", zap.String("err", err.Error()))
		return
	}

	w.reload(meta.GetStorageNodes())

	grp, gCtx := errgroup.WithContext(ctx)
	for idx := range meta.GetStorageNodes() {
		snd := meta.GetStorageNodes()[idx]
		grp.Go(func() error {
			storageNodeID := snd.GetStorageNodeID()
			if _, err := w.snMgr.GetMetadata(gCtx, storageNodeID); err == nil {
				w.set(storageNodeID)
			}
			return nil
		})
	}
	grp.Wait()
	w.handleHeartbeat(ctx)
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

func (w *snWatcher) handleHeartbeat(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cur := time.Now()
	for snID, t := range w.hb {
		if cur.Sub(t) > w.Tick*time.Duration(w.HeartbeatTimeout) {
			w.snHandler.HandleHeartbeatTimeout(ctx, snID)
			w.hb[snID] = time.Now()
		}
	}
}

func (w *snWatcher) report(c context.Context) {
	ctx, cancel := context.WithTimeout(c, w.RPCTimeout)
	defer cancel()

	meta, err := w.cmView.ClusterMetadata(ctx)
	if err != nil {
		w.logger.Warn("snWatcher: get ClusterMetadata fail", zap.String("err", err.Error()))
		return
	}

	grp, gCtx := errgroup.WithContext(ctx)
	for idx := range meta.GetStorageNodes() {
		snd := meta.GetStorageNodes()[idx]
		grp.Go(func() error {
			if snmd, err := w.snMgr.GetMetadata(gCtx, snd.GetStorageNodeID()); err == nil {
				w.snHandler.HandleReport(gCtx, snmd)
			}
			return nil
		})
	}
	grp.Wait()
}
