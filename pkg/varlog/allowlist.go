package varlog

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// Allowlist represents selectable log streams.
type Allowlist interface {
	Pick() (types.LogStreamID, bool)
	Deny(logStreamID types.LogStreamID)
	Contains(logStreamID types.LogStreamID) bool
}

// RenewableAllowlist expands Allowlist and it provides Renew method to update allowlist.
type RenewableAllowlist interface {
	Allowlist
	Renewable
	io.Closer
}

const initialCacheSize = 32

type allowlistItem struct {
	denied bool
	ts     time.Time
}

// transientAllowlist provides allowlist and denylist of log streams. It can provide stale
// information.
type transientAllowlist struct {
	allowlist      sync.Map     // map[types.LogStreamID]allowlistItem
	cache          atomic.Value // []types.LogStreamID
	group          singleflight.Group
	denyTTL        time.Duration
	expireInterval time.Duration
	runner         *runner.Runner
	cancel         context.CancelFunc
	logger         *zap.Logger
}

var _ RenewableAllowlist = (*transientAllowlist)(nil)

func newTransientAllowlist(denyTTL time.Duration, expireInterval time.Duration, logger *zap.Logger) (*transientAllowlist, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("denylist")

	adl := &transientAllowlist{
		denyTTL:        denyTTL,
		expireInterval: expireInterval,
		runner:         runner.New("denylist", logger),
		logger:         logger,
	}
	adl.cache.Store(make([]types.LogStreamID, 0, initialCacheSize))
	cancel, err := adl.runner.Run(adl.expireDenyTTL)
	if err != nil {
		adl.runner.Stop()
		return nil, err
	}
	adl.cancel = cancel
	return adl, nil
}

func (adl *transientAllowlist) Close() error {
	adl.cancel()
	adl.runner.Stop()
	return nil
}

func (adl *transientAllowlist) expireDenyTTL(ctx context.Context) {
	tick := time.NewTicker(adl.expireInterval)
	defer tick.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			changed := false
			adl.allowlist.Range(func(k, v interface{}) bool {
				logStreamID := k.(types.LogStreamID)
				item := v.(allowlistItem)

				if !item.denied {
					return true
				}

				if time.Since(item.ts) >= adl.denyTTL {
					item.denied = false
					adl.allowlist.Store(logStreamID, item)
					changed = true
				}
				return true
			})
			if changed {
				adl.warmup()
			}

		}
	}
}

func (adl *transientAllowlist) warmup() {
	adl.group.Do("warmup", func() (interface{}, error) {
		oldCache := adl.cache.Load().([]types.LogStreamID)
		newCache := make([]types.LogStreamID, 0, cap(oldCache))
		adl.allowlist.Range(func(k, v interface{}) bool {
			logStreamID := k.(types.LogStreamID)
			item := v.(allowlistItem)
			if !item.denied {
				newCache = append(newCache, logStreamID)
			}
			return true
		})
		adl.cache.Store(newCache)
		return nil, nil
	})
}

func (adl *transientAllowlist) Pick() (types.LogStreamID, bool) {
	cache := adl.cache.Load().([]types.LogStreamID)
	cacheLen := len(cache)
	if cacheLen == 0 {
		return 0, false
	}
	idx := rand.Intn(cacheLen)
	return cache[idx], true
}

func (adl *transientAllowlist) Deny(logStreamID types.LogStreamID) {
	item := allowlistItem{denied: true, ts: time.Now()}
	// NB: Storing denied LogStreamID without any checking may result in saving unknown
	// LogStreamID. But it can be deleted by Renew.
	adl.allowlist.Store(logStreamID, item)
	adl.warmup()
}

func (adl *transientAllowlist) Contains(logStreamID types.LogStreamID) bool {
	item, ok := adl.allowlist.Load(logStreamID)
	return ok && !item.(allowlistItem).denied
}

func (adl *transientAllowlist) Renew(metadata *varlogpb.MetadataDescriptor) {
	lsdescs := metadata.GetLogStreams()
	recentLSIDs := set.New(len(lsdescs))
	for _, lsdesc := range lsdescs {
		recentLSIDs.Add(lsdesc.GetLogStreamID())
	}

	changed := false
	adl.allowlist.Range(func(k, v interface{}) bool {
		logStreamID := k.(types.LogStreamID)
		item := v.(allowlistItem)
		if !recentLSIDs.Contains(logStreamID) {
			adl.allowlist.Delete(logStreamID)
			changed = changed || !item.denied
		}
		return true
	})

	now := time.Now()
	for logStreamID := range recentLSIDs {
		aitem := allowlistItem{denied: false, ts: now}
		_, loaded := adl.allowlist.LoadOrStore(logStreamID, aitem)
		changed = changed || !loaded
	}

	if changed {
		adl.warmup()
	}
}
