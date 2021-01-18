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
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
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

const (
	expireDenyTTLTick = 10 * time.Second
)

type allowlistItem struct {
	denied bool
	ts     time.Time
}

// transientAllowlist provides allowlist and denylist of log streams. It can provide stale
// information.
type transientAllowlist struct {
	allowlist sync.Map     // map[types.LogStreamID]allowlistItem
	cache     atomic.Value // []types.LogStreamID
	group     singleflight.Group
	dirty     atomicutil.AtomicBool
	denyTTL   time.Duration
	runner    *runner.Runner
	cancel    context.CancelFunc
	logger    *zap.Logger
}

var _ RenewableAllowlist = (*transientAllowlist)(nil)

func newTransientAllowlist(denyTTL time.Duration, logger *zap.Logger) (*transientAllowlist, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("denylist")

	adl := &transientAllowlist{
		denyTTL: denyTTL,
		runner:  runner.New("denylist", logger),
		logger:  logger,
	}
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
	tick := time.NewTicker(expireDenyTTLTick)
	defer tick.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			changed := false
			adl.allowlist.Range(func(logStreamID interface{}, aitem interface{}) bool {
				item := aitem.(allowlistItem)
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
			adl.dirty.Store(changed)
		}
	}
}

func (adl *transientAllowlist) pick() (types.LogStreamID, bool) {
	cacheIf := adl.cache.Load()
	if cacheIf == nil {
		return 0, false
	}

	cache := cacheIf.([]types.LogStreamID)
	l := len(cache)
	if l > 0 {
		idx := rand.Intn(l)
		return cache[idx], true
	}

	return 0, false
}

func (adl *transientAllowlist) warmup() {
	adl.group.Do("warmup", func() (interface{}, error) {
		lenHint := 0
		cacheIf := adl.cache.Load()
		if cacheIf != nil {
			lenHint = len(cacheIf.([]types.LogStreamID))
		}
		cache := make([]types.LogStreamID, 0, lenHint)
		adl.allowlist.Range(func(logStreamID interface{}, aitem interface{}) bool {
			item := aitem.(allowlistItem)
			if !item.denied {
				cache = append(cache, logStreamID.(types.LogStreamID))
			}
			return true
		})
		adl.cache.Store(cache)
		adl.dirty.Store(false)
		return nil, nil
	})
}

func (adl *transientAllowlist) Pick() (types.LogStreamID, bool) {
	if !adl.dirty.Load() {
		return adl.pick()
	}
	adl.warmup()
	return adl.pick()
}

func (adl *transientAllowlist) Deny(logStreamID types.LogStreamID) {
	if aitem, loaded := adl.allowlist.LoadAndDelete(logStreamID); loaded {
		item := aitem.(allowlistItem)
		item.denied = true
		item.ts = time.Now()
		adl.allowlist.Store(logStreamID, item)
		adl.dirty.Store(true)
	}
}

func (adl *transientAllowlist) Contains(logStreamID types.LogStreamID) bool {
	info, ok := adl.allowlist.Load(logStreamID)
	return ok && !info.(allowlistItem).denied
}

func (adl *transientAllowlist) Renew(metadata *varlogpb.MetadataDescriptor) {
	lsdescs := metadata.GetLogStreams()
	recentLSIDs := set.New(len(lsdescs))
	for _, lsdesc := range lsdescs {
		recentLSIDs.Add(lsdesc.GetLogStreamID())
	}

	changed := false

	adl.allowlist.Range(func(logStreamID interface{}, aitem interface{}) bool {
		if !recentLSIDs.Contains(logStreamID.(types.LogStreamID)) {
			adl.allowlist.Delete(logStreamID)
			changed = changed || !aitem.(allowlistItem).denied
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
