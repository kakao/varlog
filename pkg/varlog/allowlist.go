package varlog

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/varlogpb"
)

// Allowlist represents selectable log streams.
type Allowlist interface {
	Pick(topicID types.TopicID) (types.LogStreamID, bool)
	Deny(topicID types.TopicID, logStreamID types.LogStreamID)
	Contains(topicID types.TopicID, logStreamID types.LogStreamID) bool
}

// RenewableAllowlist expands Allowlist and it provides Renew method to update allowlist.
type RenewableAllowlist interface {
	Allowlist
	Renewable
	io.Closer
}

type allowlistItem struct {
	denied bool
	ts     time.Time
}

// transientAllowlist provides allowlist and denylist of log streams. It can provide stale
// information.
type transientAllowlist struct {
	allowlist      sync.Map // map[types.TopicID]map[types.LogStreamID]allowlistItem
	cache          sync.Map // map[types.TopicID][]types.LogStreamID
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
				lsMap := v.(*sync.Map)

				lsMap.Range(func(k, v interface{}) bool {
					logStreamID := k.(types.LogStreamID)
					item := v.(allowlistItem)

					if !item.denied {
						return true
					}

					if time.Since(item.ts) >= adl.denyTTL {
						item.denied = false
						lsMap.Store(logStreamID, item)
						changed = true
					}
					return true
				})
				return true
			})
			if changed {
				adl.warmup()
			}
		}
	}
}

func (adl *transientAllowlist) warmup() {
	_, _, _ = adl.group.Do("warmup", func() (interface{}, error) {
		adl.allowlist.Range(func(k, v interface{}) bool {
			topicID := k.(types.TopicID)
			lsMap := v.(*sync.Map)

			cacheCap := 0
			oldCache, ok := adl.cache.Load(topicID)
			if ok {
				cacheCap = cap(oldCache.([]types.LogStreamID))
			}
			newCache := make([]types.LogStreamID, 0, cacheCap)

			lsMap.Range(func(lsidIf, itemIf interface{}) bool {
				logStreamID := lsidIf.(types.LogStreamID)
				item := itemIf.(allowlistItem)

				if !item.denied {
					newCache = append(newCache, logStreamID)
				}

				return true
			})

			adl.cache.Store(topicID, newCache)
			return true
		})
		return nil, nil
	})
}

func (adl *transientAllowlist) Pick(topicID types.TopicID) (types.LogStreamID, bool) {
	cacheIf, ok := adl.cache.Load(topicID)
	if !ok {
		return 0, false
	}

	cache := cacheIf.([]types.LogStreamID)
	cacheLen := len(cache)
	if cacheLen == 0 {
		return 0, false
	}
	idx := rand.Intn(cacheLen)
	return cache[idx], true
}

func (adl *transientAllowlist) Deny(topicID types.TopicID, logStreamID types.LogStreamID) {
	item := allowlistItem{denied: true, ts: time.Now()}
	// NB: Storing denied LogStreamID without any checking may result in saving unknown
	// LogStreamID. But it can be deleted by Renew.
	lsMapIf, ok := adl.allowlist.Load(topicID)
	if !ok {
		lsMap := new(sync.Map)
		lsMap.Store(logStreamID, item)
		adl.allowlist.Store(topicID, lsMap)
	} else {
		lsMap := lsMapIf.(*sync.Map)
		lsMap.Store(logStreamID, item)
	}

	adl.warmup()
}

func (adl *transientAllowlist) Contains(topicID types.TopicID, logStreamID types.LogStreamID) bool {
	lsMapIf, ok := adl.allowlist.Load(topicID)
	if !ok {
		return false
	}

	lsMap := lsMapIf.(*sync.Map)
	item, ok := lsMap.Load(logStreamID)
	return ok && !item.(allowlistItem).denied
}

func (adl *transientAllowlist) Renew(metadata *varlogpb.MetadataDescriptor) {
	lsdescs := metadata.GetLogStreams()
	topicdescs := metadata.GetTopics()
	recentLSIDs := set.New(len(lsdescs))
	recentTopicIDs := set.New(len(topicdescs))

	for _, topicdesc := range topicdescs {
		recentTopicIDs.Add(topicdesc.GetTopicID())
		for _, lsid := range topicdesc.GetLogStreams() {
			recentLSIDs.Add(lsid)
		}
	}

	changed := false
	adl.allowlist.Range(func(k, v interface{}) bool {
		topicID := k.(types.TopicID)
		lsMap := v.(*sync.Map)

		if !recentTopicIDs.Contains(topicID) {
			adl.allowlist.Delete(topicID)
			changed = true
			return true
		}

		lsMap.Range(func(k, v interface{}) bool {
			logStreamID := k.(types.LogStreamID)
			item := v.(allowlistItem)
			if !recentLSIDs.Contains(logStreamID) {
				lsMap.Delete(logStreamID)
				changed = changed || !item.denied
			}
			return true
		})

		return true
	})

	now := time.Now()
	for _, topicdesc := range topicdescs {
		lsMap := new(sync.Map)
		lsMapIf, loaded := adl.allowlist.LoadOrStore(topicdesc.TopicID, lsMap)
		changed = changed || !loaded

		if loaded {
			lsMap = lsMapIf.(*sync.Map)
		}

		for _, logStreamID := range topicdesc.GetLogStreams() {
			aitem := allowlistItem{denied: false, ts: now}
			_, loaded := lsMap.LoadOrStore(logStreamID, aitem)
			changed = changed || !loaded
		}
	}

	if changed {
		adl.warmup()
	}
}
