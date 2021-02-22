package varlog

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/varlogpb"
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
	allowlist sync.Map // map[types.LogStreamID]allowlistItem
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
			adl.allowlist.Range(func(logStreamID interface{}, aitem interface{}) bool {
				item := aitem.(allowlistItem)
				if !item.denied {
					return true
				}
				if time.Since(item.ts) >= adl.denyTTL {
					item.denied = false
					adl.allowlist.Store(logStreamID, item)
				}
				return true
			})
		}
	}
}

func (adl *transientAllowlist) Pick() (types.LogStreamID, bool) {
	var allowlist []types.LogStreamID
	adl.allowlist.Range(func(lsidI interface{}, itemI interface{}) bool {
		item := itemI.(allowlistItem)
		if !item.denied {
			allowlist = append(allowlist, lsidI.(types.LogStreamID))
		}
		return true
	})
	l := len(allowlist)
	if l > 0 {
		idx := rand.Intn(l)
		return allowlist[idx], true
	}
	return 0, false
}

func (adl *transientAllowlist) Deny(logStreamID types.LogStreamID) {
	if aitem, loaded := adl.allowlist.LoadAndDelete(logStreamID); loaded {
		item := aitem.(allowlistItem)
		if item.denied {
			return
		}
		item.denied = true
		item.ts = time.Now()
		adl.allowlist.Store(logStreamID, item)
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

	adl.allowlist.Range(func(logStreamID interface{}, aitem interface{}) bool {
		if !recentLSIDs.Contains(logStreamID.(types.LogStreamID)) {
			adl.allowlist.Delete(logStreamID)
		}
		return true
	})

	now := time.Now()
	for logStreamID := range recentLSIDs {
		aitem := allowlistItem{denied: false, ts: now}
		adl.allowlist.LoadOrStore(logStreamID, aitem)
	}
}
