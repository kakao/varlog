package ee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/util/testutil"
)

type ConfChanger interface {
	Do(context.Context, *testing.T) bool
	Name() string
}

type confChanger struct {
	confChangerConfig
}

func NewConfChanger(opts ...ConfChangerOption) ConfChanger {
	cfg := newConfChangerConfig(opts)
	return &confChanger{
		confChangerConfig: cfg,
	}
}

func (cc *confChanger) waitInterval(ctx context.Context, t *testing.T) bool {
	t.Helper()

	timer := time.NewTimer(cc.interval)
	defer timer.Stop()

	var err error
	select {
	case <-timer.C:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return assert.NoError(t, err)
}

func (cc *confChanger) Name() string {
	return cc.name
}

func (cc *confChanger) Do(ctx context.Context, t *testing.T) bool {
	t.Helper()

	cc.logger.Debug("waiting before calling changeFunc function", zap.Duration("interval", cc.interval))
	if !cc.waitInterval(ctx, t) {
		return false
	}

	cc.logger.Debug("calling changeFunc function", zap.String("name", testutil.GetFunctionName(cc.changeFunc)))
	if !cc.changeFunc(ctx, t) {
		return false
	}

	cc.logger.Debug("calling checkFunc function", zap.String("name", testutil.GetFunctionName(cc.checkFunc)))
	if !cc.checkFunc(ctx, t) {
		return false
	}

	cc.logger.Debug("waiting after calling checkFunc function", zap.Duration("interval", cc.interval))
	return cc.waitInterval(ctx, t)
}
