package ee

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
)

const (
	// testClusterID must be 1 since it is fixed in k8s manifest.
	testClusterID = types.ClusterID(1)

	defaultNumAppendClients    = 10
	defaultNumSubscribeClients = 10
	defaultNumRepeats          = 1
)

type actionConfig struct {
	title               string
	preHook             func(*testing.T, *Action)
	postHook            func(*testing.T, *Action)
	clusterID           types.ClusterID
	mrAddr              string
	numAppendClients    int
	numSubscribeClients int
	numRepeats          int
	confChangers        []ConfChanger
	logger              *zap.Logger
}

func newActionConfig(t *testing.T, opts []ActionOption) actionConfig {
	t.Helper()
	cfg := actionConfig{
		clusterID:           testClusterID,
		numAppendClients:    defaultNumAppendClients,
		numSubscribeClients: defaultNumSubscribeClients,
		numRepeats:          defaultNumRepeats,
		logger:              zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyAction(&cfg)
	}
	cfg.validate(t)
	cfg.logger = cfg.logger.Named(cfg.title)
	return cfg
}

func (cfg *actionConfig) validate(t *testing.T) {
	t.Helper()
	assert.NotEmpty(t, cfg.title)
	assert.GreaterOrEqual(t, cfg.numAppendClients, 0)
	assert.GreaterOrEqual(t, cfg.numSubscribeClients, 0)
}

type ActionOption interface {
	applyAction(*actionConfig)
}

type funcActionOption struct {
	f func(*actionConfig)
}

func newFuncActionOption(f func(*actionConfig)) *funcActionOption {
	return &funcActionOption{f: f}
}

func (fao *funcActionOption) applyAction(cfg *actionConfig) {
	fao.f(cfg)
}

func WithTitle(title string) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.title = title
	})
}

func WithNumRepeats(numRepeats int) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.numRepeats = numRepeats
	})
}

func WithNumClient(nrCli int) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.numAppendClients = nrCli
	})
}

func WithNumSubscriber(nrSub int) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.numSubscribeClients = nrSub
	})
}

func WithPreHook(preHook func(*testing.T, *Action)) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.preHook = preHook
	})
}

func WithPostHook(postHook func(*testing.T, *Action)) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.postHook = postHook
	})
}

type LoggerOption interface {
	ActionOption
	ConfChangerOption
}

type loggerOption struct {
	logger *zap.Logger
}

func (lo *loggerOption) applyAction(cfg *actionConfig) {
	cfg.logger = lo.logger
}

func (lo *loggerOption) applyConfChanger(cfg *confChangerConfig) {
	cfg.logger = lo.logger
}

func WithLogger(logger *zap.Logger) LoggerOption {
	return &loggerOption{logger: logger}
}

func WithConfChange(confChanger ConfChanger) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.confChangers = append(cfg.confChangers, confChanger)
	})
}

func WithClusterID(cid types.ClusterID) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.clusterID = cid
	})
}

func WithMRAddr(mrAddr string) ActionOption {
	return newFuncActionOption(func(cfg *actionConfig) {
		cfg.mrAddr = mrAddr
	})
}

const (
	defaultConfChangerInterval = 10 * time.Second
)

type confChangerConfig struct {
	name       string
	changeFunc func(context.Context, *testing.T) bool
	checkFunc  func(context.Context, *testing.T) bool
	interval   time.Duration
	logger     *zap.Logger
}

func newConfChangerConfig(opts []ConfChangerOption) confChangerConfig {
	cfg := confChangerConfig{
		changeFunc: func(context.Context, *testing.T) bool { return true },
		checkFunc:  func(context.Context, *testing.T) bool { return true },
		interval:   defaultConfChangerInterval,
		logger:     zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyConfChanger(&cfg)
	}
	if len(cfg.name) == 0 {
		cfg.name = testutil.GetFunctionName(cfg.changeFunc)
	}
	cfg.logger = cfg.logger.Named(cfg.name)
	return cfg
}

type ConfChangerOption interface {
	applyConfChanger(*confChangerConfig)
}

type funcConfChangerOption struct {
	f func(*confChangerConfig)
}

func newFuncConfChangerOption(f func(*confChangerConfig)) *funcConfChangerOption {
	return &funcConfChangerOption{f: f}
}

func (fcco *funcConfChangerOption) applyConfChanger(cfg *confChangerConfig) {
	fcco.f(cfg)
}

func WithName(name string) ConfChangerOption {
	return newFuncConfChangerOption(func(cfg *confChangerConfig) {
		cfg.name = name
	})
}

func WithChangeFunc(changeFunc func(context.Context, *testing.T) bool) ConfChangerOption {
	return newFuncConfChangerOption(func(cfg *confChangerConfig) {
		cfg.changeFunc = changeFunc
	})
}

func WithCheckFunc(checkFunc func(context.Context, *testing.T) bool) ConfChangerOption {
	return newFuncConfChangerOption(func(cfg *confChangerConfig) {
		cfg.checkFunc = checkFunc
	})
}

func WithConfChangeInterval(dur time.Duration) ConfChangerOption {
	return newFuncConfChangerOption(func(cfg *confChangerConfig) {
		cfg.interval = dur
	})
}
