package cluster

import (
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/tests/ee/k8s/client"
	"github.com/kakao/varlog/tests/ee/k8s/cluster/nodelabel"
	"github.com/kakao/varlog/tests/ee/k8s/cluster/podlabel"
)

const (
	defaultVarlogNamespace          = "default"
	defaultDeploymentNameAdmin      = "varlogadm"
	defaultStatefulSetNameMetaRepos = "varlogmr"

	namespaceIngressNginx       = "ingress-nginx"
	defaultServiceNameAdmin     = "varlogadm-rpc-vip"
	defaultServiceNameMetaRepos = "varlogmr-rpc-vip"
)

const (
	defaultNumMetaRepos           = 3
	defaultAsyncTestWaitDuration  = 10 * time.Minute
	defaultAsyncTestCheckInterval = 10 * time.Second
)

type config struct {
	c *client.Client

	nodeGroupLabel       string
	nodeGroupAdmin       string
	nodeGroupMetaRepos   string
	nodeGroupStorageNode string

	nodeStatusLabelStorageNode string
	nodeStatusLabelMetaRepos   string
	nodeStatusStarted          string
	nodeStatusStopped          string
	nodeStatusCleared          string

	varlogNamespace                   string
	deploymentNameAdmin               string
	statefulSetNameMetadataRepository string

	appSelectorAdmin            map[string]string
	appSelectorMetaRepos        map[string]string
	appSelectorMetaReposClear   map[string]string
	appSelectorStorageNode      map[string]string
	appSelectorStorageNodeClear map[string]string

	ingressNamespace     string
	serviceNameAdmin     string
	serviceNameMetaRepos string

	numMetaRepos    int32
	numStorageNodes int32

	asyncTestWaitDuration  time.Duration
	asyncTestCheckInterval time.Duration

	logger *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		nodeGroupLabel:       nodelabel.DefaultNodeGroupLabel,
		nodeGroupAdmin:       nodelabel.DefaultNodeGroupAdmin,
		nodeGroupMetaRepos:   nodelabel.DefaultNodeGroupMetaRepos,
		nodeGroupStorageNode: nodelabel.DefaultNodeGroupStorageNode,

		nodeStatusLabelStorageNode: nodelabel.DefaultNodeStatusLabelStorageNode,
		nodeStatusLabelMetaRepos:   nodelabel.DefaultNodeStatusLabelMetaRepos,
		nodeStatusStarted:          nodelabel.DefaultNodeStatusStarted,
		nodeStatusStopped:          nodelabel.DefaultNodeStatusStopped,
		nodeStatusCleared:          nodelabel.DefaultNodeStatusCleared,

		varlogNamespace:                   defaultVarlogNamespace,
		deploymentNameAdmin:               defaultDeploymentNameAdmin,
		statefulSetNameMetadataRepository: defaultStatefulSetNameMetaRepos,

		appSelectorAdmin:            podlabel.DefaultAppSelectorAdmin(),
		appSelectorMetaRepos:        podlabel.DefaultAppSelectorMetaRepos(),
		appSelectorMetaReposClear:   podlabel.DefaultAppSelectorMetaReposClear(),
		appSelectorStorageNode:      podlabel.DefaultAppSelectorStorageNode(),
		appSelectorStorageNodeClear: podlabel.DefaultAppSelectorStorageNodeClear(),

		ingressNamespace:     namespaceIngressNginx,
		serviceNameAdmin:     defaultServiceNameAdmin,
		serviceNameMetaRepos: defaultServiceNameMetaRepos,

		numMetaRepos:           defaultNumMetaRepos,
		asyncTestWaitDuration:  defaultAsyncTestWaitDuration,
		asyncTestCheckInterval: defaultAsyncTestCheckInterval,
		logger:                 zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) validate() error {
	if cfg.c == nil {
		return errors.New("k8s cluster: invalid client")
	}
	if len(cfg.varlogNamespace) == 0 {
		return errors.New("k8s cluster: no varlog namespace")
	}
	if len(cfg.ingressNamespace) == 0 {
		return errors.New("k8s cluster: no ingress namespace")
	}
	if cfg.numMetaRepos < 1 {
		return errors.New("k8s cluster: no metadata repository node")
	}
	if cfg.numStorageNodes < 0 {
		return errors.New("k8s cluster: invalid number of storage nodes")
	}
	if cfg.logger == nil {
		return errors.New("k8s cluster: no logger")
	}
	return nil
}

type Option interface {
	apply(*config)
}

type funcOption struct {
	f func(*config)
}

func newFuncOption(f func(*config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *config) {
	fo.f(cfg)
}

func WithClient(c *client.Client) Option {
	return newFuncOption(func(cfg *config) {
		cfg.c = c
	})
}

func WithVarlogNamespace(varlogNamespace string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.varlogNamespace = varlogNamespace
	})
}

func WithIngressNamespace(ingressNamespace string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.ingressNamespace = ingressNamespace
	})
}

func WithAdminServiceResourceName(serviceName string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.serviceNameAdmin = serviceName
	})
}

func WithMetadataRepositoryServiceResourceName(serviceName string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.serviceNameMetaRepos = serviceName
	})
}

func NumMetadataRepositoryNodes(numMetaReposNodes int32) Option {
	return newFuncOption(func(cfg *config) {
		cfg.numMetaRepos = numMetaReposNodes
	})
}

func NumStorageNodes(numStorageNodes int32) Option {
	return newFuncOption(func(cfg *config) {
		cfg.numStorageNodes = numStorageNodes
	})
}

func WithAsyncTestWaitDuration(asyncTestWaitDuration time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.asyncTestWaitDuration = asyncTestWaitDuration
	})
}

func WithAsyncTestCheckInterval(asyncTestCheckInterval time.Duration) Option {
	return newFuncOption(func(cfg *config) {
		cfg.asyncTestCheckInterval = asyncTestCheckInterval
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
