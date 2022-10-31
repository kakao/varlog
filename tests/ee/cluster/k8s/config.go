package k8s

import (
	"errors"
	"fmt"
	"time"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/tests/ee/cluster"
	"github.com/kakao/varlog/tests/ee/cluster/k8s/nodelabel"
	"github.com/kakao/varlog/tests/ee/cluster/k8s/podlabel"
)

const (
	clusterID = types.ClusterID(1)

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
	cluster.Config

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

	asyncTestWaitDuration  time.Duration
	asyncTestCheckInterval time.Duration
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

		asyncTestWaitDuration:  defaultAsyncTestWaitDuration,
		asyncTestCheckInterval: defaultAsyncTestCheckInterval,
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
	if cfg.ClusterID() != clusterID {
		return fmt.Errorf("k8s cluster: wrong cluster ID %v", cfg.ClusterID())
	}

	if len(cfg.varlogNamespace) == 0 {
		return errors.New("k8s cluster: no varlog namespace")
	}
	if len(cfg.ingressNamespace) == 0 {
		return errors.New("k8s cluster: no ingress namespace")
	}
	if cfg.NumMetaRepos() < 1 {
		return errors.New("k8s cluster: no metadata repository node")
	}
	if cfg.NumStorageNodes() < 0 {
		return errors.New("k8s cluster: invalid number of storage nodes")
	}
	if cfg.Logger() == nil {
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

func WithCommonConfig(commonConfig cluster.Config) Option {
	return newFuncOption(func(cfg *config) {
		cfg.Config = commonConfig
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
