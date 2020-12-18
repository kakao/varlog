package e2e

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	vtypes "github.com/kakao/varlog/pkg/types"
)

const (
	E2E_MASTERURL = "master-url"
	E2E_CLUSTER   = "cluster"
	E2E_CONTEXT   = "context"
	E2E_USER      = "user"
	E2E_TOKEN     = "token"

	DEFAULT_MR_CNT     = 3
	DEFAULT_SN_CNT     = 3
	DEFAULT_LS_CNT     = 2
	DEFAULT_REP_FACTOR = 3

	defaultClientCnt     = 10
	defaultSubscriberCnt = 10
)

type K8sVarlogClusterOptions struct {
	MasterUrl string
	User      string
	Token     string
	Cluster   string
	Context   string
	NrMR      int
	NrSN      int
	NrLS      int
	RepFactor int
	Reset     bool
}

func getK8sVarlogClusterOpts() K8sVarlogClusterOptions {
	info, err := getVarlogK8sConnInfo()
	if err != nil {
		return K8sVarlogClusterOptions{}
	}

	opts := K8sVarlogClusterOptions{}
	if f, ok := info[E2E_MASTERURL]; ok {
		opts.MasterUrl = f.(string)
	}

	if f, ok := info[E2E_CLUSTER]; ok {
		opts.Cluster = f.(string)
	}

	if f, ok := info[E2E_CONTEXT]; ok {
		opts.Context = f.(string)
	}

	if f, ok := info[E2E_USER]; ok {
		opts.User = f.(string)
	}

	if f, ok := info[E2E_TOKEN]; ok {
		opts.Token = f.(string)
	}

	opts.NrMR = DEFAULT_MR_CNT
	opts.NrSN = DEFAULT_SN_CNT
	opts.NrLS = DEFAULT_LS_CNT
	opts.RepFactor = DEFAULT_REP_FACTOR
	opts.Reset = true

	return opts
}

func optsToConfigBytes(opts K8sVarlogClusterOptions) []byte {
	return []byte(fmt.Sprintf("apiVersion: v1\n"+
		"clusters:\n"+
		"- cluster:\n"+
		"    insecure-skip-tls-verify: true\n"+
		"    server: %s\n"+
		"  name: %s\n"+
		"contexts:\n"+
		"- context:\n"+
		"    cluster: %s\n"+
		"    user: %s\n"+
		"  name: %s\n"+
		"current-context: %s\n"+
		"kind: Config\n"+
		"preferences: {}\n"+
		"users:\n"+
		"- name: %s\n"+
		"  user:\n"+
		"    token: %s",
		opts.MasterUrl,
		opts.Cluster,
		opts.Cluster,
		opts.User,
		opts.Context,
		opts.Context,
		opts.User,
		opts.Token))
}

type actionOptions struct {
	title       string
	prevf       func() error
	postf       func() error
	clusterID   vtypes.ClusterID
	mrAddr      string
	nrCli       int
	nrSub       int
	confChanger ConfChanger
	logger      *zap.Logger
}

type confChangerOptions struct {
	change       func() error
	check        func() error
	recover      func() error
	recoverCheck func() error
	interval     time.Duration
}

var defaultActionOptions = actionOptions{
	nrCli:  defaultClientCnt,
	nrSub:  defaultSubscriberCnt,
	logger: zap.NewNop(),
}

var defaultConfChangerOptions = confChangerOptions{
	change:       func() error { return nil },
	check:        func() error { return nil },
	recover:      func() error { return nil },
	recoverCheck: func() error { return nil },
}

type ActionOption func(*actionOptions)

type ConfChangerOption func(*confChangerOptions)

func WithTitle(title string) ActionOption {
	return func(opts *actionOptions) {
		opts.title = title
	}
}

func WithNumClient(n int) ActionOption {
	return func(opts *actionOptions) {
		opts.nrCli = n
	}
}

func WithNumSubscriber(n int) ActionOption {
	return func(opts *actionOptions) {
		opts.nrSub = n
	}
}

func WithPrevFunc(pf func() error) ActionOption {
	return func(opts *actionOptions) {
		opts.prevf = pf
	}
}

func WithPostFunc(pf func() error) ActionOption {
	return func(opts *actionOptions) {
		opts.postf = pf
	}
}

func WithLogger(logger *zap.Logger) ActionOption {
	return func(opts *actionOptions) {
		opts.logger = logger
	}
}

func WithConfChange(cc ConfChanger) ActionOption {
	return func(opts *actionOptions) {
		opts.confChanger = cc
	}
}

func WithClusterID(cid vtypes.ClusterID) ActionOption {
	return func(opts *actionOptions) {
		opts.clusterID = cid
	}
}

func WithMRAddr(addr string) ActionOption {
	return func(opts *actionOptions) {
		opts.mrAddr = addr
	}
}

func WithChangeFunc(f func() error) ConfChangerOption {
	return func(opts *confChangerOptions) {
		opts.change = f
	}
}

func WithCheckFunc(f func() error) ConfChangerOption {
	return func(opts *confChangerOptions) {
		opts.check = f
	}
}

func WithRecoverFunc(f func() error) ConfChangerOption {
	return func(opts *confChangerOptions) {
		opts.recover = f
	}
}

func WithRecoverCheckFunc(f func() error) ConfChangerOption {
	return func(opts *confChangerOptions) {
		opts.recoverCheck = f
	}
}

func WithConfChangeInterval(dur time.Duration) ConfChangerOption {
	return func(opts *confChangerOptions) {
		opts.interval = dur
	}
}
