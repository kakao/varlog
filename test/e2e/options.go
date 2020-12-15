package e2e

import "fmt"

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
