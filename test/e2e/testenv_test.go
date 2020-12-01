// +build e2e

package e2e

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	E2E_MATERURL = "master-url"
	E2E_CLUSTER  = "cluster"
	E2E_CONTEXT  = "context"
	E2E_USER     = "user"
	E2E_TOKEN    = "token"

	DEFAULT_MR_CNT = 3
	DEFAULT_SN_CNT = 5
)

func getK8sVarlogClusterOpts() K8sVarlogClusterOptions {
	info, err := getVarlogK8sConnInfo()
	if err != nil {
		return K8sVarlogClusterOptions{}
	}

	opts := K8sVarlogClusterOptions{}
	if f, ok := info[E2E_MATERURL]; ok {
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

	return opts
}

func TestK8sVarlogClient(t *testing.T) {
	opts := getK8sVarlogClusterOpts()
	k8s, err := NewK8sVarlogCluster(opts)
	if err != nil {
		t.Fatal(err)
	}

	addr, err := k8s.MRAddress()
	if err != nil {
		t.Fatal(err)
	}

	k8s.IsMRRunning()
	t.Log(addr)
}

func TestK8sVarlogReset(t *testing.T) {
	Convey("Given K8s Varlog cluster", t, func(ctx C) {
		opts := getK8sVarlogClusterOpts()
		k8s, err := NewK8sVarlogCluster(opts)
		So(err, ShouldBeNil)

		Convey("When Reset Cluster", func(ctx C) {
			err := k8s.Reset()
			So(err, ShouldBeNil)

			Convey("Then, Pods should be created", func(ctx C) {
				ok, err := k8s.IsMRRunning()
				So(err, ShouldBeNil)
				So(ok, ShouldBeTrue)

				ok, err = k8s.IsSNRunning()
				So(err, ShouldBeNil)
				So(ok, ShouldBeTrue)

				ok, err = k8s.IsVMSRunning()
				So(err, ShouldBeNil)
				So(ok, ShouldBeTrue)
			})
		})
	})
}
