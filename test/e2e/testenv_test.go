package e2e

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
)

const (
	E2E_MATERURL = "master-url"
	E2E_CLUSTER  = "cluster"
	E2E_CONTEXT  = "context"
	E2E_USER     = "user"
	E2E_TOKEN    = "token"
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

	t.Log(addr)

	k8s.IsVMSRunning()
}

func TestK8sVarlogRestart(t *testing.T) {
	Convey("Given K8s Varlog cluster", t, func(ctx C) {
		opts := getK8sVarlogClusterOpts()
		k8s, err := NewK8sVarlogCluster(opts)
		So(err, ShouldBeNil)

		mrs, err := k8s.GetMRs()
		So(err, ShouldBeNil)
		So(len(mrs), ShouldBeGreaterThan, 0)

		sns, err := k8s.GetSNs()
		So(err, ShouldBeNil)
		So(len(sns), ShouldBeGreaterThan, 0)

		Convey("When StopAll", func(ctx C) {
			err := k8s.StopAll()
			So(err, ShouldBeNil)

			Convey("Then, there are no alive nodes", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					mrs, err := k8s.GetMRs()
					if err != nil {
						return false
					}
					return len(mrs) == 0
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(50, func() bool {
					sns, err := k8s.GetSNs()
					if err != nil {
						return false
					}
					return len(sns) == 0
				}), ShouldBeTrue)

				Convey("When Start Again, it should make cluster well", func(ctx C) {
					err := k8s.StartMRs()
					So(err, ShouldBeNil)
					So(testutil.CompareWaitN(50, func() bool {
						mrs, err := k8s.GetMRs()
						if err != nil {
							return false
						}
						return len(mrs) > 0
					}), ShouldBeTrue)

					err = k8s.StartVMS()
					So(testutil.CompareWaitN(50, func() bool {
						ok, err := k8s.IsVMSRunning()
						if err != nil {
							return false
						}

						return ok
					}), ShouldBeTrue)
					So(err, ShouldBeNil)

					err = k8s.StartSNs()
					So(err, ShouldBeNil)
					So(testutil.CompareWaitN(50, func() bool {
						sns, err := k8s.GetSNs()
						if err != nil {
							return false
						}
						return len(sns) > 0
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(50, func() bool {
						ok, err := k8s.IsMRRunning()
						if err != nil {
							return false
						}
						return ok
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(50, func() bool {
						ok, err := k8s.IsSNRunning()
						if err != nil {
							return false
						}
						return ok
					}), ShouldBeTrue)
				})
			})
		})
	})
}
