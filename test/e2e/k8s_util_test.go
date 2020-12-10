// +build e2e

package e2e

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

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
