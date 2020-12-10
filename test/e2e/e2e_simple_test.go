// +build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	vtypes "github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
)

func TestK8sVarlogSimple(t *testing.T) {
	opts := getK8sVarlogClusterOpts()

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mcli, err := varlog.NewClusterManagerClient(vmsaddr)
		So(err, ShouldBeNil)

		Reset(func() {
			mcli.Close()
		})

		Convey("AddLogStream - Simple", func() {
			r, err := mcli.AddLogStream(context.TODO(), nil)
			So(err, ShouldBeNil)

			lsID := r.LogStream.LogStreamID

			mrseed, err := k8s.MRAddress()
			So(err, ShouldBeNil)

			varlog, err := varlog.Open(vtypes.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
			So(err, ShouldBeNil)

			Reset(func() {
				varlog.Close()
			})

			glsn, err := varlog.Append(context.TODO(), []byte("foo"))
			So(err, ShouldBeNil)

			_, err = varlog.Read(context.TODO(), lsID, glsn)
			So(err, ShouldBeNil)

			Convey("Seal", func() {
				rsp, err := mcli.Seal(context.TODO(), lsID)
				So(err, ShouldBeNil)
				lsmetaList := rsp.GetLogStreams()
				So(len(lsmetaList), ShouldEqual, k8s.RepFactor)
				So(lsmetaList[0].HighWatermark, ShouldEqual, glsn)

				glsn, err := varlog.Append(context.TODO(), []byte("foo"))
				So(err, ShouldNotBeNil)
				So(glsn, ShouldEqual, vtypes.InvalidGLSN)

				Convey("Unseal", func() {
					_, err := mcli.Unseal(context.TODO(), lsID)
					So(err, ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						glsn, err := varlog.Append(context.TODO(), []byte("foo"))
						return err == nil && glsn > lsmetaList[0].HighWatermark
					}), ShouldBeTrue)
				})
			})
		})
	}))
}

func TestK8sVarlogFailoverMR(t *testing.T) {
	opts := getK8sVarlogClusterOpts()

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mcli, err := varlog.NewClusterManagerClient(vmsaddr)
		So(err, ShouldBeNil)

		Reset(func() {
			mcli.Close()
		})

		_, err = mcli.AddLogStream(context.TODO(), nil)
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		varlog, err := varlog.Open(vtypes.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
		So(err, ShouldBeNil)

		Reset(func() {
			varlog.Close()
		})

		_, err = varlog.Append(context.TODO(), []byte("foo"))
		So(err, ShouldBeNil)

		Convey("When MR follower fail", func() {
			members, err := mcli.GetMRMembers(context.TODO())
			So(err, ShouldBeNil)
			So(len(members.Members), ShouldEqual, opts.NrMR)

			for nodeID := range members.Members {
				if nodeID != members.Leader {
					err := k8s.StopMR(nodeID)
					So(err, ShouldBeNil)
					break
				}
			}

			Convey("Then it should be appendable", func() {
				for i := 0; i < 1000; i++ {
					_, err := varlog.Append(context.TODO(), []byte("foo"))
					So(err, ShouldBeNil)
				}

				Convey("When MR recover", func() {
					err := k8s.RecoverMR()
					So(err, ShouldBeNil)

					Convey("Then if should be appendable", func() {
						for i := 0; i < 1000; i++ {
							_, err := varlog.Append(context.TODO(), []byte("foo"))
							So(err, ShouldBeNil)
						}
					})
				})
			})
		})
	}))
}
