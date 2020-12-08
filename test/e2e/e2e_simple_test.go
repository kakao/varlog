// +build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	vtypes "github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
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
