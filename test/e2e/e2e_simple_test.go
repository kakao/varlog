// +build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/kakao/varlog/pkg/mrc"
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

			Convey("Then it should be abel to append", func() {
				for i := 0; i < 1000; i++ {
					_, err := varlog.Append(context.TODO(), []byte("foo"))
					So(err, ShouldBeNil)
				}

				Convey("When MR recover", func() {
					err := k8s.RecoverMR()
					So(err, ShouldBeNil)

					Convey("Then if should be abel to append", func() {
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

func TestK8sVarlogFailoverSN(t *testing.T) {
	opts := getK8sVarlogClusterOpts()

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		mcli, err := varlog.NewClusterManagerClient(vmsaddr)
		So(err, ShouldBeNil)

		mrcli, err := mrc.NewMetadataRepositoryClient(mrseed)
		So(err, ShouldBeNil)

		Reset(func() {
			mcli.Close()
			mrcli.Close()
		})

		r, err := mcli.AddLogStream(context.TODO(), nil)
		So(err, ShouldBeNil)

		lsID := r.LogStream.LogStreamID

		So(testutil.CompareWaitN(100, func() bool {
			meta, err := mrcli.GetMetadata(context.TODO())
			if err != nil {
				return false
			}

			return meta.GetLogStream(lsID) != nil
		}), ShouldBeTrue)

		varlog, err := varlog.Open(vtypes.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
		So(err, ShouldBeNil)

		Reset(func() {
			varlog.Close()
		})

		_, err = varlog.Append(context.TODO(), []byte("foo"))
		So(err, ShouldBeNil)

		Convey("When SN backup fail", func() {
			meta, err := mrcli.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			lsdesc := meta.GetLogStream(lsID)

			err = k8s.StopSN(lsdesc.Replicas[len(lsdesc.Replicas)-1].StorageNodeID)
			So(err, ShouldBeNil)

			Convey("Then it should be sealed", func() {
				So(testutil.CompareWaitN(100, func() bool {
					varlog.Append(context.TODO(), []byte("foo"))

					meta, err := mrcli.GetMetadata(context.TODO())
					if err != nil {
						return false
					}

					return meta.GetLogStream(lsID).Status.Sealed()
				}), ShouldBeTrue)

				Convey("When SN recover", func() {
					err := k8s.RecoverSN()
					So(err, ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						ok, err := k8s.IsSNRunning()
						if err != nil {
							return false
						}

						return ok
					}), ShouldBeTrue)

					Convey("Then if should be able to append", func() {
						So(testutil.CompareWaitN(100, func() bool {
							mcli.Unseal(context.TODO(), lsID)

							_, err := varlog.Append(context.TODO(), []byte("foo"))
							return err == nil
						}), ShouldBeTrue)
					})
				})
			})
		})
	}))
}

func TestK8sVarlogAppend(t *testing.T) {
	opts := getK8sVarlogClusterOpts()
	opts.Reset = false

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		mcli, err := varlog.NewClusterManagerClient(vmsaddr)
		So(err, ShouldBeNil)

		mrcli, err := mrc.NewMetadataRepositoryClient(mrseed)
		So(err, ShouldBeNil)

		varlog, err := varlog.Open(vtypes.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
		So(err, ShouldBeNil)

		Reset(func() {
			mcli.Close()
			mrcli.Close()
			varlog.Close()
		})

		meta, err := mrcli.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(len(meta.GetLogStreams()), ShouldBeGreaterThan, 0)
		lsdesc := meta.GetLogStreams()[0]

		if lsdesc.Status.Sealed() {
			mcli.Unseal(context.TODO(), lsdesc.LogStreamID)
		}

		_, err = varlog.Append(context.TODO(), []byte("foo"))
		So(err, ShouldBeNil)
	}))
}
