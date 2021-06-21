// +build e2e

package e2e

import (
	"context"
	"log"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

func TestK8sVarlogSimple(t *testing.T) {
	opts := getK8sVarlogClusterOpts()

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		ctx, cancel := k8s.TimeoutContext()
		defer cancel()
		mcli, err := varlog.NewClusterManagerClient(ctx, vmsaddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(mcli.Close(), ShouldBeNil)
		})

		Convey("AddLogStream - Simple", func() {
			var lsID types.LogStreamID

			testutil.CompareWaitN(100, func() bool {
				var (
					err error
					rsp *vmspb.AddLogStreamResponse
				)
				k8s.WithTimeoutContext(func(ctx context.Context) {
					rsp, err = mcli.AddLogStream(ctx, nil)
					So(err, ShouldBeNil)
					lsID = rsp.GetLogStream().GetLogStreamID()
				})
				return err == nil
			})

			mrseed, err := k8s.MRAddress()
			So(err, ShouldBeNil)

			var vlg varlog.Varlog
			k8s.WithTimeoutContext(func(ctx context.Context) {
				vlg, err = varlog.Open(
					ctx,
					types.ClusterID(1),
					[]string{mrseed},
					varlog.WithDenyTTL(5*time.Second),
					varlog.WithMRConnectorCallTimeout(3*time.Second),
					varlog.WithMetadataRefreshTimeout(3*time.Second),
					varlog.WithOpenTimeout(10*time.Second),
				)
				So(err, ShouldBeNil)
			})

			Reset(func() {
				So(vlg.Close(), ShouldBeNil)
			})

			appendCtx, appendCancel := k8s.TimeoutContext()
			defer appendCancel()
			glsn, err := vlg.Append(appendCtx, []byte("foo"))
			So(err, ShouldBeNil)

			readCtx, readCancel := k8s.TimeoutContext()
			defer readCancel()
			_, err = vlg.Read(readCtx, lsID, glsn)
			So(err, ShouldBeNil)

			Convey("Seal", func() {
				sealCtx, sealCancel := k8s.TimeoutContext()
				defer sealCancel()
				rsp, err := mcli.Seal(sealCtx, lsID)
				So(err, ShouldBeNil)
				lsmetaList := rsp.GetLogStreams()
				So(len(lsmetaList), ShouldEqual, k8s.RepFactor)
				So(lsmetaList[0].HighWatermark, ShouldEqual, glsn)

				appendCtx, appendCancel := k8s.TimeoutContext()
				defer appendCancel()
				glsn, err := vlg.Append(appendCtx, []byte("foo"))
				So(err, ShouldNotBeNil)
				So(glsn, ShouldEqual, types.InvalidGLSN)

				Convey("Unseal", func() {
					unsealCtx, unsealCancel := k8s.TimeoutContext()
					defer unsealCancel()
					_, err := mcli.Unseal(unsealCtx, lsID)
					So(err, ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						ctx, cancel := k8s.TimeoutContext()
						defer cancel()

						glsn, err := vlg.Append(ctx, []byte("foo"))
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

		connCtx, connCancel := k8s.TimeoutContext()
		defer connCancel()

		mcli, err := varlog.NewClusterManagerClient(connCtx, vmsaddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(mcli.Close(), ShouldBeNil)
		})

		callCtx, callCancel := k8s.TimeoutContext()
		defer callCancel()
		_, err = mcli.AddLogStream(callCtx, nil)
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		openCtx, openCancel := k8s.TimeoutContext()
		defer openCancel()
		varlog, err := varlog.Open(openCtx, types.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
		So(err, ShouldBeNil)

		Reset(func() {
			varlog.Close()
		})

		appendCtx, appendCancel := k8s.TimeoutContext()
		defer appendCancel()
		_, err = varlog.Append(appendCtx, []byte("foo"))
		So(err, ShouldBeNil)

		Convey("When MR follower fail", func() {
			k8s.WithTimeoutContext(func(ctx context.Context) {
				members, err := mcli.GetMRMembers(ctx)
				So(err, ShouldBeNil)
				So(len(members.Members), ShouldEqual, opts.NrMR)

				for nodeID := range members.Members {
					if nodeID != members.Leader {
						err := k8s.StopMR(nodeID)
						So(err, ShouldBeNil)
						break
					}
				}
			})

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
	opts.timeout = 20 * time.Second

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		var (
			mcli  varlog.ClusterManagerClient
			mrcli mrc.MetadataRepositoryClient
			lsID  types.LogStreamID
		)
		k8s.WithTimeoutContext(func(ctx context.Context) {
			mcli, err = varlog.NewClusterManagerClient(ctx, vmsaddr)
			So(err, ShouldBeNil)
		})

		k8s.WithTimeoutContext(func(ctx context.Context) {
			mrcli, err = mrc.NewMetadataRepositoryClient(ctx, mrseed)
			So(err, ShouldBeNil)
		})

		Reset(func() {
			So(mcli.Close(), ShouldBeNil)
			So(mrcli.Close(), ShouldBeNil)
		})

		k8s.WithTimeoutContext(func(ctx context.Context) {
			r, err := mcli.AddLogStream(ctx, nil)
			So(err, ShouldBeNil)
			lsID = r.LogStream.LogStreamID
		})

		So(testutil.CompareWaitN(100, func() bool {
			ctx, cancel := k8s.TimeoutContext()
			defer cancel()
			meta, err := mrcli.GetMetadata(ctx)
			return err == nil && meta.GetLogStream(lsID) != nil
		}), ShouldBeTrue)

		openCtx, openCancel := k8s.TimeoutContext()
		defer openCancel()
		vlg, err := varlog.Open(openCtx, types.ClusterID(1), []string{mrseed},
			varlog.WithDenyTTL(2*time.Second),
			varlog.WithMRConnectorCallTimeout(3*time.Second),
			varlog.WithMetadataRefreshTimeout(3*time.Second),
			varlog.WithMetadataRefreshInterval(10*time.Second),
			varlog.WithOpenTimeout(10*time.Second),
		)
		So(err, ShouldBeNil)

		Reset(func() {
			So(vlg.Close(), ShouldBeNil)
		})

		k8s.WithTimeoutContext(func(ctx context.Context) {
			_, err = vlg.Append(ctx, []byte("foo"))
			So(err, ShouldBeNil)
		})

		Convey("When SN backup fail", func() {
			meta, err := mrcli.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			lsdesc := meta.GetLogStream(lsID)

			victimStorageNodeID := lsdesc.Replicas[len(lsdesc.Replicas)-1].StorageNodeID
			err = k8s.StopSN(victimStorageNodeID)
			So(err, ShouldBeNil)
			log.Printf("Failed SN: %v", victimStorageNodeID)

			Convey("Then it should be sealed", func() {
				So(testutil.CompareWaitN(100, func() bool {
					k8s.WithTimeoutContext(func(ctx context.Context) {
						vlg.Append(context.TODO(), []byte("foo"))
					})
					ctx, cancel := k8s.TimeoutContext()
					defer cancel()
					meta, err := mrcli.GetMetadata(ctx)
					return err == nil && meta.GetLogStream(lsID).GetStatus().Sealed()
				}), ShouldBeTrue)

				Convey("When SN recover", func() {
					err := k8s.RecoverSN()
					So(err, ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						ok, err := k8s.IsSNRunning()
						return err == nil && ok
					}), ShouldBeTrue)

					Convey("Then if should be able to append", func() {
						var (
							err         error
							start       time.Time
							end         time.Time
							firstUnseal time.Time
							firstAppend time.Time
						)
						start = time.Now()
						ok := testutil.CompareWaitN(500, func() bool {
							ctx, cancel := k8s.TimeoutContext()
							defer cancel()
							if _, err = mcli.Unseal(ctx, lsID); err == nil {
								if firstUnseal.IsZero() {
									firstUnseal = time.Now()
								}
								if _, err = vlg.Append(ctx, []byte("foo")); err == nil {
									firstAppend = time.Now()
									return true
								}
							}
							// NOTE: 5s is enough time to unseal the log
							// stream.
							time.Sleep(5 * time.Second)
							return false
						})
						end = time.Now()

						log.Printf("total wait time = %s", end.Sub(start).String())
						log.Printf("unseal wait time = %s", firstUnseal.Sub(start).String())
						log.Printf("append wait elapsed time = %s", firstAppend.Sub(start).String())

						if err != nil {
							t.Logf("err = %+v", err)
						}
						So(err, ShouldBeNil)
						So(ok, ShouldBeTrue)

						k8s.WithTimeoutContext(func(ctx context.Context) {
							_, err = vlg.Append(ctx, []byte("foo"))
							So(err, ShouldBeNil)
						})
					})
				})
			})
		})
	}))
}

func TestK8sVarlogAppend(t *testing.T) {
	t.SkipNow()

	opts := getK8sVarlogClusterOpts()
	opts.Reset = false

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		var (
			mcli  varlog.ClusterManagerClient
			mrcli mrc.MetadataRepositoryClient
			vlg   varlog.Varlog
		)

		k8s.WithTimeoutContext(func(ctx context.Context) {
			mcli, err = varlog.NewClusterManagerClient(ctx, vmsaddr)
			So(err, ShouldBeNil)
		})

		k8s.WithTimeoutContext(func(ctx context.Context) {
			mrcli, err = mrc.NewMetadataRepositoryClient(ctx, mrseed)
			So(err, ShouldBeNil)
		})

		k8s.WithTimeoutContext(func(ctx context.Context) {
			vlg, err = varlog.Open(ctx, types.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
			So(err, ShouldBeNil)
		})

		Reset(func() {
			So(mcli.Close(), ShouldBeNil)
			So(mrcli.Close(), ShouldBeNil)
			So(vlg.Close(), ShouldBeNil)
		})

		ctx, cancel := k8s.TimeoutContext()
		defer cancel()
		meta, err := mrcli.GetMetadata(ctx)
		So(err, ShouldBeNil)
		So(len(meta.GetLogStreams()), ShouldBeGreaterThan, 0)
		lsdesc := meta.GetLogStreams()[0]

		if lsdesc.Status.Sealed() {
			unsealCtx, unsealCancel := k8s.TimeoutContext()
			mcli.Unseal(unsealCtx, lsdesc.LogStreamID)
			unsealCancel()
		}

		k8s.WithTimeoutContext(func(ctx context.Context) {
			_, err = vlg.Append(ctx, []byte("foo"))
			So(err, ShouldBeNil)
		})
	}))
}

func TestK8sVarlogEnduranceExample(t *testing.T) {
	opts := getK8sVarlogClusterOpts()
	opts.Reset = true
	opts.NrSN = 5
	opts.NrLS = 5
	opts.timeout = 20 * time.Second
	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		snFail := NewConfChanger(
			WithChangeFunc(AnyBackupSNFail(k8s)),
			WithCheckFunc(WaitSNFail(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		updateLS := NewConfChanger(
			WithChangeFunc(ReconfigureSealedLogStreams(k8s)),
			WithCheckFunc(WaitSealed(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		recoverSN := NewConfChanger(
			WithChangeFunc(RecoverSN(k8s)),
			WithCheckFunc(RecoverSNCheck(k8s)),
		)

		action := NewAction(WithTitle("backup sn fail"),
			WithClusterID(types.ClusterID(1)),
			WithMRAddr(mrseed),
			WithPrevFunc(InitLogStream(k8s)),
			WithConfChange(snFail),
			WithConfChange(updateLS),
			WithConfChange(recoverSN),
			WithNumClient(1),
			WithNumSubscriber(0),
		)

		err = action.Do(context.TODO())
		if err != nil {
			t.Logf("failed: %+v", err)
		}
		So(err, ShouldBeNil)
	}))
}

func TestK8sVarlogEnduranceFollowerMRFail(t *testing.T) {
	opts := getK8sVarlogClusterOpts()
	opts.NrMR = 3
	opts.NrSN = 3
	opts.NrLS = 1
	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		mrFail := NewConfChanger(
			WithChangeFunc(FollowerMRFail(k8s)),
			WithCheckFunc(WaitMRFail(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		recoverMR := NewConfChanger(
			WithChangeFunc(RecoverMR(k8s)),
			WithCheckFunc(RecoverMRCheck(k8s)),
			WithConfChangeInterval(10*time.Second),
		)

		action := NewAction(WithTitle("follower mr fail"),
			WithClusterID(types.ClusterID(1)),
			WithMRAddr(mrseed),
			WithConfChange(mrFail),
			WithConfChange(recoverMR),
			WithNumRepeat(3),
			WithPrevFunc(InitLogStream(k8s)),
			WithNumClient(1),
			WithNumSubscriber(0),
		)

		err = action.Do(context.TODO())
		if err != nil {
			t.Logf("failed: %+v", err)
		}
		So(err, ShouldBeNil)
	}))
}

func TestK8sCreateCluster(t *testing.T) {
	opts := getK8sVarlogClusterOpts()
	opts.NrMR = 3
	opts.NrSN = 12
	opts.NrLS = 4
	opts.RepFactor = 3

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsAddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		ctx, cancel := k8s.TimeoutContext()
		defer cancel()

		vmsCL, err := varlog.NewClusterManagerClient(ctx, vmsAddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(vmsCL.Close(), ShouldBeNil)
		})

		logStreamIDs := make([]types.LogStreamID, 0, opts.NrLS)
		for i := 0; i < opts.NrLS; i++ {
			testutil.CompareWaitN(100, func() bool {
				var (
					rsp *vmspb.AddLogStreamResponse
					err error
				)
				k8s.WithTimeoutContext(func(ctx context.Context) {
					rsp, err = vmsCL.AddLogStream(ctx, nil)
					So(err, ShouldBeNil)
					logStreamID := rsp.GetLogStream().GetLogStreamID()
					logStreamIDs = append(logStreamIDs, logStreamID)
					log.Printf("AddLogStream (%d)", logStreamID)
				})
				return err == nil
			})
		}

		for _, logStreamID := range logStreamIDs {
			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					_, err = vmsCL.Seal(ctx, logStreamID)
					So(err, ShouldBeNil)
				})
				log.Printf("Seal (%d)", logStreamID)
				return err == nil
			})
		}

		for _, logStreamID := range logStreamIDs {
			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					_, err = vmsCL.Unseal(ctx, logStreamID)
					So(err, ShouldBeNil)
				})
				log.Printf("Unseal (%d)", logStreamID)
				return err == nil
			})
		}
	}))
}
