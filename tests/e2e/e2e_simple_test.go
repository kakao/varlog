//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"log"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestK8sVarlogSimple(t *testing.T) {
	opts := getK8sVarlogClusterOpts()

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		ctx, cancel := k8s.TimeoutContext()
		defer cancel()
		mcli, err := varlog.NewAdmin(ctx, vmsaddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(mcli.Close(), ShouldBeNil)
		})

		Convey("AddLogStream - Simple", func() {
			var topicID types.TopicID
			k8s.WithTimeoutContext(func(ctx context.Context) {
				topic, err := mcli.AddTopic(ctx)
				So(err, ShouldBeNil)
				topicID = topic.TopicID
			})

			var lsID types.LogStreamID

			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					var logStreamDesc *varlogpb.LogStreamDescriptor
					logStreamDesc, err = mcli.AddLogStream(ctx, topicID, nil)
					So(err, ShouldBeNil)
					lsID = logStreamDesc.GetLogStreamID()
				})
				return err == nil
			})

			mrseed, err := k8s.MRAddress()
			So(err, ShouldBeNil)

			var vlg varlog.Log
			k8s.WithTimeoutContext(func(ctx context.Context) {
				vlg, err = varlog.Open(
					ctx,
					types.ClusterID(1),
					[]string{mrseed},
					varlog.WithDenyTTL(5*time.Second),
					varlog.WithMetadataRefreshInterval(time.Second),
				)
				So(err, ShouldBeNil)
			})

			Reset(func() {
				So(vlg.Close(), ShouldBeNil)
			})

			var glsn types.GLSN
			k8s.WithTimeoutContext(func(ctx context.Context) {
				res := vlg.Append(ctx, topicID, [][]byte{[]byte("foo")})
				So(res.Err, ShouldBeNil)
				glsn = res.Metadata[0].GLSN
			})

			readCtx, readCancel := k8s.TimeoutContext()
			defer readCancel()
			_, err = vlg.Read(readCtx, topicID, lsID, glsn)
			So(err, ShouldBeNil)

			Convey("Seal", func() {
				sealCtx, sealCancel := k8s.TimeoutContext()
				defer sealCancel()
				rsp, err := mcli.Seal(sealCtx, topicID, lsID)
				So(err, ShouldBeNil)
				lsmetaList := rsp.GetLogStreams()
				So(len(lsmetaList), ShouldEqual, k8s.RepFactor)
				So(lsmetaList[0].HighWatermark, ShouldEqual, glsn)

				appendCtx, appendCancel := k8s.TimeoutContext()
				defer appendCancel()
				res := vlg.Append(appendCtx, topicID, [][]byte{[]byte("foo")})
				So(res.Err, ShouldNotBeNil)

				Convey("Unseal", func() {
					unsealCtx, unsealCancel := k8s.TimeoutContext()
					defer unsealCancel()
					_, err := mcli.Unseal(unsealCtx, topicID, lsID)
					So(err, ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						ctx, cancel := k8s.TimeoutContext()
						defer cancel()

						res := vlg.Append(ctx, topicID, [][]byte{[]byte("foo")})
						return res.Err == nil && res.Metadata[0].GLSN > lsmetaList[0].HighWatermark
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

		mcli, err := varlog.NewAdmin(connCtx, vmsaddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(mcli.Close(), ShouldBeNil)
		})

		var topicID types.TopicID
		k8s.WithTimeoutContext(func(ctx context.Context) {
			topic, err := mcli.AddTopic(ctx)
			So(err, ShouldBeNil)
			topicID = topic.TopicID
		})

		callCtx, callCancel := k8s.TimeoutContext()
		defer callCancel()
		_, err = mcli.AddLogStream(callCtx, topicID, nil)
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		openCtx, openCancel := k8s.TimeoutContext()
		defer openCancel()
		vlg, err := varlog.Open(openCtx, types.ClusterID(1), []string{mrseed}, varlog.WithDenyTTL(5*time.Second))
		So(err, ShouldBeNil)

		Reset(func() {
			vlg.Close()
		})

		appendCtx, appendCancel := k8s.TimeoutContext()
		defer appendCancel()
		res := vlg.Append(appendCtx, topicID, [][]byte{[]byte("foo")})
		So(res.Err, ShouldBeNil)

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
					res := vlg.Append(context.TODO(), topicID, [][]byte{[]byte("foo")})
					So(res.Err, ShouldBeNil)
				}

				Convey("When MR recover", func() {
					err := k8s.RecoverMR()
					So(err, ShouldBeNil)

					Convey("Then if should be abel to append", func() {
						for i := 0; i < 1000; i++ {
							res := vlg.Append(context.TODO(), topicID, [][]byte{[]byte("foo")})
							So(res.Err, ShouldBeNil)
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
			mcli  varlog.Admin
			mrcli mrc.MetadataRepositoryClient
			lsID  types.LogStreamID
		)
		k8s.WithTimeoutContext(func(ctx context.Context) {
			mcli, err = varlog.NewAdmin(ctx, vmsaddr)
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

		var topicID types.TopicID
		k8s.WithTimeoutContext(func(ctx context.Context) {
			topic, err := mcli.AddTopic(ctx)
			So(err, ShouldBeNil)
			topicID = topic.TopicID
		})

		k8s.WithTimeoutContext(func(ctx context.Context) {
			logStreamDesc, err := mcli.AddLogStream(ctx, topicID, nil)
			So(err, ShouldBeNil)
			lsID = logStreamDesc.LogStreamID
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
			res := vlg.Append(ctx, topicID, [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
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
						vlg.Append(context.TODO(), topicID, [][]byte{[]byte("foo")})
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
							if _, err = mcli.Unseal(ctx, topicID, lsID); err == nil {
								if firstUnseal.IsZero() {
									firstUnseal = time.Now()
								}
								if res := vlg.Append(ctx, topicID, [][]byte{[]byte("foo")}); res.Err == nil {
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
							res := vlg.Append(ctx, topicID, [][]byte{[]byte("foo")})
							So(res.Err, ShouldBeNil)
						})
					})
				})
			})
		})
	}))
}

func TestK8sVarlogAppend(t *testing.T) {
	t.SkipNow()

	const topicID = types.TopicID(1)
	opts := getK8sVarlogClusterOpts()
	opts.Reset = false

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsaddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		mrseed, err := k8s.MRAddress()
		So(err, ShouldBeNil)

		var (
			mcli  varlog.Admin
			mrcli mrc.MetadataRepositoryClient
			vlg   varlog.Log
		)

		k8s.WithTimeoutContext(func(ctx context.Context) {
			mcli, err = varlog.NewAdmin(ctx, vmsaddr)
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
			mcli.Unseal(unsealCtx, topicID, lsdesc.LogStreamID)
			unsealCancel()
		}

		k8s.WithTimeoutContext(func(ctx context.Context) {
			res := vlg.Append(ctx, topicID, [][]byte{[]byte("foo")})
			So(res.Err, ShouldBeNil)
		})
	}))
}

func TestK8sVarlogEnduranceExample(t *testing.T) {
	t.Skip()

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
	t.Skip()

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
	opts.NrSN = 6
	opts.NrLS = 2
	opts.RepFactor = 3
	opts.Reset = true

	Convey("Given Varlog Cluster", t, withTestCluster(opts, func(k8s *K8sVarlogCluster) {
		vmsAddr, err := k8s.VMSAddress()
		So(err, ShouldBeNil)

		ctx, cancel := k8s.TimeoutContext()
		defer cancel()

		vmsCL, err := varlog.NewAdmin(ctx, vmsAddr)
		So(err, ShouldBeNil)

		Reset(func() {
			So(vmsCL.Close(), ShouldBeNil)
		})

		var topicID types.TopicID

		k8s.WithTimeoutContext(func(ctx context.Context) {
			rsp, err := vmsCL.AddTopic(ctx)
			So(err, ShouldBeNil)
			topicID = rsp.TopicID
		})

		logStreamIDs := make([]types.LogStreamID, 0, opts.NrLS)
		for i := 0; i < opts.NrLS; i++ {
			testutil.CompareWaitN(100, func() bool {
				var err error
				k8s.WithTimeoutContext(func(ctx context.Context) {
					var logStreamDesc *varlogpb.LogStreamDescriptor
					logStreamDesc, err = vmsCL.AddLogStream(ctx, topicID, nil)
					So(err, ShouldBeNil)
					logStreamID := logStreamDesc.GetLogStreamID()
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
					_, err = vmsCL.Seal(ctx, topicID, logStreamID)
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
					_, err = vmsCL.Unseal(ctx, topicID, logStreamID)
					So(err, ShouldBeNil)
				})
				log.Printf("Unseal (%d)", logStreamID)
				return err == nil
			})
		}
	}))
}
