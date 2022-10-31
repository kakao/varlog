package management

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/tests/it"
	"github.com/kakao/varlog/vtesting"
)

// FIXME: This test checks MRManager, move unit test or something similar.
func TestVarlogNewMRManager(t *testing.T) {
	Convey("Given that MRManager runs without any running MR", t, func(c C) {
		const (
			clusterID = types.ClusterID(1)
			portBase  = 20000
		)

		portLease, err := ports.ReserveWeaklyWithRetry(portBase)
		So(err, ShouldBeNil)

		var (
			mrRAFTAddr = fmt.Sprintf("http://127.0.0.1:%d", portLease.Base())
			mrRPCAddr  = fmt.Sprintf("127.0.0.1:%d", portLease.Base()+1)
		)

		defer func() {
			So(portLease.Release(), ShouldBeNil)
		}()

		Convey("When MR does not start within specified periods", func(c C) {
			Convey("Then the MRManager should return an error", func(ctx C) {
				_, err := mrmanager.New(context.TODO(),
					mrmanager.WithInitialMRConnRetryCount(1),
					mrmanager.WithInitialMRConnRetryBackoff(100*time.Millisecond),
					mrmanager.WithAddresses(mrRPCAddr),
					mrmanager.WithClusterID(clusterID),
				)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When MR starts within specified periods", func(c C) {
			// vms
			mrmC := make(chan mrmanager.MetadataRepositoryManager, 1)
			go func() {
				defer close(mrmC)
				mrm, err := mrmanager.New(context.TODO(),
					mrmanager.WithInitialMRConnRetryCount(30),
					mrmanager.WithInitialMRConnRetryBackoff(1*time.Second),
					mrmanager.WithAddresses(mrRPCAddr),
					mrmanager.WithClusterID(clusterID),
				)
				if err != nil {
					fmt.Printf("err: %+v\n", err)
				}
				c.So(err, ShouldBeNil)
				if err == nil {
					mrmC <- mrm
				}
			}()

			time.Sleep(10 * time.Millisecond)

			// mr
			mr := metarepos.NewRaftMetadataRepository(
				metarepos.WithClusterID(clusterID),
				metarepos.WithRPCAddress(mrRPCAddr),
				metarepos.WithRaftAddress(mrRAFTAddr),
				metarepos.WithReplicationFactor(1),
				metarepos.WithSnapshotCount(uint64(10)),
				metarepos.WithPeers(mrRAFTAddr),
				metarepos.WithRPCTimeout(vtesting.TimeoutAccordingToProcCnt(metarepos.DefaultRPCTimeout)),
				metarepos.WithRaftDirectory(filepath.Join(t.TempDir(), "raftdir")),
				metarepos.WithRaftTick(vtesting.TestRaftTick()),
				metarepos.WithLogger(zap.L()),
			)
			mr.Run()

			Reset(func() {
				So(mr.Close(), ShouldBeNil)
			})

			Convey("Then the MRManager should connect to the MR", func(ctx C) {
				mrm, ok := <-mrmC
				So(ok, ShouldBeTrue)
				So(mrm.Close(), ShouldBeNil)
			})
		})

	})

	Convey("Given MR cluster", t, func(ctx C) {
		opts := []mrmanager.Option{
			mrmanager.WithInitialMRConnRetryCount(3),
			mrmanager.WithClusterID(1),
		}
		env := it.NewVarlogCluster(t,
			it.WithMetadataRepositoryManagerOptions(
				mrmanager.WithInitialMRConnRetryCount(3),
				mrmanager.WithClusterID(1),
			),
		)
		defer env.Close(t)

		mr := env.GetMR(t)
		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		Convey("When create MRManager with non addrs", func(ctx C) {
			// VMS Server
			_, err := mrmanager.New(context.TODO(), opts...)
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with invalid addrs", func(ctx C) {
			// VMS Server
			opts = append(opts, mrmanager.WithAddresses(fmt.Sprintf("%s%d", mrAddr, 0)))
			_, err := mrmanager.New(context.TODO(), opts...)
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with valid addrs", func(ctx C) {
			// VMS Server
			opts = append(opts, mrmanager.WithAddresses(mrAddr))
			mrm, err := mrmanager.New(context.TODO(), opts...)
			Convey("Then it should be success", func(ctx C) {
				So(err, ShouldBeNil)

				Convey("and it should work", func(ctx C) {
					cinfo, err := mrm.GetClusterInfo(context.TODO())
					So(err, ShouldBeNil)
					So(len(cinfo.GetMembers()), ShouldEqual, 1)
				})
			})
			defer mrm.Close()
		})
	})
}

func TestVarlogMRManagerWithLeavedNode(t *testing.T) {
	t.Skip()
	Convey("Given MR cluster", t, func(ctx C) {
		vmsOpts := it.NewTestVMSOptions()
		env := it.NewVarlogCluster(t,
			it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
			it.WithVMSOptions(vmsOpts...),
		)
		defer env.Close(t)

		mr := env.GetMR(t)
		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		// VMS Server
		mrmOpts := []mrmanager.Option{
			mrmanager.WithAddresses(mrAddr),
			mrmanager.WithClusterID(1),
		}
		mrm, err := mrmanager.New(context.TODO(), mrmOpts...)
		So(err, ShouldBeNil)
		defer mrm.Close()

		Convey("When all the cluster configuration nodes are changed", func(ctx C) {
			env.AppendMR(t)
			env.StartMR(t, 1)

			nmr := env.GetMRByIndex(t, 1)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.AddPeer(rctx, env.MetadataRepositoryIDAt(t, 1), env.MRPeerAtIndex(t, 1), env.MRRPCEndpointAtIndex(t, 1))
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return nmr.IsMember()
			}), ShouldBeTrue)

			rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.RemovePeer(rctx, env.MetadataRepositoryIDAt(t, 0))
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return !mr.IsMember()
			}), ShouldBeTrue)

			oldCL, err := mrc.NewMetadataRepositoryManagementClient(context.TODO(), mrAddr)
			So(err, ShouldBeNil)
			_, err = oldCL.GetClusterInfo(context.TODO(), types.ClusterID(0))
			So(errors.Is(err, verrors.ErrNotMember), ShouldBeTrue)
			So(oldCL.Close(), ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				cinfo, err := nmr.GetClusterInfo(context.TODO(), types.ClusterID(0))
				return err == nil && len(cinfo.GetMembers()) == 1 && cinfo.GetLeader() == cinfo.GetNodeID()
			}), ShouldBeTrue)

			Convey("Then it should be success", func(ctx C) {
				rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
				defer cancel()

				cinfo, err := mrm.GetClusterInfo(rctx)
				So(err, ShouldBeNil)
				So(len(cinfo.GetMembers()), ShouldEqual, 1)
			})
		})
	})
}

/*
func TestVarlogStatRepositoryRefresh(t *testing.T) {
	t.Skip("Tests for StatRepository")

	Convey("Given MR cluster", t, func(ctx C) {
		opts := []it.Option{
			it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		}

		Convey("cluster", it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
			mr := env.GetMR(t)

			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetServerAddr() != ""
			}), ShouldBeTrue)
			mrAddr := mr.GetServerAddr()

			snID := env.AddSN(t)
			topicID := env.AddTopic(t)
			lsID := env.AddLS(t, topicID)

			cmView := &dummyCMView{
				clusterID: env.ClusterID(),
				addr:      mrAddr,
			}
			statRepository := varlogadm.NewStatRepository(context.TODO(), cmView)

			metaIndex := statRepository.GetAppliedIndex()
			So(metaIndex, ShouldBeGreaterThan, 0)
			So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
			lsStat := statRepository.GetLogStream(lsID)
			So(lsStat.Replicas, ShouldNotBeNil)

			Convey("When varlog cluster is not changed", func(ctx C) {
				Convey("Then refresh the statRepository and nothing happens", func(ctx C) {
					statRepository.Refresh(context.TODO())
					So(metaIndex, ShouldEqual, statRepository.GetAppliedIndex())
				})
			})

			Convey("When AddSN", func(ctx C) {
				snID2 := env.AddSN(t)

				Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
					statRepository.Refresh(context.TODO())
					So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())
					metaIndex := statRepository.GetAppliedIndex()

					So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
					So(statRepository.GetStorageNode(snID2), ShouldNotBeNil)
					lsStat := statRepository.GetLogStream(lsID)
					So(lsStat.Replicas, ShouldNotBeNil)

					Convey("When UpdateLS", func(ctx C) {
						meta, err := mr.GetMetadata(context.TODO())
						So(err, ShouldBeNil)

						ls := meta.GetLogStream(lsID)
						So(ls, ShouldNotBeNil)

						newLS := proto.Clone(ls).(*varlogpb.LogStreamDescriptor)
						newLS.Replicas[0].StorageNodeID = snID2

						err = mr.UpdateLogStream(context.TODO(), newLS)
						So(err, ShouldBeNil)

						Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
							statRepository.Refresh(context.TODO())
							So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())

							So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
							So(statRepository.GetStorageNode(snID2), ShouldNotBeNil)
							lsStat := statRepository.GetLogStream(lsID)
							So(lsStat.Replicas, ShouldNotBeNil)

							_, ok := lsStat.Replica(snID)
							So(ok, ShouldBeFalse)
							_, ok = lsStat.Replica(snID2)
							So(ok, ShouldBeTrue)
						})
					})
				})
			})

			Convey("When AddLS", func(ctx C) {
				lsID2 := env.AddLS(t, topicID)

				Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
					statRepository.Refresh(context.TODO())
					So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())

					So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
					lsStat := statRepository.GetLogStream(lsID)
					So(lsStat.Replicas, ShouldNotBeNil)
					lsStat = statRepository.GetLogStream(lsID2)
					So(lsStat.Replicas, ShouldNotBeNil)
				})
			})

			Convey("When SealLS", func(ctx C) {
				_, err := mr.Seal(context.TODO(), lsID)
				So(err, ShouldBeNil)

				Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
					statRepository.Refresh(context.TODO())
					So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())
					metaIndex := statRepository.GetAppliedIndex()

					So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
					lsStat := statRepository.GetLogStream(lsID)
					So(lsStat.Replicas, ShouldNotBeNil)

					Convey("When UnsealLS", func(ctx C) {
						err := mr.Unseal(context.TODO(), lsID)
						So(err, ShouldBeNil)

						Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
							statRepository.Refresh(context.TODO())
							So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())

							So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
							lsStat := statRepository.GetLogStream(lsID)
							So(lsStat.Replicas, ShouldNotBeNil)
						})
					})
				})
			})
		}))
	})
}

func TestVarlogStatRepositoryReport(t *testing.T) {
	t.Skip("Tests for StatRepository")

	Convey("Given MR cluster", t, func(ctx C) {
		opts := []it.Option{
			it.WithReporterClientFactory(metarepos.NewReporterClientFactory()),
		}

		Convey("cluster", it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
			mr := env.GetMR(t)

			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetServerAddr() != ""
			}), ShouldBeTrue)
			mrAddr := mr.GetServerAddr()

			snID := env.AddSN(t)
			topicID := env.AddTopic(t)
			lsID := env.AddLS(t, topicID)

			cmView := &dummyCMView{
				clusterID: env.ClusterID(),
				addr:      mrAddr,
			}
			statRepository := varlogadm.NewStatRepository(context.TODO(), cmView)

			So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
			lsStat := statRepository.GetLogStream(lsID)
			So(lsStat.Replicas, ShouldNotBeNil)

			Convey("When Report", func(ctx C) {
				sn := env.LookupSN(t, snID)

				addr := storagenode.TestGetAdvertiseAddress(t, sn)

				storagenode.TestSealLogStreamReplica(t, env.ClusterID(), snID, topicID, lsID, types.InvalidGLSN, addr)

				snm := storagenode.TestGetStorageNodeMetadataDescriptor(t, env.ClusterID(), snID, addr)

				statRepository.Report(context.TODO(), snm)

				Convey("Then it should be updated", func(ctx C) {
					lsStat := statRepository.GetLogStream(lsID)
					So(lsStat.Replicas, ShouldNotBeNil)

					r, ok := lsStat.Replica(snID)
					So(ok, ShouldBeTrue)

					So(r.Status.Sealed(), ShouldBeTrue)

					Convey("When AddSN and refresh the statRepository", func(ctx C) {
						_ = env.AddSN(t)
						statRepository.Refresh(context.TODO())

						Convey("Then reported info should be applied", func(ctx C) {
							lsStat := statRepository.GetLogStream(lsID)
							So(lsStat.Replicas, ShouldNotBeNil)

							r, ok := lsStat.Replica(snID)
							So(ok, ShouldBeTrue)

							So(r.Status.Sealed(), ShouldBeTrue)
						})
					})
				})
			})
		}))
	})
}
*/
