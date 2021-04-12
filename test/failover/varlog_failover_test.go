package main

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/test"
	"github.com/kakao/varlog/vtesting"
)

func TestVarlogFailoverMRLeaderFail(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:              3,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := test.NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.HealthCheck()
		}), ShouldBeTrue)

		leader := env.Leader()

		_, err := env.AddSN()
		So(err, ShouldBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		errC := make(chan error, 1024)
		glsnC := make(chan types.GLSN, 1024)

		appendf := func(actx context.Context) {
			cli, err := env.NewLogIOClient(lsID)
			if err != nil {
				errC <- err
				return
			}
			defer cli.Close()

			for {
				select {
				case <-actx.Done():
					return
				default:
					rctx, cancel := context.WithTimeout(actx, vtesting.TimeoutUnitTimesFactor(50))
					glsn, err := cli.Append(rctx, lsID, []byte("foo"))
					cancel()
					if err != nil {
						if err != context.DeadlineExceeded {
							errC <- err
						}
					} else {
						glsnC <- glsn
					}
				}
			}
		}

		runner := runner.New("failover-leader-test", zap.NewNop())
		defer runner.Stop()

		for i := 0; i < 5; i++ {
			runner.Run(appendf)
		}

		Convey("When MR leader fail", func(ctx C) {
			maxGLSN := types.InvalidGLSN
			triggerGLSN := types.GLSN(20)
			goalGLSN := types.GLSN(3) * triggerGLSN
			stopped := false

			timer := time.NewTimer(vtesting.TimeoutUnitTimesFactor(100))
			defer timer.Stop()

		Loop:
			for {
				select {
				case <-timer.C:
					t.Fatal("timeout")
				case glsn := <-glsnC:
					if maxGLSN < glsn {
						maxGLSN = glsn
					}
					if !stopped && maxGLSN > triggerGLSN {
						So(env.LeaderFail(), ShouldBeTrue)
						stopped = true

						So(testutil.CompareWaitN(50, func() bool {
							return env.Leader() != leader
						}), ShouldBeTrue)

					} else if maxGLSN > goalGLSN {
						break Loop
					}
				case err := <-errC:
					So(err, ShouldBeNil)
				}
			}

			Convey("Then it should be able to keep appending log", func(ctx C) {
				cli, err := env.NewLogIOClient(lsID)
				So(err, ShouldBeNil)
				defer cli.Close()

				for glsn := types.MinGLSN; glsn <= maxGLSN; glsn += types.GLSN(1) {
					_, err := cli.Read(context.TODO(), lsID, glsn)
					So(err, ShouldBeNil)
				}
			})
		})
	})
}

func TestVarlogFailoverSNBackupFail(t *testing.T) {
	t.Skip("[WIP] Sync API")
	nrRep := 2
	nrCli := 5
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := test.VarlogClusterOptions{
		NrMR:              1,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}

	Convey("Given Varlog cluster", t, test.WithTestCluster(opts, func(env *test.VarlogCluster) {
		for i := 0; i < nrRep; i++ {
			_, err := env.AddSNByVMS()
			So(err, ShouldBeNil)
		}

		lsID, err := env.AddLSByVMS()
		So(err, ShouldBeNil)

		meta, _ := env.GetMR().GetMetadata(context.TODO())
		So(meta, ShouldNotBeNil)

		ls := meta.LogStreams[0]

		errC := make(chan error, 1024)
		glsnC := make(chan types.GLSN, 1024)

		appendf := func(actx context.Context) {
			cli, err := env.NewLogIOClient(lsID)
			if err != nil {
				errC <- err
				return
			}
			defer cli.Close()

			var backups []logc.StorageNode
			backups = make([]logc.StorageNode, nrRep-1)
			for i := 1; i < nrRep; i++ {
				replicaID := ls.Replicas[i].StorageNodeID
				replica := meta.GetStorageNode(replicaID)

				backups[i-1].ID = replicaID
				backups[i-1].Addr = replica.Address
			}

			for {
				select {
				case <-actx.Done():
					return
				default:
					rctx, cancel := context.WithTimeout(actx, vtesting.TimeoutUnitTimesFactor(10))
					glsn, err := cli.Append(rctx, lsID, []byte("foo"), backups...)
					cancel()
					if err != nil {
						if verrors.IsTransient(err) {
							continue
						}
						errC <- err
						return
					} else {
						glsnC <- glsn
					}
				}
			}
		}

		runner := runner.New("backup-fail-test", zap.NewNop())
		defer func() {
			runner.Stop()
		}()

		for i := 0; i < nrCli; i++ {
			runner.Run(appendf)
		}

		Convey("When backup SN fail", func(ctx C) {
			errCnt := 0
			maxGLSN := types.InvalidGLSN
			oldsn, _ := env.GetBackupSN(lsID, 1)

			timer := time.NewTimer(vtesting.TimeoutUnitTimesFactor(100))
			defer timer.Stop()

		Loop:
			for {
				select {
				case <-timer.C:
					t.Fatal("timeout")
				case glsn := <-glsnC:
					if maxGLSN < glsn {
						maxGLSN = glsn
					}

					if glsn == types.GLSN(32) {
						oldsn.Close()
					}
				case <-errC:
					errCnt++
					if errCnt == nrCli {
						break Loop
					}
				}
			}

			Convey("Then it should not be able to append", func(ctx C) {
				sealedGLSN, _ := env.GetMR().Seal(context.TODO(), lsID)
				So(sealedGLSN, ShouldBeGreaterThanOrEqualTo, maxGLSN)

				psn, _ := env.GetPrimarySN(lsID)
				So(psn, ShouldNotBeNil)

				So(testutil.CompareWaitN(50, func() bool {
					status, _, _ := psn.Seal(context.TODO(), lsID, sealedGLSN)
					return status == varlogpb.LogStreamStatusSealed
				}), ShouldBeTrue)

				_, hwm, _ := psn.Seal(context.TODO(), lsID, sealedGLSN)
				So(hwm, ShouldEqual, sealedGLSN)

				Convey("When backup SN recover", func(ctx C) {
					sn, err := env.RecoverSN(ls.Replicas[1].StorageNodeID)
					So(err, ShouldBeNil)

					So(testutil.CompareWaitN(50, func() bool {
						snmeta, err := sn.GetMetadata(context.TODO())
						if err != nil {
							return false
						}

						status, _, _ := sn.Seal(context.TODO(), lsID, sealedGLSN)
						if status == varlogpb.LogStreamStatusSealing {
							replica := snpb.Replica{
								StorageNodeID: ls.Replicas[1].StorageNodeID,
								LogStreamID:   lsID,
								Address:       snmeta.StorageNode.Address,
							}
							psn.Sync(context.TODO(), lsID, replica, sealedGLSN)
						}

						return status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					Convey("Then it should be abel to append", func(ctx C) {

						cli, err := env.NewLogIOClient(lsID)
						So(err, ShouldBeNil)
						Reset(func() {
							cli.Close()
						})

						var backups []logc.StorageNode
						backups = make([]logc.StorageNode, nrRep-1)
						for i := 1; i < nrRep; i++ {
							replicaID := ls.Replicas[i].StorageNodeID
							replica := meta.GetStorageNode(replicaID)

							backups[i-1].ID = replicaID
							backups[i-1].Addr = replica.Address
						}

						cmCli := env.GetClusterManagerClient()

						So(testutil.CompareWaitN(10, func() bool {
							cmCli.Unseal(context.TODO(), lsID)

							rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
							defer cancel()
							_, err = cli.Append(rctx, lsID, []byte("foo"), backups...)
							return err == nil
						}), ShouldBeTrue)
					})
				})
			})
		})
	}))
}

func TestVarlogFailoverRecoverFromSML(t *testing.T) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := test.VarlogClusterOptions{
		NrMR:              1,
		NrRep:             1,
		UnsafeNoWal:       true,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}

	Convey("Given Varlog cluster with StateMachineLog", t, test.WithTestCluster(opts, func(env *test.VarlogCluster) {
		So(testutil.CompareWaitN(10, func() bool {
			return env.HealthCheck()
		}), ShouldBeTrue)

		leader := env.Leader()

		_, err := env.AddSNByVMS()
		So(err, ShouldBeNil)

		lsID, err := env.AddLSByVMS()
		So(err, ShouldBeNil)

		cli, err := env.NewLogIOClient(lsID)
		So(err, ShouldBeNil)
		defer cli.Close()

		var glsn types.GLSN
		for i := 0; i < 5; i++ {
			glsn, err = cli.Append(context.TODO(), lsID, []byte("foo"))
			So(err, ShouldBeNil)
		}

		Convey("When MR leader restart", func(ctx C) {
			env.RestartMR(leader)

			So(testutil.CompareWaitN(10, func() bool {
				return env.HealthCheck()
			}), ShouldBeTrue)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR()
				So(mr.GetHighWatermark(), ShouldEqual, glsn)

				metadata, err := mr.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				So(len(metadata.LogStreams), ShouldEqual, 1)

				Convey("Then ls metadata from SN should be sealed", func(ctx C) {
					ls := metadata.LogStreams[0]
					So(ls.Status, ShouldEqual, varlogpb.LogStreamStatusSealed)

					So(testutil.CompareWaitN(10, func() bool {
						meta, err := env.SNs[0].GetMetadata(context.TODO())
						if err != nil {
							return false
						}

						lsmeta, ok := meta.GetLogStream(lsID)
						if !ok {
							return false
						}

						return lsmeta.Status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					cmCli := env.GetClusterManagerClient()

					recoveredGLSN := types.InvalidGLSN
					So(testutil.CompareWaitN(10, func() bool {
						cmCli.Unseal(context.TODO(), lsID)

						rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
						defer cancel()
						recoveredGLSN, err = cli.Append(rctx, lsID, []byte("foo"))
						return err == nil
					}), ShouldBeTrue)

					So(recoveredGLSN, ShouldEqual, glsn+1)
				})
			})
		})
	}))
}
