package main

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/test/it"
	"github.com/kakao/varlog/vtesting"
)

func TestVarlogFailoverMRLeaderFail(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := []it.Option{
			it.WithMRCount(3),
			it.WithNumberOfStorageNodes(1),
			it.WithNumberOfLogStreams(1),
			it.WithNumberOfClients(5),
		}

		Convey("cluster", it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
			leader := env.IndexOfLeaderMR()

			errC := make(chan error, 1024)
			glsnC := make(chan types.GLSN, 1024)

			lsID := env.LogStreamIDs()[0]

			var (
				wg   sync.WaitGroup
				quit = make(chan struct{})
			)
			for i := 0; i < env.NumberOfClients(); i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					client := env.ClientAtIndex(t, idx)
					for {
						select {
						case <-quit:
							return
						default:
						}

						glsn, err := client.Append(context.Background(), []byte("foo"))
						if err != nil {
							errC <- err
						} else {
							glsnC <- glsn
						}
					}
				}(i)
			}
			defer func() {
				close(quit)
				wg.Wait()
			}()

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
							env.LeaderFail(t)
							stopped = true

							So(testutil.CompareWaitN(50, func() bool {
								return env.IndexOfLeaderMR() != leader
							}), ShouldBeTrue)

						} else if maxGLSN > goalGLSN {
							break Loop
						}
					case err := <-errC:
						So(err, ShouldBeNil)
					}
				}

				Convey("Then it should be able to keep appending log", func(ctx C) {
					client := env.ClientAtIndex(t, 0)

					_, err := client.Append(context.Background(), []byte("bar"))
					So(err, ShouldBeNil)

					for glsn := types.MinGLSN; glsn <= maxGLSN; glsn += types.GLSN(1) {
						_, err := client.Read(context.TODO(), lsID, glsn)
						So(err, ShouldBeNil)
					}
				})
			})
		}))
	})
}

func TestVarlogFailoverSNBackupFail(t *testing.T) {
	t.Skip("[WIP] Sync API")

	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(5),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		errC := make(chan error, 1024)
		glsnC := make(chan types.GLSN, 1024)

		var wg sync.WaitGroup
		for i := 0; i < env.NumberOfClients(); i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				client := env.ClientAtIndex(t, idx)
				glsn, err := client.Append(context.Background(), []byte("foo"))
				if err != nil {
					errC <- err
					return
				}
				glsnC <- glsn

			}(i)
		}

		Convey("When backup SN fail", func(ctx C) {
			errCnt := 0
			maxGLSN := types.InvalidGLSN

			lsID := env.LogStreamIDs()[0]
			oldsn := env.BackupSNOf(t, lsID)
			oldSNID := oldsn.StorageNodeID()

			timer := time.NewTimer(vtesting.TimeoutUnitTimesFactor(100))
			defer timer.Stop()

			for errCnt < env.NumberOfClients() {
				select {
				case <-timer.C:
					t.Fatal("timeout")
				case glsn := <-glsnC:
					if maxGLSN < glsn {
						maxGLSN = glsn
					}

					if glsn == types.GLSN(32) {
						env.CloseSN(t, oldSNID)
						env.CloseSNClientOf(t, oldSNID)
					}
				case <-errC:
					errCnt++
				}
			}

			Convey("Then it should not be able to append", func(ctx C) {
				rsp, err := env.GetVMSClient(t).Seal(context.Background(), lsID)
				So(err, ShouldBeNil)
				sealedGLSN := rsp.GetSealedGLSN()
				So(sealedGLSN, ShouldBeGreaterThanOrEqualTo, maxGLSN)

				psn := env.PrimarySNOf(t, lsID)
				psnCL := env.SNClientOf(t, psn.StorageNodeID())
				snmd, err := psnCL.GetMetadata(context.Background())
				lsmd, ok := snmd.GetLogStream(lsID)
				So(ok, ShouldBeTrue)
				So(lsmd.GetStatus(), ShouldEqual, varlogpb.LogStreamStatusSealed)
				// TODO (jun): Add assertion that HWM of the LS equals to sealedGLSN

				Convey("When backup SN recover", func(ctx C) {
					env.RecoverSN(t, oldSNID)
					env.NewSNClient(t, oldSNID)

					So(testutil.CompareWaitN(50, func() bool {
						mcl := env.SNClientOf(t, oldSNID)
						snmeta, err := mcl.GetMetadata(context.Background())
						if err != nil {
							return false
						}
						lsmd, ok := snmeta.GetLogStream(lsID)
						return ok && lsmd.GetStatus() == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					Convey("Then it should be abel to append", func(ctx C) {
						client := env.ClientAtIndex(t, 0)
						So(testutil.CompareWaitN(10, func() bool {
							env.GetVMSClient(t).Unseal(context.TODO(), lsID)
							_, err := client.Append(context.Background(), []byte("foo"))
							return err == nil
						}), ShouldBeTrue)
					})
				})
			})
		})
	}))
}

func TestVarlogFailoverRecoverFromSML(t *testing.T) {
	opts := []it.Option{
		it.WithoutWAL(),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		leader := env.IndexOfLeaderMR()
		lsID := env.LogStreamID(t, 0)

		cli := env.NewLogIOClient(t, lsID)
		defer cli.Close()

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < 5; i++ {
			glsn, err = cli.Append(context.TODO(), lsID, []byte("foo"))
			So(err, ShouldBeNil)
		}

		Convey("When MR leader restart", func(ctx C) {
			env.RestartMR(t, leader)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetHighWatermark(), ShouldEqual, glsn)

				metadata, err := mr.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				So(len(metadata.LogStreams), ShouldEqual, 1)

				Convey("Then ls metadata from SN should be sealed", func(ctx C) {
					ls := metadata.LogStreams[0]
					So(ls.Status.Sealed(), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						metadata, err := mr.GetMetadata(context.TODO())
						if err != nil {
							return false
						}

						return metadata.LogStreams[0].Status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						meta, err := env.StorageNodes()[0].GetMetadata(context.TODO())
						if err != nil {
							return false
						}

						lsmeta, ok := meta.GetLogStream(lsID)
						if !ok {
							return false
						}

						return lsmeta.Status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					cmCli := env.GetVMSClient(t)

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

func TestVarlogFailoverRecoverFromIncompleteSML(t *testing.T) {
	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		lsID := env.LogStreamID(t, 0)
		env.WaitCommit(t, lsID, glsn)

		Convey("When commit happens during MR close all without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, lsID, []byte("foo"))
			}

			prev := glsn
			offset := glsn + 1
			glsn = glsn + types.GLSN(nrAppend)
			env.CommitWithoutMR(t, lsID, types.LLSN(offset), offset, nrAppend, prev, glsn)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetHighWatermark(), ShouldEqual, glsn)

				metadata, err := mr.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				So(len(metadata.LogStreams), ShouldEqual, 1)

				Convey("Then ls metadata from SN should be sealed", func(ctx C) {
					ls := metadata.LogStreams[0]
					So(ls.Status.Sealed(), ShouldBeTrue)

					env.WaitSealed(t, lsID)

					cmCli := env.GetVMSClient(t)

					recoveredGLSN := types.InvalidGLSN
					So(testutil.CompareWaitN(10, func() bool {
						cmCli.Unseal(context.TODO(), lsID)

						rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
						defer cancel()

						recoveredGLSN, err = client.Append(rctx, []byte("foo"))
						return err == nil
					}), ShouldBeTrue)

					So(recoveredGLSN, ShouldEqual, glsn+1)
				})
			})
		})
	}))
}

func TestVarlogFailoverRecoverFromIncompleteSMLWithEmptyCommit(t *testing.T) {
	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(2),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 10

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		for _, lsID := range env.LogStreamIDs() {
			env.WaitCommit(t, lsID, glsn)
		}

		Convey("When empty commit happens during MR close all without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)
			/*
				       HWM | PrevHWM
				LS1 |  11  | 10
				LS2 |  <EMPTY>

				LS1 |  <EMPTY>
				LS2 |  12  | 11

				LS1 |  13  | 12
				LS2 |  <EMPTY>


				LS1 |  <EMPTY>
				LS2 |  14  | 13
			*/

			for i := 0; i < 2; i++ {
				for _, lsID := range env.LogStreamIDs() {
					llsn := env.GetUncommittedLLSNOffset(t, lsID)

					env.AppendUncommittedLog(t, lsID, []byte("foo"))

					env.CommitWithoutMR(t, lsID, llsn, glsn+types.GLSN(1), 1, glsn, glsn+types.GLSN(1))
					glsn += types.GLSN(1)

					env.WaitCommit(t, lsID, glsn)
				}
			}

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetHighWatermark(), ShouldEqual, glsn)

				prevHWM := mr.GetPrevHighWatermark()

				crs, _ := mr.LookupNextCommitResults(prevHWM)
				So(crs, ShouldNotBeNil)

				for _, lsID := range env.LogStreamIDs() {
					cr := crs.LookupCommitResult(lsID)
					So(cr, ShouldNotBeNil)

					llsn := env.GetUncommittedLLSNOffset(t, lsID)

					So(cr.CommittedLLSNOffset+types.LLSN(cr.CommittedGLSNLength), ShouldEqual, llsn)

				}
			})
		})
	}))
}

func TestVarlogFailoverSyncLogStream(t *testing.T) {
	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		lsID := env.LogStreamID(t, 0)
		env.WaitCommit(t, lsID, glsn)

		Convey("When add log stream without writeing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			addedLSID := env.AddLSWithoutMR(t)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, addedLSID, []byte("foo"))
			}

			prev := glsn
			offset := glsn + 1
			glsn = glsn + types.GLSN(nrAppend)

			env.CommitWithoutMR(t, lsID, types.LLSN(offset), offset, 0, prev, glsn)
			env.WaitCommit(t, lsID, glsn)

			env.CommitWithoutMR(t, addedLSID, types.MinLLSN, offset, nrAppend, prev, glsn)
			env.WaitCommit(t, addedLSID, glsn)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetHighWatermark(), ShouldEqual, glsn)

				metadata, err := mr.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				So(len(metadata.LogStreams), ShouldEqual, 2)

				Convey("Then ls metadata from SN should be sealed", func(ctx C) {
					for _, ls := range metadata.LogStreams {
						So(ls.Status.Sealed(), ShouldBeTrue)

						env.WaitSealed(t, ls.LogStreamID)
					}

					cmCli := env.GetVMSClient(t)

					recoveredGLSN := types.InvalidGLSN
					So(testutil.CompareWaitN(10, func() bool {
						cmCli.Unseal(context.TODO(), addedLSID)

						rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
						defer cancel()

						env.ClientRefresh(t)
						client := env.ClientAtIndex(t, 0)

						recoveredGLSN, err = client.Append(rctx, []byte("foo"))
						return err == nil
					}), ShouldBeTrue)

					So(recoveredGLSN, ShouldEqual, glsn+1)
				})
			})
		})

		Convey("When update log stream without writing SML", func(ctx C) {
			addedSNID := env.AddSN(t)

			env.CloseMRAllForRestart(t)

			env.UpdateLSWithoutMR(t, lsID, addedSNID, true)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should update LS", func(ctx C) {
				mr := env.GetMR(t)
				meta, err := mr.GetMetadata(context.Background())
				require.NoError(t, err)

				logStreamDesc := meta.GetLogStream(lsID)
				require.NotNil(t, logStreamDesc)
				require.Equal(t, logStreamDesc.Replicas[0].StorageNodeID, addedSNID)
			})
		})
	}))
}

func TestVarlogFailoverSyncLogStreamSelectReplica(t *testing.T) {
	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		snID := env.StorageNodeIDAtIndex(t, 0)
		lsID := env.LogStreamID(t, 0)

		Convey("When update log stream without writing SML, and do not clear victim replica", func(ctx C) {
			client := env.ClientAtIndex(t, 0)

			var (
				err  error
				glsn types.GLSN
			)
			for i := 0; i < nrAppend; i++ {
				glsn, err = client.Append(context.TODO(), []byte("foo"))
				So(err, ShouldBeNil)
			}
			env.WaitCommit(t, lsID, glsn)

			addedSNID := env.AddSN(t)

			env.CloseMRAllForRestart(t)

			env.UpdateLSWithoutMR(t, lsID, addedSNID, false)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should select exist replica", func(ctx C) {
				mr := env.GetMR(t)
				meta, err := mr.GetMetadata(context.Background())
				require.NoError(t, err)

				logStreamDesc := meta.GetLogStream(lsID)
				require.NotNil(t, logStreamDesc)
				require.Equal(t, logStreamDesc.Replicas[0].StorageNodeID, snID)
			})
		})

		Convey("When update log stream without writing SML, and append", func(ctx C) {
			addedSNID := env.AddSN(t)

			env.CloseMRAllForRestart(t)

			env.UpdateLSWithoutMR(t, lsID, addedSNID, false)
			env.UnsealWithoutMR(t, lsID, types.InvalidGLSN)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, lsID, []byte("foo"))
			}

			glsn := types.GLSN(nrAppend)

			env.CommitWithoutMR(t, lsID, types.MinLLSN, types.MinGLSN, nrAppend, types.InvalidGLSN, glsn)
			env.WaitCommit(t, lsID, glsn)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should select exist replica", func(ctx C) {
				mr := env.GetMR(t)
				meta, err := mr.GetMetadata(context.Background())
				require.NoError(t, err)

				logStreamDesc := meta.GetLogStream(lsID)
				require.NotNil(t, logStreamDesc)
				require.Equal(t, logStreamDesc.Replicas[0].StorageNodeID, addedSNID)
			})
		})
	}))
}

func TestVarlogFailoverSyncLogStreamIgnore(t *testing.T) {
	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		lsID := env.LogStreamID(t, 0)
		env.WaitCommit(t, lsID, glsn)

		Convey("When add log stream incomplete without writeing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			incompleteLSID := env.AddLSIncomplete(t)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				meta, err := mr.GetMetadata(context.Background())
				require.NoError(t, err)

				logStreamDesc := meta.GetLogStream(incompleteLSID)
				require.Nil(t, logStreamDesc)
			})
		})
	}))
}

func TestVarlogFailoverSyncLogStreamError(t *testing.T) {
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		lsID := env.LogStreamID(t, 0)
		env.WaitCommit(t, lsID, glsn)

		Convey("When remove replica without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			snID := env.StorageNodeIDAtIndex(t, 0)

			snMCL := env.SNClientOf(t, snID)
			snMCL.RemoveLogStream(context.Background(), lsID)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			// TODO:: how to catch panic
			Convey("Then it should not be recovered", func(ctx C) {
			})
		})

		Convey("When remove replica without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			addedLSID := env.AddLSWithoutMR(t)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, addedLSID, []byte("foo"))
			}

			prev := glsn
			offset := glsn + 1
			glsn = glsn + types.GLSN(nrAppend)
			env.CommitWithoutMR(t, addedLSID, types.MinLLSN, offset, nrAppend, prev, glsn)
			env.WaitCommit(t, addedLSID, glsn)

			snID := env.StorageNodeIDAtIndex(t, 0)

			snMCL := env.SNClientOf(t, snID)
			snMCL.RemoveLogStream(context.Background(), addedLSID)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			// TODO:: how to catch panic
			Convey("Then it should not be recovered", func(ctx C) {
			})
		})
	}))
}

func TestVarlogFailoverUpdateLS(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(3),
		it.WithNumberOfLogStreams(2),
		it.WithNumberOfClients(5),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		client := env.ClientAtIndex(t, 0)

		for i := 0; i < 32; i++ {
			_, err := client.Append(context.Background(), []byte("foo"))
			So(err, ShouldBeNil)
		}

		Convey("When SN fail", func(ctx C) {

			var victim types.StorageNodeID
			var updateLS types.LogStreamID
			for _, lsID := range env.LogStreamIDs() {
				sn := env.PrimarySNOf(t, lsID)
				snCL := env.SNClientOf(t, sn.StorageNodeID())
				snmd, _ := snCL.GetMetadata(context.TODO())
				if len(snmd.GetLogStreams()) == 1 {
					updateLS = lsID
					victim = sn.StorageNodeID()
					break
				}

				sn = env.BackupSNOf(t, lsID)
				snCL = env.SNClientOf(t, sn.StorageNodeID())
				snmd, _ = snCL.GetMetadata(context.TODO())
				if len(snmd.GetLogStreams()) == 1 {
					updateLS = lsID
					victim = sn.StorageNodeID()
					break
				}
			}

			addedSN := env.AddSN(t)

			env.CloseSN(t, victim)
			env.CloseSNClientOf(t, victim)

			for i := 0; i < 32; i++ {
				_, err := client.Append(context.Background(), []byte("foo"))
				So(err, ShouldBeNil)
			}

			Convey("Then it should not be able to append", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					meta := env.GetMetadata(t)
					lsdesc := meta.GetLogStream(updateLS)
					return lsdesc.Status == varlogpb.LogStreamStatusSealed
				}), ShouldBeTrue)

				env.UpdateLS(t, updateLS, victim, addedSN)

				for i := 0; i < 32; i++ {
					_, err := client.Append(context.Background(), []byte("foo"))
					So(err, ShouldBeNil)
				}

				Convey("When backup SN recover", func(ctx C) {
					env.RecoverSN(t, victim)
					env.NewSNClient(t, victim)

					So(testutil.CompareWaitN(50, func() bool {
						mcl := env.SNClientOf(t, victim)
						snmeta, err := mcl.GetMetadata(context.Background())
						if err != nil {
							return false
						}
						_, ok := snmeta.GetLogStream(updateLS)
						return ok
					}), ShouldBeTrue)

					for i := 0; i < 32; i++ {
						_, err := client.Append(context.Background(), []byte("foo"))
						So(err, ShouldBeNil)
					}
				})
			})
		})
	}))
}
