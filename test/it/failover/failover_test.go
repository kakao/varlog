package main

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/test/it"
	"github.daumkakao.com/varlog/varlog/vtesting"
)

func TestVarlogFailoverMRLeaderFail(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := []it.Option{
			it.WithMRCount(3),
			it.WithNumberOfStorageNodes(1),
			it.WithNumberOfLogStreams(1),
			it.WithNumberOfClients(5),
			it.WithNumberOfTopics(1),
		}

		Convey("cluster", it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
			leader := env.IndexOfLeaderMR()

			errC := make(chan error, 1024)
			glsnC := make(chan types.GLSN, 1024)

			topicID := env.TopicIDs()[0]
			lsID := env.LogStreamIDs(topicID)[0]

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

						glsn, err := client.Append(context.Background(), topicID, []byte("foo"))
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

					_, err := client.Append(context.Background(), topicID, []byte("bar"))
					So(err, ShouldBeNil)

					for glsn := types.MinGLSN; glsn <= maxGLSN; glsn += types.GLSN(1) {
						_, err := client.Read(context.TODO(), topicID, lsID, glsn)
						So(err, ShouldBeNil)
					}
				})
			})
		}))
	})
}

func TestVarlogFailoverSNBackupInitialFault(t *testing.T) {
	clus := it.NewVarlogCluster(t,
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(1),
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	)

	defer func() {
		clus.Close(t)
		testutil.GC()
	}()

	topicID := clus.TopicIDs()[0]
	_, err := clus.ClientAtIndex(t, 0).Append(context.Background(), topicID, []byte("foo"))
	require.NoError(t, err)

	lsID := clus.LogStreamID(t, topicID, 0)
	backupSNID := clus.BackupStorageNodeIDOf(t, lsID)

	clus.CloseSN(t, backupSNID)
	clus.CloseSNClientOf(t, backupSNID)

	clus.RecoverSN(t, backupSNID)
	clus.NewSNClient(t, backupSNID)
	clus.NewReportCommitterClient(t, backupSNID)

	require.Eventually(t, func() bool {
		snmd, err := clus.SNClientOf(t, backupSNID).GetMetadata(context.Background())
		if !assert.NoError(t, err) {
			return false
		}

		lsmd, ok := snmd.GetLogStream(lsID)
		if !assert.True(t, ok) {
			return false
		}

		return varlogpb.LogStreamStatusSealed == lsmd.GetStatus()
	}, 10*time.Second, 100*time.Millisecond)

	_, err = clus.GetVMSClient(t).Unseal(context.Background(), topicID, lsID)
	require.NoError(t, err)

	clus.ClientRefresh(t)
	_, err = clus.ClientAtIndex(t, 0).Append(context.Background(), topicID, []byte("foo"))
	require.NoError(t, err)
}

func TestVarlogFailoverSNBackupFail(t *testing.T) {
	opts := []it.Option{
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithNumberOfClients(5),
		it.WithVMSOptions(it.NewTestVMSOptions()),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		errC := make(chan error, 1024)
		glsnC := make(chan types.GLSN, 1024)

		done := make(chan struct{})
		defer close(done)

		var wg sync.WaitGroup
		for i := 0; i < env.NumberOfClients(); i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				topicID := env.TopicIDs()[0]
				client := env.ClientAtIndex(t, idx)
				for {
					select {
					case <-done:
						return
					default:
					}
					glsn, err := client.Append(context.Background(), topicID, []byte("foo"))
					if err != nil {
						errC <- err
						return
					}
					glsnC <- glsn
				}
			}(i)
		}

		Convey("When backup SN fail", func(ctx C) {
			errCnt := 0
			maxGLSN := types.InvalidGLSN

			topicID := env.TopicIDs()[0]
			lsID := env.LogStreamID(t, topicID, 0)
			backupSNID := env.BackupStorageNodeIDOf(t, lsID)

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
						env.CloseSN(t, backupSNID)
						env.CloseSNClientOf(t, backupSNID)
					}
				case <-errC:
					errCnt++
				}
			}

			Convey("Then it should not be able to append", func(ctx C) {
				rsp, err := env.GetVMSClient(t).Seal(context.Background(), topicID, lsID)
				So(err, ShouldBeNil)
				sealedGLSN := rsp.GetSealedGLSN()
				So(sealedGLSN, ShouldBeGreaterThanOrEqualTo, maxGLSN)
				t.Logf("SealedGLSN=%d", sealedGLSN)

				primarySNID := env.PrimaryStorageNodeIDOf(t, lsID)
				snmd, err := env.SNClientOf(t, primarySNID).GetMetadata(context.Background())
				lsmd, ok := snmd.GetLogStream(lsID)
				So(ok, ShouldBeTrue)
				So(lsmd.GetStatus(), ShouldEqual, varlogpb.LogStreamStatusSealed)

				// check if all clients stopped
				wg.Wait()

				// TODO (jun): Add assertion that HWM of the LS equals to sealedGLSN

				Convey("When backup SN recover", func(ctx C) {
					env.RecoverSN(t, backupSNID)
					env.NewSNClient(t, backupSNID)
					env.NewReportCommitterClient(t, backupSNID)

					So(testutil.CompareWaitN(100, func() bool {
						snmeta, err := env.SNClientOf(t, backupSNID).GetMetadata(context.Background())
						if err != nil {
							return false
						}
						lsmd, ok := snmeta.GetLogStream(lsID)
						return ok && lsmd.GetStatus() == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)

					_, err := env.GetVMSClient(t).Unseal(context.TODO(), topicID, lsID)
					So(err, ShouldBeNil)

					Convey("Then it should be able to append", func(ctx C) {
						// FIXME (jun): Explicit refreshing clients is not
						// necessary. Rather automated refreshing metadata
						// and connections in clients should be worked.
						env.ClientRefresh(t)
						client := env.ClientAtIndex(t, 0)
						var err error
						So(testutil.CompareWaitN(10, func() bool {
							_, err = client.Append(context.Background(), topicID, []byte("foo"))
							return err == nil
						}), ShouldBeTrue)
						if err != nil {
							log.Printf("Error=%+v", err)
						}
					})
				})
			})
		})
	}))
}

func TestVarlogFailoverRecoverFromSML(t *testing.T) {
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		leader := env.IndexOfLeaderMR()
		topicID := env.TopicIDs()[0]
		lsID := env.LogStreamID(t, topicID, 0)

		cli := env.NewLogIOClient(t, lsID)
		defer cli.Close()

		var (
			err  error
			glsn types.GLSN
		)
		for i := 0; i < 5; i++ {
			glsn, err = cli.Append(context.TODO(), topicID, lsID, []byte("foo"))
			So(err, ShouldBeNil)
		}
		ver := types.Version(glsn)

		Convey("When MR leader restart", func(ctx C) {
			env.RestartMR(t, leader)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetLastCommitVersion(), ShouldEqual, ver)

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
						meta, err := env.SNClientOf(t, env.StorageNodeIDAtIndex(t, 0)).GetMetadata(context.Background())
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
						cmCli.Unseal(context.TODO(), topicID, lsID)

						rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
						defer cancel()
						recoveredGLSN, err = cli.Append(rctx, topicID, lsID, []byte("foo"))
						return err == nil
					}), ShouldBeTrue)

					So(recoveredGLSN, ShouldEqual, glsn+1)
				})
			})
		})
	}))
}

func TestVarlogFailoverRecoverFromIncompleteSML(t *testing.T) {
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
			ver  types.Version
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
		}
		ver = types.Version(glsn)

		lsID := env.LogStreamID(t, topicID, 0)
		env.WaitCommit(t, lsID, ver)

		Convey("When commit happens during MR close all without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, topicID, lsID, []byte("foo"))
			}

			offset := glsn + 1
			glsn += types.GLSN(nrAppend)
			ver++
			env.CommitWithoutMR(t, lsID, types.LLSN(offset), offset, nrAppend, ver, glsn)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetLastCommitVersion(), ShouldEqual, ver)

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
						cmCli.Unseal(context.TODO(), topicID, lsID)

						rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
						defer cancel()

						recoveredGLSN, err = client.Append(rctx, topicID, []byte("foo"))
						return err == nil
					}), ShouldBeTrue)

					So(recoveredGLSN, ShouldEqual, glsn+1)
				})
			})
		})
	}))
}

func TestVarlogFailoverRecoverFromIncompleteSMLWithEmptyCommit(t *testing.T) {
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(2),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 10

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
			ver  types.Version
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
		}
		ver = types.Version(glsn)

		for _, lsID := range env.LogStreamIDs(topicID) {
			env.WaitCommit(t, lsID, ver)
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
				for _, lsID := range env.LogStreamIDs(topicID) {
					llsn := env.GetUncommittedLLSNOffset(t, lsID)

					env.AppendUncommittedLog(t, topicID, lsID, []byte("foo"))

					ver++
					glsn++
					env.CommitWithoutMR(t, lsID, llsn, glsn+types.GLSN(1), 1, ver, glsn)

					env.WaitCommit(t, lsID, ver)
				}
			}

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetLastCommitVersion(), ShouldEqual, ver)

				crs, _ := mr.LookupNextCommitResults(ver - 1)
				So(crs, ShouldNotBeNil)

				for _, lsID := range env.LogStreamIDs(topicID) {
					cr, _, ok := crs.LookupCommitResult(topicID, lsID, -1)
					So(ok, ShouldBeTrue)

					llsn := env.GetUncommittedLLSNOffset(t, lsID)

					So(cr.CommittedLLSNOffset+types.LLSN(cr.CommittedGLSNLength), ShouldEqual, llsn)

				}
			})
		})
	}))
}

func TestVarlogFailoverSyncLogStream(t *testing.T) {
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
			ver  types.Version
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
		}
		ver = types.Version(glsn)

		lsID := env.LogStreamID(t, topicID, 0)
		env.WaitCommit(t, lsID, ver)

		Convey("When add log stream without writeing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			addedLSID := env.AddLSWithoutMR(t, topicID)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, topicID, addedLSID, []byte("foo"))
			}

			offset := glsn + 1
			glsn += types.GLSN(nrAppend)
			ver++

			env.CommitWithoutMR(t, lsID, types.LLSN(offset), offset, 0, ver, glsn)
			env.WaitCommit(t, lsID, ver)

			env.CommitWithoutMR(t, addedLSID, types.MinLLSN, offset, nrAppend, ver, glsn)
			env.WaitCommit(t, addedLSID, ver)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			Convey("Then it should be recovered", func(ctx C) {
				mr := env.GetMR(t)
				So(mr.GetLastCommitVersion(), ShouldEqual, ver)

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
						cmCli.Unseal(context.TODO(), topicID, addedLSID)

						rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
						defer cancel()

						env.ClientRefresh(t)
						client := env.ClientAtIndex(t, 0)

						recoveredGLSN, err = client.Append(rctx, topicID, []byte("foo"))
						return err == nil
					}), ShouldBeTrue)

					So(recoveredGLSN, ShouldEqual, glsn+1)
				})
			})
		})

		Convey("When update log stream without writing SML", func(ctx C) {
			addedSNID := env.AddSN(t)

			env.CloseMRAllForRestart(t)

			env.UpdateLSWithoutMR(t, topicID, lsID, addedSNID, true)

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
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(1),
		it.WithNumberOfStorageNodes(1),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		snID := env.StorageNodeIDAtIndex(t, 0)
		topicID := env.TopicIDs()[0]
		lsID := env.LogStreamID(t, topicID, 0)

		Convey("When update log stream without writing SML, and do not clear victim replica", func(ctx C) {
			client := env.ClientAtIndex(t, 0)

			var (
				err  error
				glsn types.GLSN
				ver  types.Version
			)
			for i := 0; i < nrAppend; i++ {
				glsn, err = client.Append(context.TODO(), topicID, []byte("foo"))
				So(err, ShouldBeNil)
			}
			ver = types.Version(glsn)
			env.WaitCommit(t, lsID, ver)

			addedSNID := env.AddSN(t)

			env.CloseMRAllForRestart(t)

			env.UpdateLSWithoutMR(t, topicID, lsID, addedSNID, false)

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

			env.UpdateLSWithoutMR(t, topicID, lsID, addedSNID, false)
			env.UnsealWithoutMR(t, topicID, lsID, types.InvalidGLSN)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, topicID, lsID, []byte("foo"))
			}

			glsn := types.GLSN(nrAppend)
			ver := types.MinVersion

			env.CommitWithoutMR(t, lsID, types.MinLLSN, types.MinGLSN, nrAppend, ver, glsn)
			env.WaitCommit(t, lsID, ver)

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
	t.Skip()

	opts := []it.Option{
		it.WithoutWAL(),
		it.WithReplicationFactor(2),
		it.WithNumberOfStorageNodes(2),
		it.WithNumberOfLogStreams(1),
		it.WithMRCount(1),
		it.WithNumberOfClients(1),
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
			ver  types.Version
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
		}
		ver = types.Version(glsn)

		lsID := env.LogStreamID(t, topicID, 0)
		env.WaitCommit(t, lsID, ver)

		Convey("When add log stream incomplete without writeing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			incompleteLSID := env.AddLSIncomplete(t, topicID)

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
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster with StateMachineLog", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		const nrAppend = 5

		meta := env.GetMetadata(t)
		So(meta, ShouldNotBeNil)

		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		var (
			err  error
			glsn types.GLSN
			ver  types.Version
		)
		for i := 0; i < nrAppend; i++ {
			glsn, err = client.Append(context.TODO(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
		}
		ver = types.Version(glsn)

		lsID := env.LogStreamID(t, topicID, 0)
		env.WaitCommit(t, lsID, ver)

		Convey("When remove replica without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			snID := env.StorageNodeIDAtIndex(t, 0)

			snMCL := env.SNClientOf(t, snID)
			snMCL.RemoveLogStream(context.Background(), topicID, lsID)

			env.RecoverMR(t)

			env.HealthCheckForMR(t)

			// TODO:: how to catch panic
			Convey("Then it should not be recovered", func(ctx C) {
			})
		})

		Convey("When remove replica without writing SML", func(ctx C) {
			env.CloseMRAllForRestart(t)

			addedLSID := env.AddLSWithoutMR(t, topicID)

			for i := 0; i < nrAppend; i++ {
				env.AppendUncommittedLog(t, topicID, addedLSID, []byte("foo"))
			}

			offset := glsn + 1
			glsn += types.GLSN(nrAppend)
			ver++
			env.CommitWithoutMR(t, addedLSID, types.MinLLSN, offset, nrAppend, ver, glsn)
			env.WaitCommit(t, addedLSID, ver)

			snID := env.StorageNodeIDAtIndex(t, 0)

			snMCL := env.SNClientOf(t, snID)
			snMCL.RemoveLogStream(context.Background(), topicID, addedLSID)

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
		it.WithNumberOfTopics(1),
	}

	Convey("Given Varlog cluster", t, it.WithTestCluster(t, opts, func(env *it.VarlogCluster) {
		topicID := env.TopicIDs()[0]
		client := env.ClientAtIndex(t, 0)

		for i := 0; i < 32; i++ {
			_, err := client.Append(context.Background(), topicID, []byte("foo"))
			So(err, ShouldBeNil)
		}

		Convey("When SN fail", func(ctx C) {
			var victim types.StorageNodeID
			var updateLS types.LogStreamID
			for _, lsID := range env.LogStreamIDs(topicID) {
				sn := env.PrimaryStorageNodeIDOf(t, lsID)
				snCL := env.SNClientOf(t, sn)
				snmd, _ := snCL.GetMetadata(context.TODO())
				if len(snmd.GetLogStreams()) == 1 {
					updateLS = lsID
					victim = sn
					break
				}

				sn = env.BackupStorageNodeIDOf(t, lsID)
				snCL = env.SNClientOf(t, sn)
				snmd, _ = snCL.GetMetadata(context.TODO())
				if len(snmd.GetLogStreams()) == 1 {
					updateLS = lsID
					victim = sn
					break
				}
			}

			addedSN := env.AddSN(t)

			env.CloseSN(t, victim)
			env.CloseSNClientOf(t, victim)

			for i := 0; i < 32; i++ {
				_, err := client.Append(context.Background(), topicID, []byte("foo"))
				So(err, ShouldBeNil)
			}

			Convey("Then it should not be able to append", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					meta := env.GetMetadata(t)
					lsdesc := meta.GetLogStream(updateLS)
					return lsdesc.Status == varlogpb.LogStreamStatusSealed
				}), ShouldBeTrue)

				env.UpdateLS(t, topicID, updateLS, victim, addedSN)

				for i := 0; i < 32; i++ {
					_, err := client.Append(context.Background(), topicID, []byte("foo"))
					So(err, ShouldBeNil)
				}

				Convey("When backup SN recover", func(ctx C) {
					env.RecoverSN(t, victim)
					env.NewSNClient(t, victim)
					env.NewReportCommitterClient(t, victim)

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
						_, err := client.Append(context.Background(), topicID, []byte("foo"))
						So(err, ShouldBeNil)
					}
				})
			})
		})
	}))
}
