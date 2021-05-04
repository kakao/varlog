package cluster

import (
	"context"
	"errors"
	"io"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/smartystreets/assertions"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/test"
	"github.com/kakao/varlog/vtesting"
)

func TestMetadataRepositoryClientSimpleRegister(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewEmptyStorageNodeClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}
		env := test.NewVarlogCluster(t, opts)
		env.Start(t)

		defer env.Close(t)

		mr := env.GetMR(t)
		So(testutil.CompareWaitN(10, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		addr := mr.GetServerAddr()

		cli, err := mrc.NewMetadataRepositoryClient(context.TODO(), addr)
		So(err, ShouldBeNil)
		defer cli.Close()

		Convey("When register storage node by CLI", func(ctx C) {
			snId := types.StorageNodeID(time.Now().UnixNano())

			s := &varlogpb.StorageDescriptor{
				Path:  "test",
				Used:  0,
				Total: 100,
			}
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snId,
				Address:       "localhost",
			}
			sn.Storages = append(sn.Storages, s)

			err = cli.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)

			Convey("Then getting storage node info from metadata should be success", func(ctx C) {
				meta, err := cli.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(meta.GetStorageNode(snId), ShouldNotEqual, nil)
			})
		})
	})
}

func TestVarlogRegisterStorageNode(t *testing.T) {
	Convey("Given Varlog cluster", t, func() {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			Convey("When register SN & LS", func(ctx C) {
				_ = env.AddSN(t)

				_ = env.AddLS(t)

				meta, err := mr.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(meta, ShouldNotBeNil)
				So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
				So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

				Convey("Then nrReport of MR should be updated", func(ctx C) {
					So(testutil.CompareWaitN(10, func() bool {
						return mr.GetReportCount() > 0
					}), ShouldBeTrue)
				})
			})
		}))
	})
}

func TestVarlogAppendLogManyLogStreams(t *testing.T) {
	Convey("Append with many log streams", t, func() {
		const (
			numClient = 10
			numSN     = 2
			numLS     = 10
			numAppend = 100
		)

		vmsOpts := vms.DefaultOptions()
		vmsOpts.HeartbeatTimeout *= 10
		vmsOpts.Logger = zap.L()

		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 2,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
			VMSOpts:               &vmsOpts,
		}

		Convey("Varlog cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			defer func() {
				log.Println("closing")
				env.Close(t)
			}()

			for i := 0; i < numSN; i++ {
				log.Printf("addSN(%d)", i)
				_ = env.AddSN(t)
			}

			for i := 0; i < numLS; i++ {
				log.Printf("addLS(%d)", i)
				_ = env.AddLS(t)
			}

			mrAddr := env.GetMR(t).GetServerAddr()

			maxGLSNs := make([]types.GLSN, numClient)
			var wg sync.WaitGroup
			wg.Add(numClient)
			for i := 0; i < numClient; i++ {
				go func(idx int) {
					defer wg.Done()

					client, err := varlog.Open(context.TODO(), env.ClusterID, []string{mrAddr})
					if err != nil {
						t.Error(err)
					}
					defer client.Close()
					for i := 0; i < numAppend; i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
						glsn, err := client.Append(ctx, []byte("foo"))
						if err == nil {
							log.Printf("append(%d) glsn=%v", idx, glsn)
							maxGLSNs[idx] = glsn
						}
						cancel()
						// FIXME: As a workaround for difficult issue, below
						// assertions are commented out.
						// Please see: VARLOG-418
						/*
							if err != nil {
								t.Error(err)
							}
							if glsn == types.InvalidGLSN {
								t.Error(glsn)
							}
						*/
					}
				}(i)
			}

			log.Println("waiting")
			wg.Wait()
			log.Println("waiting done")

			maxGLSN := types.InvalidGLSN
			for _, mg := range maxGLSNs {
				if mg > maxGLSN {
					maxGLSN = mg
				}
			}

			// FIXME: If some append operations above fail, the max of GLSN is not
			// expected value.
			log.Printf("<TODO> MaxGLSN: expected=%d, actual=%d", numClient*numAppend, maxGLSN)

			client, err := varlog.Open(context.TODO(), env.ClusterID, []string{mrAddr})
			So(err, ShouldBeNil)
			defer client.Close()

			subC := make(chan types.GLSN, maxGLSN)
			onNext := func(logEntry types.LogEntry, err error) {
				if err != nil {
					close(subC)
					return
				}
				subC <- logEntry.GLSN
			}
			closer, err := client.Subscribe(context.TODO(), 1, maxGLSN+1, onNext, varlog.SubscribeOption{})
			So(err, ShouldBeNil)
			defer closer()

			expectedGLSN := types.GLSN(1)
			for sub := range subC {
				So(sub, ShouldEqual, expectedGLSN)
				expectedGLSN++
			}
			So(expectedGLSN, ShouldEqual, maxGLSN+1)
		}))
	})
}

func TestVarlogAppendLog(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			_ = env.AddSN(t)

			lsID := env.AddLS(t)

			meta, _ := mr.GetMetadata(context.TODO())
			So(meta, ShouldNotBeNil)

			Convey("When Append log entry to SN", func(ctx C) {
				cli := env.NewLogIOClient(t, lsID)
				defer cli.Close()

				Convey("Then it should return valid GLSN", func(ctx C) {
					for i := 0; i < 100; i++ {
						glsn, err := cli.Append(context.TODO(), lsID, []byte("foo"))
						So(err, ShouldBeNil)
						So(glsn, ShouldEqual, types.GLSN(i+1))
					}
				})
			})
		}))
	})
}

func TestVarlogReplicateLog(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		const nrRep = 2
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 nrRep,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}
		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			for i := 0; i < nrRep; i++ {
				_ = env.AddSN(t)
			}

			lsID := env.AddLS(t)

			meta, _ := mr.GetMetadata(context.TODO())
			So(meta, ShouldNotBeNil)

			Convey("When Append log entry to SN", func(ctx C) {
				cli := env.NewLogIOClient(t, lsID)
				defer cli.Close()

				ls := meta.LogStreams[0]

				var backups []logc.StorageNode
				backups = make([]logc.StorageNode, nrRep-1)
				for i := 1; i < nrRep; i++ {
					replicaID := ls.Replicas[i].StorageNodeID
					replica := meta.GetStorageNode(replicaID)
					So(replica, ShouldNotBeNil)

					backups[i-1].ID = replicaID
					backups[i-1].Addr = replica.Address
				}

				Convey("Then it should return valid GLSN", func(ctx C) {
					for i := 0; i < 100; i++ {
						func() {
							rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
							defer cancel()

							glsn, err := cli.Append(rctx, lsID, []byte("foo"), backups...)
							if verrors.IsTransient(err) {
								return
							}
							So(err, ShouldBeNil)
							So(glsn, ShouldEqual, types.GLSN(i+1))
						}()
					}
				})
			})
		}))
	})
}

func TestVarlogSeal(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}
		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			snID := env.AddSN(t)

			sn := env.LookupSN(t, snID)

			lsID := env.AddLS(t)

			meta, _ := mr.GetMetadata(context.TODO())
			So(meta, ShouldNotBeNil)

			Convey("When seal LS", func(ctx C) {
				errC := make(chan error, 1024)
				glsnC := make(chan types.GLSN, 1024)

				appendf := func(actx context.Context) {
					cli := env.NewLogIOClient(t, lsID)
					/*
						if err != nil {
							errC <- err
							return
						}
					*/
					defer cli.Close()

					for {
						select {
						case <-actx.Done():
							return
						default:
							rctx, cancel := context.WithTimeout(actx, vtesting.TimeoutUnitTimesFactor(10))
							glsn, err := cli.Append(rctx, lsID, []byte("foo"))
							cancel()
							if err != nil {
								if err != context.DeadlineExceeded {
									errC <- err
								}
							} else {
								log.Printf("appended: %v", glsn)
								glsnC <- glsn
							}
						}
					}
				}

				runner := runner.New("seal-test", zap.NewNop())
				defer runner.Stop()

				for i := 0; i < 5; i++ {
					runner.Run(appendf)
				}

				var err error
				sealedGLSN := types.InvalidGLSN

				timer := time.NewTimer(vtesting.TimeoutUnitTimesFactor(100))
				defer timer.Stop()

			Loop:
				for {
					select {
					case <-timer.C:
						t.Fatal("timeout")
					case glsn := <-glsnC:
						if sealedGLSN.Invalid() && glsn > types.GLSN(10) {
							log.Printf("SEAL (%v)", glsn)
							sealedGLSN, err = mr.Seal(context.TODO(), lsID)
							So(err, ShouldBeNil)
						}

						if !sealedGLSN.Invalid() {
							status, hwm, err := sn.Seal(context.TODO(), lsID, sealedGLSN)
							So(err, ShouldBeNil)
							if status == varlogpb.LogStreamStatusSealing {
								continue Loop
							}

							So(hwm, ShouldEqual, sealedGLSN)
							break Loop
						}
					case err := <-errC:
						assertions.ShouldWrap(err, verrors.ErrSealed)
					}
				}

				Convey("Then it should be able to read all logs which is lower than sealedGLSN", func(ctx C) {
					cli := env.NewLogIOClient(t, lsID)
					defer cli.Close()

					for glsn := types.MinGLSN; glsn <= sealedGLSN; glsn += types.GLSN(1) {
						_, err := cli.Read(context.TODO(), lsID, glsn)
						So(err, ShouldBeNil)
					}
				})
			})
		}))
	})
}

func TestVarlogTrimGLS(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		nrLS := 2
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			SnapCount:             10,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			_ = env.AddSN(t)

			var lsIDs []types.LogStreamID
			for i := 0; i < nrLS; i++ {
				lsID := env.AddLS(t)

				lsIDs = append(lsIDs, lsID)
			}

			meta, _ := mr.GetMetadata(context.TODO())
			So(meta, ShouldNotBeNil)

			Convey("When Append Log", func(ctx C) {
				cli := env.NewLogIOClient(t, lsIDs[0])
				defer cli.Close()

				glsn := types.InvalidGLSN
				for i := 0; i < 10; i++ {
					rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
					glsn, _ = cli.Append(rctx, lsIDs[0], []byte("foo"))
					So(glsn, ShouldNotEqual, types.InvalidGLSN)
					cancel()
				}

				for i := 0; i < 10; i++ {
					rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
					glsn, _ = cli.Append(rctx, lsIDs[1], []byte("foo"))
					So(glsn, ShouldNotEqual, types.InvalidGLSN)
					cancel()
				}

				hwm := mr.GetHighWatermark()
				So(hwm, ShouldEqual, glsn)

				Convey("Then GLS history of MR should be trimmed", func(ctx C) {
					So(testutil.CompareWaitN(50, func() bool {
						return mr.GetMinHighWatermark() == mr.GetPrevHighWatermark()
					}), ShouldBeTrue)
				})
			})
		}))
	})
}

func TestVarlogTrimGLSWithSealedLS(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		nrLS := 2
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			SnapCount:             10,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			snID := env.AddSN(t)

			sn := env.LookupSN(t, snID)

			var lsIDs []types.LogStreamID
			for i := 0; i < nrLS; i++ {
				lsID := env.AddLS(t)

				lsIDs = append(lsIDs, lsID)
			}

			mr := env.GetMR(t)
			meta, _ := mr.GetMetadata(context.TODO())
			So(meta, ShouldNotBeNil)

			Convey("When Append Log", func(ctx C) {
				cli := env.NewLogIOClient(t, lsIDs[0])
				defer cli.Close()

				glsn := types.InvalidGLSN

				for i := 0; i < 32; i++ {
					rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
					glsn, _ = cli.Append(rctx, lsIDs[i%nrLS], []byte("foo"))
					So(glsn, ShouldNotEqual, types.InvalidGLSN)
					cancel()
				}

				sealedLS := meta.LogStreams[0]
				runningLS := meta.LogStreams[1]

				sealedGLSN, err := mr.Seal(context.TODO(), sealedLS.LogStreamID)
				So(err, ShouldBeNil)

				_, _, err = sn.Seal(context.TODO(), sealedLS.LogStreamID, sealedGLSN)
				So(err, ShouldBeNil)

				for i := 0; i < 10; i++ {
					rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
					glsn, err = cli.Append(rctx, runningLS.LogStreamID, []byte("foo"))
					So(err, ShouldBeNil)
					So(glsn, ShouldNotEqual, types.InvalidGLSN)
					cancel()
				}

				hwm := mr.GetHighWatermark()
				So(hwm, ShouldEqual, glsn)

				Convey("Then GLS history of MR should be trimmed", func(ctx C) {
					So(testutil.CompareWaitN(50, func() bool {
						return mr.GetMinHighWatermark() == mr.GetPrevHighWatermark()
					}), ShouldBeTrue)
				})
			})
		}))
	})
}

type testSnHandler struct {
	hbC     chan types.StorageNodeID
	reportC chan *varlogpb.StorageNodeMetadataDescriptor
}

func newTestSnHandler() *testSnHandler {
	return &testSnHandler{
		hbC:     make(chan types.StorageNodeID),
		reportC: make(chan *varlogpb.StorageNodeMetadataDescriptor),
	}
}

func (sh *testSnHandler) HandleHeartbeatTimeout(ctx context.Context, snID types.StorageNodeID) {
	select {
	case sh.hbC <- snID:
	default:
	}
}

func (sh *testSnHandler) HandleReport(ctx context.Context, sn *varlogpb.StorageNodeMetadataDescriptor) {
	select {
	case sh.reportC <- sn:
	default:
	}
}

func TestVarlogSNWatcher(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		opts.VMSOpts = &vmsOpts

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetServerAddr() != ""
			}), ShouldBeTrue)
			mrAddr := mr.GetServerAddr()

			snID := env.AddSN(t)

			lsID := env.AddLS(t)

			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{mrAddr}
			mrMgr, err := vms.NewMRManager(context.TODO(), env.ClusterID, opts.VMSOpts.MRManagerOptions, env.Logger())
			So(err, ShouldBeNil)

			cmView := mrMgr.ClusterMetadataView()
			snMgr, err := vms.NewStorageNodeManager(context.TODO(), env.ClusterID, cmView, zap.NewNop())
			So(err, ShouldBeNil)

			snHandler := newTestSnHandler()

			wopts := vms.WatcherOptions{
				Tick:             vms.DefaultTick,
				ReportInterval:   vms.DefaultReportInterval,
				HeartbeatTimeout: vms.DefaultHeartbeatTimeout,
				RPCTimeout:       vms.DefaultWatcherRPCTimeout,
			}
			snWatcher := vms.NewStorageNodeWatcher(wopts, cmView, snMgr, snHandler, zap.NewNop())
			snWatcher.Run()
			defer snWatcher.Close()

			Convey("When seal LS", func(ctx C) {
				sn := env.GetPrimarySN(t, lsID)
				_, _, err := sn.Seal(context.TODO(), lsID, types.InvalidGLSN)
				So(err, ShouldBeNil)

				Convey("Then it should be reported by watcher", func(ctx C) {
					So(testutil.CompareWaitN(100, func() bool {
						select {
						case meta := <-snHandler.reportC:
							replica, exist := meta.FindLogStream(lsID)
							return exist && replica.GetStatus().Sealed()
						case <-time.After(vms.DefaultTick * time.Duration(2*vms.DefaultReportInterval)):
						}

						return false
					}), ShouldBeTrue)
				})
			})

			Convey("When close SN", func(ctx C) {
				sn := env.GetPrimarySN(t, lsID)
				env.CloseSN(t, sn.StorageNodeID())

				Convey("Then it should be heartbeat timeout", func(ctx C) {
					So(testutil.CompareWaitN(50, func() bool {
						select {
						case hsnid := <-snHandler.hbC:
							return hsnid == snID
						case <-time.After(vms.DefaultTick * time.Duration(2*vms.DefaultHeartbeatTimeout)):
						}

						return false
					}), ShouldBeTrue)
				})
			})
		}))
	})
}

type dummyCMView struct {
	clusterID types.ClusterID
	addr      string
}

func (cmView *dummyCMView) ClusterMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	cli, err := mrc.NewMetadataRepositoryClient(ctx, cmView.addr)
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	meta, err := cli.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (cmView *dummyCMView) StorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*varlogpb.StorageNodeDescriptor, error) {
	meta, err := cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if sndesc := meta.GetStorageNode(storageNodeID); sndesc != nil {
		return sndesc, nil
	}
	return nil, errors.New("cmview: no such storage node")
}

func TestVarlogStatRepositoryRefresh(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetServerAddr() != ""
			}), ShouldBeTrue)
			mrAddr := mr.GetServerAddr()

			snID := env.AddSN(t)

			lsID := env.AddLS(t)

			cmView := &dummyCMView{
				clusterID: env.ClusterID,
				addr:      mrAddr,
			}
			statRepository := vms.NewStatRepository(context.TODO(), cmView)

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
				lsID2 := env.AddLS(t)

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
	Convey("Given MR cluster", t, func(ctx C) {
		opts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 1,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		}

		Convey("cluster", test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
			mr := env.GetMR(t)

			So(testutil.CompareWaitN(50, func() bool {
				return mr.GetServerAddr() != ""
			}), ShouldBeTrue)
			mrAddr := mr.GetServerAddr()

			snID := env.AddSN(t)

			lsID := env.AddLS(t)

			cmView := &dummyCMView{
				clusterID: env.ClusterID,
				addr:      mrAddr,
			}
			statRepository := vms.NewStatRepository(context.TODO(), cmView)

			So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
			lsStat := statRepository.GetLogStream(lsID)
			So(lsStat.Replicas, ShouldNotBeNil)

			Convey("When Report", func(ctx C) {
				sn := env.LookupSN(t, snID)

				_, _, err := sn.Seal(context.TODO(), lsID, types.InvalidGLSN)
				So(err, ShouldBeNil)

				snm, err := sn.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

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

func TestVarlogLogStreamSync(t *testing.T) {
	t.Skip("[WIP] Sync API")

	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	nrRep := 2
	opts := test.VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 nrRep,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		VMSOpts:               &vmsOpts,
	}
	opts.VMSOpts.HeartbeatTimeout *= 5
	opts.VMSOpts.Logger = zap.L()

	Convey("Given LogStream", t, test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
		for i := 0; i < nrRep; i++ {
			_ = env.AddSN(t)
		}

		lsID := env.AddLS(t)

		meta, err := env.GetVMS().Metadata(context.TODO())
		So(err, ShouldBeNil)

		ls := meta.GetLogStream(lsID)
		So(ls, ShouldNotBeNil)

		var backups []logc.StorageNode
		backups = make([]logc.StorageNode, nrRep-1)
		for i := 1; i < nrRep; i++ {
			replicaID := ls.Replicas[i].StorageNodeID
			replica := meta.GetStorageNode(replicaID)
			So(replica, ShouldNotBeNil)

			backups[i-1].ID = replicaID
			backups[i-1].Addr = replica.Address
		}

		cli := env.NewLogIOClient(t, lsID)
		defer cli.Close()

		for i := 0; i < 100; i++ {
			glsn, err := cli.Append(context.TODO(), lsID, []byte("foo"), backups...)
			So(err, ShouldBeNil)
			So(glsn, ShouldEqual, types.GLSN(i+1))
		}

		Convey("Seal", func(ctx C) {
			result, err := env.GetVMS().Seal(context.TODO(), lsID)
			So(err, ShouldBeNil)

			Convey("Update LS", func(ctx C) {
				newsn := env.AddSN(t)

				victim := result[len(result)-1].StorageNodeID

				// test if victim exists in the logstream and newsn does not exist
				// in the log stream
				meta, err := env.GetMR(t).GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				snidmap := make(map[types.StorageNodeID]bool)
				replicas := meta.GetLogStream(lsID).GetReplicas()
				for _, replica := range replicas {
					snidmap[replica.GetStorageNodeID()] = true
				}
				So(snidmap, ShouldNotContainKey, newsn)
				So(snidmap, ShouldContainKey, victim)

				// update LS
				env.UpdateLS(t, lsID, victim, newsn)

				// test if victim does not exist in the logstream and newsn exists
				// in the log stream
				meta, err = env.GetMR(t).GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				snidmap = make(map[types.StorageNodeID]bool)
				replicas = meta.GetLogStream(lsID).GetReplicas()
				for _, replica := range replicas {
					snidmap[replica.GetStorageNodeID()] = true
				}
				So(snidmap, ShouldContainKey, newsn)
				So(snidmap, ShouldNotContainKey, victim)

				Convey("Then it should be synced", func(ctx C) {
					So(testutil.CompareWaitN(200, func() bool {
						snMeta, err := env.LookupSN(t, newsn).GetMetadata(context.TODO())
						if err != nil {
							return false
						}

						replica, exist := snMeta.FindLogStream(lsID)
						if !exist {
							return false
						}

						return replica.Status == varlogpb.LogStreamStatusSealed
					}), ShouldBeTrue)
				})
			})
		})
	}))
}

func TestVarlogLogStreamIncompleteSeal(t *testing.T) {
	nrRep := 2
	opts := test.VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 nrRep,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
	}

	Convey("Given LogStream", t, test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
		cmcli := env.GetClusterManagerClient()

		for i := 0; i < nrRep; i++ {
			_ = env.AddSN(t)
		}

		lsID := env.AddLS(t)

		meta, err := env.GetVMS().Metadata(context.TODO())
		So(err, ShouldBeNil)

		ls := meta.GetLogStream(lsID)
		So(ls, ShouldNotBeNil)

		Convey("When Seal is incomplete", func(ctx C) {
			// control failedSN for making test condition
			var failedSN *storagenode.StorageNode
			for _, sn := range env.StorageNodes() {
				failedSN = sn
				break
			}

			// remove replica to make Seal LS imcomplete
			err := failedSN.RemoveLogStream(context.TODO(), lsID)
			So(err, ShouldBeNil)

			_, err = cmcli.Seal(context.TODO(), lsID)
			So(err, ShouldNotBeNil)
			//So(len(rsp.GetLogStreams()), ShouldBeLessThan, nrRep)

			Convey("Then SN Watcher make LS sealed", func(ctx C) {
				snmeta, err := failedSN.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				path := snmeta.GetStorageNode().GetStorages()[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)

				_, err = failedSN.AddLogStream(context.TODO(), lsID, path)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(100, func() bool {
					meta, err := env.GetVMS().Metadata(context.TODO())
					if err != nil {
						return false
					}
					ls := meta.GetLogStream(lsID)
					if ls == nil {
						return false
					}

					return ls.Status.Sealed()
				}), ShouldBeTrue)
			})
		})
	}))
}

func TestVarlogLogStreamIncompleteUnseal(t *testing.T) {
	nrRep := 2
	opts := test.VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 nrRep,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
	}

	Convey("Given Sealed LogStream", t, test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
		cmcli := env.GetClusterManagerClient()

		for i := 0; i < nrRep; i++ {
			_ = env.AddSN(t)
		}

		lsID := env.AddLS(t)

		meta, err := env.GetVMS().Metadata(context.TODO())
		So(err, ShouldBeNil)

		ls := meta.GetLogStream(lsID)
		So(ls, ShouldNotBeNil)

		_, err = cmcli.Seal(context.TODO(), lsID)
		So(err, ShouldBeNil)

		Convey("When Unseal is incomplete", func(ctx C) {
			// control failedSN for making test condition
			var failedSN *storagenode.StorageNode
			for _, sn := range env.StorageNodes() {
				failedSN = sn
				break
			}

			// remove replica to make Unseal LS imcomplete
			err := failedSN.RemoveLogStream(context.TODO(), lsID)
			So(err, ShouldBeNil)

			_, err = cmcli.Unseal(context.TODO(), lsID)
			So(err, ShouldNotBeNil)

			Convey("Then SN Watcher make LS sealed", func(ctx C) {
				snmeta, err := failedSN.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				path := snmeta.GetStorageNode().GetStorages()[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)

				_, err = failedSN.AddLogStream(context.TODO(), lsID, path)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(100, func() bool {
					meta, err := failedSN.GetMetadata(context.TODO())
					if err != nil {
						return false
					}

					for _, r := range meta.LogStreams {
						return r.Status.Sealed()
					}

					return false
				}), ShouldBeTrue)
			})
		})
	}))
}

func TestVarlogLogStreamGCZombie(t *testing.T) {
	nrRep := 1
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := test.VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 nrRep,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		VMSOpts:               &vmsOpts,
	}

	opts.VMSOpts.GCTimeout = 6 * time.Duration(opts.VMSOpts.ReportInterval) * opts.VMSOpts.Tick

	Convey("Given Varlog cluster", t, test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
		snID := env.AddSN(t)

		lsID := types.LogStreamID(1)

		Convey("When AddLogStream to SN but do not register MR", func(ctx C) {
			sn := env.LookupSN(t, snID)
			meta, err := sn.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			path := meta.GetStorageNode().GetStorages()[0].GetPath()
			_, err = sn.AddLogStream(context.TODO(), lsID, path)
			So(err, ShouldBeNil)

			meta, err = sn.GetMetadata(context.TODO())
			So(err, ShouldBeNil)

			_, exist := meta.FindLogStream(lsID)
			So(exist, ShouldBeTrue)

			Convey("Then the LogStream should removed after GCTimeout", func(ctx C) {
				time.Sleep(opts.VMSOpts.GCTimeout / 2)
				meta, err := sn.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				_, exist := meta.FindLogStream(lsID)
				So(exist, ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					meta, err := sn.GetMetadata(context.TODO())
					if err != nil {
						return false
					}
					_, exist := meta.FindLogStream(lsID)
					return !exist
				}, opts.VMSOpts.GCTimeout), ShouldBeTrue)
			})
		})
	}))
}

// TODO: This test is intended to test varlog client. It is extracted to `client_test.go`. This test
// function will be removed soon.
func TestVarlogClient(t *testing.T) {
	Convey("Given cluster option", t, func() {
		const (
			nrSN = 3
			nrLS = 3
		)
		vmsOpts := vms.DefaultOptions()
		vmsOpts.HeartbeatTimeout *= 20
		vmsOpts.Logger = zap.L()

		clusterOpts := test.VarlogClusterOptions{
			NrMR:                  1,
			NrRep:                 3,
			ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
			SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
			VMSOpts:               &vmsOpts,
		}

		Convey("and running cluster", test.WithTestCluster(t, clusterOpts, func(env *test.VarlogCluster) {
			defer func() {
				env.Close(t)
			}()

			for i := 0; i < nrSN; i++ {
				_ = env.AddSN(t)
			}

			mrAddr := env.GetMR(t).GetServerAddr()

			Convey("No log stream in the cluster", func() {
				vlg, err := varlog.Open(context.TODO(), env.ClusterID, []string{mrAddr}, varlog.WithLogger(zap.L()), varlog.WithOpenTimeout(5*time.Second))
				So(err, ShouldBeNil)

				_, err = vlg.Append(context.TODO(), []byte("foo"))
				So(err, ShouldNotBeNil)

				So(vlg.Close(), ShouldBeNil)
			})

			Convey("Log stream in the cluster", func() {
				var lsIDs []types.LogStreamID
				for i := 0; i < nrLS; i++ {
					lsID := env.AddLS(t)
					lsIDs = append(lsIDs, lsID)
				}

				vlg, err := varlog.Open(context.TODO(),
					env.ClusterID,
					[]string{mrAddr},
					varlog.WithLogger(zap.L()),
					varlog.WithOpenTimeout(5*time.Second),
				)
				So(err, ShouldBeNil)

				defer func() {
					So(vlg.Close(), ShouldBeNil)
				}()

				// FIXME: This subtest can occurs ErrSealed easily.
				// See VARLOG-418.
				Convey("AppendTo", func() {
					lsID := lsIDs[0]
					glsn, err := vlg.AppendTo(context.TODO(), lsID, []byte("foo"))
					So(err, ShouldBeNil)
					So(glsn, ShouldNotEqual, types.InvalidGLSN)

					bytes, err := vlg.Read(context.TODO(), lsID, glsn)
					So(err, ShouldBeNil)
					So(string(bytes), ShouldEqual, "foo")
				})

				Convey("AppendTo not existing log stream", func() {
					lsID := lsIDs[len(lsIDs)-1] + 1
					_, err := vlg.AppendTo(context.TODO(), lsID, []byte("foo"))
					So(err, ShouldNotBeNil)
				})

				Convey("Append", func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					glsn, err := vlg.Append(ctx, []byte("foo"), varlog.WithRetryCount(nrLS-1))
					if err != nil {
						log.Printf("Varlog error: %+v", err)
					}
					So(err, ShouldBeNil)
					So(glsn, ShouldNotEqual, types.InvalidGLSN)

					var errList []error
					var bytesList []string
					for _, lsID := range lsIDs {
						bytes, err := vlg.Read(context.TODO(), lsID, glsn)
						errList = append(errList, err)
						bytesList = append(bytesList, string(bytes))
					}
					So(errList, ShouldContain, nil)
					So(bytesList, ShouldContain, "foo")
				})

				Convey("Subscribe", func() {
					const (
						numLogs = 10
						minGLSN = types.MinGLSN
					)
					maxGLSN := types.InvalidGLSN
					for i := minGLSN; i < minGLSN+types.GLSN(numLogs); i++ {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						if glsn, err := vlg.Append(ctx, []byte(strconv.Itoa(int(i))), varlog.WithRetryCount(nrLS-1)); err == nil {
							maxGLSN = glsn
						}
						cancel()
					}
					var wg sync.WaitGroup
					wg.Add(1)
					glsnC := make(chan types.GLSN, numLogs)
					onNext := func(logEntry types.LogEntry, err error) {
						if err == io.EOF {
							wg.Done()
							close(glsnC)
							return
						}
						if err != nil {
							t.Error(err)
						}
						glsnC <- logEntry.GLSN
					}
					closer, err := vlg.Subscribe(context.TODO(), minGLSN, maxGLSN, onNext, varlog.SubscribeOption{})
					So(err, ShouldBeNil)
					wg.Wait()
					expectedGLSN := types.GLSN(1)
					for glsn := range glsnC {
						So(glsn, ShouldEqual, expectedGLSN)
						expectedGLSN++
					}
					closer()
				})

				Convey("Trim", func() {
					const (
						numLogs = 10
						minGLSN = types.MinGLSN
					)

					// append
					for i := minGLSN; i < minGLSN+types.GLSN(numLogs); i++ {
						glsn, err := vlg.Append(context.TODO(), []byte(strconv.Itoa(int(i))), varlog.WithRetryCount(nrLS-1))
						So(err, ShouldBeNil)
						So(glsn, ShouldEqual, types.GLSN(i))
					}

					makeOnNext := func(outc chan<- types.LogEntry) varlog.OnNext {
						return func(logEntry types.LogEntry, err error) {
							if err != nil {
								close(outc)
								return
							}
							outc <- logEntry
						}
					}

					// check appended logs
					leC := make(chan types.LogEntry)
					onNext := makeOnNext(leC)
					closer, err := vlg.Subscribe(context.TODO(), minGLSN, minGLSN+types.GLSN(numLogs), onNext, varlog.SubscribeOption{})
					So(err, ShouldBeNil)
					expectedGLSN := minGLSN
					for logEntry := range leC {
						So(logEntry.GLSN, ShouldEqual, expectedGLSN)
						expectedGLSN++
					}
					closer()

					// trim
					until := types.GLSN(numLogs/2) + minGLSN
					err = vlg.Trim(context.TODO(), until, varlog.TrimOption{})
					So(err, ShouldBeNil)

					// actual deletion in SN is asynchronous.
					subscribeTest := testutil.CompareWait100(func() bool {
						errC := make(chan error)
						nopOnNext := func(le types.LogEntry, err error) {
							isErr := err != nil
							errC <- err
							if isErr {
								close(errC)
							}
						}
						closer, err := vlg.Subscribe(context.TODO(), minGLSN, until, nopOnNext, varlog.SubscribeOption{})
						So(err, ShouldBeNil)

						isErr := false
						for err := range errC {
							isErr = isErr || (err != nil && err != io.EOF)
						}
						closer()
						return isErr
					})
					So(subscribeTest, ShouldBeTrue)

					// subscribe remains
					leC = make(chan types.LogEntry)
					onNext = makeOnNext(leC)
					closer, err = vlg.Subscribe(context.TODO(), until+1, minGLSN+types.GLSN(numLogs), onNext, varlog.SubscribeOption{})
					So(err, ShouldBeNil)
					expectedGLSN = until + 1
					for logEntry := range leC {
						So(logEntry.GLSN, ShouldEqual, expectedGLSN)
						expectedGLSN++
					}
					closer()
				})
			})
		}))
	})
}

func TestVarlogRaftDelay(t *testing.T) {
	t.Skip()

	nrRep := 1
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := test.VarlogClusterOptions{
		NrMR:                  3,
		NrRep:                 nrRep,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewEmptyStorageNodeClientFactory(),
		CollectorName:         "otel",
		VMSOpts:               &vmsOpts,
	}

	Convey("Given Varlog cluster", t, test.WithTestCluster(t, opts, func(env *test.VarlogCluster) {
		time.Sleep(10 * time.Minute)
	}))
}
