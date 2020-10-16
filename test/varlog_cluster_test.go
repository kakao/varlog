package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	"github.com/kakao/varlog/pkg/varlog/util/testutil"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"
	vpb "github.com/kakao/varlog/proto/varlog"
	"github.com/kakao/varlog/vtesting"
	"go.uber.org/zap"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMetadataRepositoryClientSimpleRegister(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewEmptyReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.MRs[0]
		addr := mr.GetServerAddr()

		cli, err := varlog.NewMetadataRepositoryClient(addr)
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
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()

		Convey("When register SN & LS", func(ctx C) {
			_, err := env.AddSN()
			So(err, ShouldBeNil)

			_, err = env.AddLS()
			So(err, ShouldBeNil)

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
	})
}

func TestVarlogAppendLog(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()

		_, err := env.AddSN()
		So(err, ShouldBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		meta, _ := mr.GetMetadata(context.TODO())
		So(meta, ShouldNotBeNil)

		Convey("When Append log entry to SN", func(ctx C) {
			cli, err := env.NewLogIOClient(lsID)
			So(err, ShouldBeNil)
			defer cli.Close()

			Convey("Then it should return valid GLSN", func(ctx C) {
				for i := 0; i < 100; i++ {
					glsn, err := cli.Append(context.TODO(), lsID, []byte("foo"))
					So(err, ShouldBeNil)
					So(glsn, ShouldEqual, types.GLSN(i+1))
				}
			})
		})
	})
}

func TestVarlogReplicateLog(t *testing.T) {
	nrRep := 2
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             nrRep,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()

		for i := 0; i < nrRep; i++ {
			_, err := env.AddSN()
			So(err, ShouldBeNil)
		}

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		meta, _ := mr.GetMetadata(context.TODO())
		So(meta, ShouldNotBeNil)

		Convey("When Append log entry to SN", func(ctx C) {
			cli, err := env.NewLogIOClient(lsID)
			So(err, ShouldBeNil)
			defer cli.Close()

			ls := meta.LogStreams[0]

			var backups []varlog.StorageNode
			backups = make([]varlog.StorageNode, nrRep-1)
			for i := 1; i < nrRep; i++ {
				replicaID := ls.Replicas[i].StorageNodeID
				replica := meta.GetStorageNode(replicaID)
				So(replica, ShouldNotBeNil)

				backups[i-1].ID = replicaID
				backups[i-1].Addr = replica.Address
			}

			Convey("Then it should return valid GLSN", func(ctx C) {
				for i := 0; i < 100; i++ {
					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
					defer cancel()

					glsn, err := cli.Append(rctx, lsID, []byte("foo"), backups...)
					if varlog.IsTransientErr(err) {
						continue
					}
					So(err, ShouldBeNil)
					So(glsn, ShouldEqual, types.GLSN(i+1))
				}
			})
		})
	})
}

func TestVarlogSeal(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()

		snID, err := env.AddSN()
		So(err, ShouldBeNil)

		sn := env.LookupSN(snID)
		So(sn, ShouldNotBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		meta, _ := mr.GetMetadata(context.TODO())
		So(meta, ShouldNotBeNil)

		Convey("When seal LS", func(ctx C) {
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
						rctx, cancel := context.WithTimeout(actx, vtesting.TimeoutUnitTimesFactor(10))
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

			runner := runner.New("seal-test", zap.NewNop())
			defer runner.Stop()

			for i := 0; i < 5; i++ {
				runner.Run(appendf)
			}

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
						sealedGLSN, err = mr.Seal(context.TODO(), lsID)
						So(err, ShouldBeNil)
					}

					if !sealedGLSN.Invalid() {
						status, hwm, err := sn.Seal(env.ClusterID, snID, lsID, sealedGLSN)
						So(err, ShouldBeNil)
						if status == varlogpb.LogStreamStatusSealing {
							continue Loop
						}

						So(hwm, ShouldEqual, sealedGLSN)
						break Loop
					}
				case err := <-errC:
					So(err, ShouldEqual, varlog.ErrSealed)
				}
			}

			Convey("Then it should be able to read all logs which is lower than sealedGLSN", func(ctx C) {
				cli, err := env.NewLogIOClient(lsID)
				So(err, ShouldBeNil)
				defer cli.Close()

				for glsn := types.MinGLSN; glsn <= sealedGLSN; glsn += types.GLSN(1) {
					_, err := cli.Read(context.TODO(), lsID, glsn)
					So(err, ShouldBeNil)
				}
			})
		})
	})
}

func TestVarlogTrimGLS(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		nrLS := 2
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			SnapCount:         10,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()

		_, err := env.AddSN()
		So(err, ShouldBeNil)

		var lsIDs []types.LogStreamID
		for i := 0; i < nrLS; i++ {
			lsID, err := env.AddLS()
			So(err, ShouldBeNil)

			lsIDs = append(lsIDs, lsID)
		}

		meta, _ := mr.GetMetadata(context.TODO())
		So(meta, ShouldNotBeNil)

		Convey("When Append Log", func(ctx C) {
			cli, err := env.NewLogIOClient(lsIDs[0])
			So(err, ShouldBeNil)
			defer cli.Close()

			glsn := types.InvalidGLSN
			for i := 0; i < 10; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
				defer cancel()
				glsn, _ = cli.Append(rctx, lsIDs[0], []byte("foo"))
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			for i := 0; i < 10; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
				defer cancel()
				glsn, _ = cli.Append(rctx, lsIDs[1], []byte("foo"))
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			hwm := mr.GetHighWatermark()
			So(hwm, ShouldEqual, glsn)

			Convey("Then GLS history of MR should be trimmed", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return mr.GetMinHighWatermark() == hwm
				}), ShouldBeTrue)
			})
		})
	})
}

func TestVarlogTrimGLSWithSealedLS(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		nrLS := 2
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			SnapCount:         10,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		snID, err := env.AddSN()
		So(err, ShouldBeNil)

		sn := env.LookupSN(snID)
		So(sn, ShouldNotBeNil)

		var lsIDs []types.LogStreamID
		for i := 0; i < nrLS; i++ {
			lsID, err := env.AddLS()
			So(err, ShouldBeNil)

			lsIDs = append(lsIDs, lsID)
		}

		mr := env.GetMR()
		meta, _ := mr.GetMetadata(context.TODO())
		So(meta, ShouldNotBeNil)

		Convey("When Append Log", func(ctx C) {
			cli, err := env.NewLogIOClient(lsIDs[0])
			So(err, ShouldBeNil)
			defer cli.Close()

			glsn := types.InvalidGLSN

			for i := 0; i < 32; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
				defer cancel()
				glsn, _ = cli.Append(rctx, lsIDs[i%nrLS], []byte("foo"))
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			sealedLS := meta.LogStreams[0]
			runningLS := meta.LogStreams[1]

			sealedGLSN, err := mr.Seal(context.TODO(), sealedLS.LogStreamID)
			So(err, ShouldBeNil)

			_, _, err = sn.Seal(env.ClusterID, snID, sealedLS.LogStreamID, sealedGLSN)
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(10))
				defer cancel()
				glsn, err = cli.Append(rctx, runningLS.LogStreamID, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			hwm := mr.GetHighWatermark()
			So(hwm, ShouldEqual, glsn)

			Convey("Then GLS history of MR should be trimmed", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return mr.GetMinHighWatermark() == hwm
				}), ShouldBeTrue)
			})
		})
	})
}

func TestVarlogFailoverMRLeaderFail(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              3,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
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
	Convey("Given Varlog cluster", t, func(ctx C) {
		nrRep := 2
		nrCli := 5
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             nrRep,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		for i := 0; i < nrRep; i++ {
			_, err := env.AddSN()
			So(err, ShouldBeNil)
		}

		lsID, err := env.AddLS()
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

			var backups []varlog.StorageNode
			backups = make([]varlog.StorageNode, nrRep-1)
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
						if varlog.IsTransientErr(err) {
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
						sn, _ := env.GetBackupSN(lsID)
						sn.Close()
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
					status, _, _ := psn.Seal(env.ClusterID, ls.Replicas[0].StorageNodeID, lsID, sealedGLSN)
					return status == varlogpb.LogStreamStatusSealed
				}), ShouldBeTrue)

				_, hwm, _ := psn.Seal(env.ClusterID, ls.Replicas[0].StorageNodeID, lsID, sealedGLSN)
				So(hwm, ShouldEqual, sealedGLSN)
			})
		})
	})
}

func withTestCluster(opts VarlogClusterOptions, f func(env *VarlogCluster)) func() {
	return func() {
		env := NewVarlogCluster(opts)
		env.Start()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()
		mrAddr := mr.GetServerAddr()

		// VMS Server
		_, err := env.RunClusterManager([]string{mrAddr})
		So(err, ShouldBeNil)

		// VMS Client
		cmCli, err := env.NewClusterManagerClient()
		So(err, ShouldBeNil)

		Reset(func() {
			env.Close()
			cmCli.Close()
		})

		f(env)
	}
}

func TestVarlogManagerServer(t *testing.T) {
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             3,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
	}

	Convey("AddStorageNode", t, withTestCluster(opts, func(env *VarlogCluster) {
		nrSN := opts.NrRep

		cmcli := env.GetClusterManagerClient()

		for i := 0; i < nrSN; i++ {
			storageNodeID := types.StorageNodeID(i + 1)
			snopts := &storage.StorageNodeOptions{
				RPCOptions:               storage.RPCOptions{RPCBindAddress: ":0"},
				LogStreamExecutorOptions: storage.DefaultLogStreamExecutorOptions,
				LogStreamReporterOptions: storage.DefaultLogStreamReporterOptions,
				ClusterID:                env.ClusterID,
				StorageNodeID:            storageNodeID,
				Verbose:                  true,
				Logger:                   env.logger,
			}
			sn, err := storage.NewStorageNode(snopts)
			So(err, ShouldBeNil)
			So(sn.Run(), ShouldBeNil)
			env.SNs[storageNodeID] = sn
		}

		defer func() {
			for _, sn := range env.SNs {
				sn.Close()
			}
		}()

		snAddrs := make(map[types.StorageNodeID]string, nrSN)
		for snid, sn := range env.SNs {
			meta, err := sn.GetMetadata(env.ClusterID, snpb.MetadataTypeHeartbeat)
			So(err, ShouldBeNil)
			snAddr := meta.GetStorageNode().GetAddress()
			So(len(snAddr), ShouldBeGreaterThan, 0)
			snAddrs[snid] = snAddr

			snmeta, err := cmcli.AddStorageNode(context.TODO(), snAddr)
			So(err, ShouldBeNil)
			So(snmeta.GetStorageNode().GetStorageNodeID(), ShouldEqual, snid)
		}

		Convey("Duplicated AddStorageNode", func() {
			for _, snAddr := range snAddrs {
				_, err := cmcli.AddStorageNode(context.TODO(), snAddr)
				So(errors.Is(err, varlog.ErrStorageNodeAlreadyExists), ShouldBeTrue)
			}
		})

		Convey("AddLogStream - Simple", func() {
			logStreamDesc, err := cmcli.AddLogStream(context.TODO(), nil)
			So(err, ShouldBeNil)
			So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)

			// pass since NrRep equals to NrSN
			var snidList []types.StorageNodeID
			for _, replica := range logStreamDesc.GetReplicas() {
				snidList = append(snidList, replica.GetStorageNodeID())
			}
			for snid := range env.SNs {
				So(snidList, ShouldContain, snid)
			}

			clusmeta, err := env.GetMR().GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(clusmeta.GetLogStream(logStreamDesc.GetLogStreamID()), ShouldNotBeNil)

			Convey("Seal", func() {
				lsmetaList, err := cmcli.Seal(context.TODO(), logStreamDesc.GetLogStreamID())
				So(err, ShouldBeNil)
				So(len(lsmetaList), ShouldEqual, opts.NrRep)
				So(lsmetaList[0].HighWatermark, ShouldEqual, types.InvalidGLSN)

				// check MR metadata: sealed
				clusmeta, err := env.GetMR().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				lsdesc := clusmeta.GetLogStream(logStreamDesc.GetLogStreamID())
				So(lsdesc, ShouldNotBeNil)
				So(lsdesc.GetStatus(), ShouldEqual, vpb.LogStreamStatusSealed)

				// check SN metadata: sealed
				for _, sn := range env.SNs {
					snmeta, err := sn.GetMetadata(env.ClusterID, snpb.MetadataTypeLogStreams)
					So(err, ShouldBeNil)
					So(snmeta.GetLogStreams()[0].GetStatus(), ShouldEqual, vpb.LogStreamStatusSealed)
				}
			})
		})

		// replicas to add log stream
		var replicas []*vpb.ReplicaDescriptor
		for snid := range env.SNs {
			replica := &vpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          "/tmp",
			}
			replicas = append(replicas, replica)
		}

		Convey("AddLogStream - Manual", func() {
			logStreamDesc, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldBeNil)
			So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)

			// pass since NrRep equals to NrSN
			var snidList []types.StorageNodeID
			for _, replica := range logStreamDesc.GetReplicas() {
				snidList = append(snidList, replica.GetStorageNodeID())
			}
			for snid := range env.SNs {
				So(snidList, ShouldContain, snid)
			}

			clusmeta, err := env.GetMR().GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(clusmeta.GetLogStream(logStreamDesc.GetLogStreamID()), ShouldNotBeNil)
		})

		Convey("AddLogStream - Error: no such StorageNodeID", func() {
			rand.Seed(time.Now().UnixNano())
			i := rand.Intn(len(replicas))
			// invalid storage node id
			replicas[i].StorageNodeID += types.StorageNodeID(nrSN)

			_, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
		})

		Convey("AddLogStream - Error: duplicated, but not registered logstream", func() {
			// Add logstream to storagenode
			badSNID := types.StorageNodeID(1)
			badSN := env.SNs[badSNID]
			_, err := badSN.AddLogStream(env.ClusterID, badSNID, types.LogStreamID(1), "/tmp")
			So(err, ShouldBeNil)

			// Not registered logstream
			clusmeta, err := env.GetMR().GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(clusmeta.GetLogStream(types.LogStreamID(1)), ShouldBeNil)

			_, err = cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
			So(varlog.IsTransientErr(err), ShouldBeTrue)

			Convey("AddLogStreamx - OK: due to LogStreamIDGenerator.Refresh", func() {
				logStreamDesc, err := cmcli.AddLogStream(context.TODO(), replicas)
				So(err, ShouldBeNil)
				So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)

				clusmeta, err := env.GetMR().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(clusmeta.GetLogStream(logStreamDesc.GetLogStreamID()), ShouldNotBeNil)
			})

		})
	}))
}

func TestVarlogNewMRManager(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		mr := env.MRs[0]
		So(testutil.CompareWaitN(50, func() bool {
			return mr.IsMember()
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		Convey("When create MRManager with non addrs", func(ctx C) {
			// VMS Server
			_, err := vms.NewMRManager(types.ClusterID(0), nil, zap.L())
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldResemble, varlog.ErrInvalid)
			})
		})

		Convey("When create MRManager with invalid addrs", func(ctx C) {
			// VMS Server
			_, err := vms.NewMRManager(types.ClusterID(0), []string{fmt.Sprintf("%s%d", mrAddr, 0)}, zap.L())
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with valid addrs", func(ctx C) {
			// VMS Server
			mrm, err := vms.NewMRManager(types.ClusterID(0), []string{mrAddr}, zap.L())
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
	Convey("Given MR cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		mr := env.MRs[0]
		So(testutil.CompareWaitN(50, func() bool {
			return mr.IsMember()
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		// VMS Server
		mrm, err := vms.NewMRManager(types.ClusterID(0), []string{mrAddr}, zap.L())
		So(err, ShouldBeNil)
		defer mrm.Close()

		Convey("When all the cluster configuration nodes are changed", func(ctx C) {
			env.AppendMR()
			err := env.StartMR(1)
			So(err, ShouldBeNil)

			nmr := env.MRs[1]

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.AddPeer(rctx, env.mrIDs[1], env.mrPeers[1], env.mrRPCEndpoints[1])
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return nmr.IsMember()
			}), ShouldBeTrue)

			rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.RemovePeer(rctx, env.mrIDs[0])
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return !mr.IsMember()
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(50, func() bool {
				cinfo, err := nmr.GetClusterInfo(context.TODO(), types.ClusterID(0))
				if err != nil {
					return false
				}

				return len(cinfo.GetMembers()) == 1
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

type testSnHandler struct {
	hbC     chan types.StorageNodeID
	reportC chan *vpb.StorageNodeMetadataDescriptor
}

func newTestSnHandler() *testSnHandler {
	return &testSnHandler{
		hbC:     make(chan types.StorageNodeID),
		reportC: make(chan *vpb.StorageNodeMetadataDescriptor),
	}
}

func (sh *testSnHandler) HeartbeatTimeout(snID types.StorageNodeID) {
	select {
	case sh.hbC <- snID:
	default:
	}
}

func (sh *testSnHandler) Report(sn *vpb.StorageNodeMetadataDescriptor) {
	select {
	case sh.reportC <- sn:
	default:
	}
}

func TestVarlogSNWatcher(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.LeaderElected()
		}), ShouldBeTrue)

		mr := env.GetMR()

		So(testutil.CompareWaitN(50, func() bool {
			return mr.IsMember()
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		snID, err := env.AddSN()
		So(err, ShouldBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		mrMgr, err := vms.NewMRManager(env.ClusterID, []string{mrAddr}, env.logger)
		So(err, ShouldBeNil)

		cmView := mrMgr.ClusterMetadataView()
		snMgr := vms.NewStorageNodeManager(cmView, zap.NewNop())

		err = snMgr.Init()
		So(err, ShouldBeNil)

		snHandler := newTestSnHandler()

		wopts := vms.WatcherOptions{
			Tick:             vms.DefaultTick,
			ReportInterval:   vms.DefaultReportInterval,
			HeartbeatTimeout: vms.DefaultHeartbeatTimeout,
		}
		snWatcher := vms.NewStorageNodeWatcher(wopts, cmView, snMgr, snHandler, zap.NewNop())
		snWatcher.Run()
		defer snWatcher.Close()

		Convey("When seal LS", func(ctx C) {
			sn, _ := env.GetPrimarySN(lsID)
			_, _, err := sn.Seal(env.ClusterID, snID, lsID, types.InvalidGLSN)
			So(err, ShouldBeNil)

			Convey("Then it should be reported by watcher", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					select {
					case meta := <-snHandler.reportC:
						return meta.GetLogStreams()[0].GetStatus().Sealed()
					case <-time.After(vms.DefaultTick * time.Duration(2*vms.DefaultReportInterval)):
					}

					return false
				}), ShouldBeTrue)
			})
		})

		Convey("When close SN", func(ctx C) {
			sn, _ := env.GetPrimarySN(lsID)
			sn.Close()

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
	})
}
