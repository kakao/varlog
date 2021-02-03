package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	assert "github.com/smartystreets/assertions"
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
	"github.com/kakao/varlog/vtesting"
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

		mr := env.MRs[0]
		So(testutil.CompareWaitN(10, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		addr := mr.GetServerAddr()

		cli, err := mrc.NewMetadataRepositoryClient(addr)
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
			return env.HealthCheck()
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

		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             2,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
			VMSOpts:           &vmsOpts,
		}

		Convey("Varlog cluster", withTestCluster(opts, func(env *VarlogCluster) {
			So(testutil.CompareWaitN(50, func() bool {
				return env.HealthCheck()
			}), ShouldBeTrue)

			for i := 0; i < numSN; i++ {
				_, err := env.AddSNByVMS()
				So(err, ShouldBeNil)
			}

			for i := 0; i < numLS; i++ {
				_, err := env.AddLSByVMS()
				So(err, ShouldBeNil)
			}

			mrAddr := env.GetMR().GetServerAddr()

			var wg sync.WaitGroup
			wg.Add(numClient)
			for i := 0; i < numClient; i++ {
				go func() {
					defer wg.Done()

					client, err := varlog.Open(env.ClusterID, []string{mrAddr})
					if err != nil {
						t.Error(err)
					}
					defer client.Close()
					for i := 0; i < numAppend; i++ {
						glsn, err := client.Append(context.TODO(), []byte("foo"))
						if err != nil {
							t.Error(err)
						}
						if glsn == types.InvalidGLSN {
							t.Error(glsn)
						}
					}
				}()
			}

			wg.Wait()

			client, err := varlog.Open(env.ClusterID, []string{mrAddr})
			So(err, ShouldBeNil)
			defer client.Close()

			subC := make(chan types.GLSN, numClient*numAppend)
			onNext := func(logEntry types.LogEntry, err error) {
				if err != nil {
					close(subC)
					return
				}
				subC <- logEntry.GLSN
			}
			closer, err := client.Subscribe(context.TODO(), 1, numClient*numAppend+1, onNext, varlog.SubscribeOption{})
			So(err, ShouldBeNil)
			defer closer()

			expectedGLSN := types.GLSN(1)
			for sub := range subC {
				So(sub, ShouldEqual, expectedGLSN)
				expectedGLSN++
			}
			So(expectedGLSN, ShouldEqual, numClient*numAppend+1)

			So(env.Close(), ShouldBeNil)
		}))
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
			return env.HealthCheck()
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
			return env.HealthCheck()
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
					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
					defer cancel()

					glsn, err := cli.Append(rctx, lsID, []byte("foo"), backups...)
					if verrors.IsTransient(err) {
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
			return env.HealthCheck()
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
						status, hwm, err := sn.Seal(context.TODO(), lsID, sealedGLSN)
						So(err, ShouldBeNil)
						if status == varlogpb.LogStreamStatusSealing {
							continue Loop
						}

						So(hwm, ShouldEqual, sealedGLSN)
						break Loop
					}
				case err := <-errC:
					// TODO (jun): Sentry error should be fixed.
					// So(err, ShouldEqual, verrors.ErrSealed)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldContainSubstring, "sealed")
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
			return env.HealthCheck()
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
					return mr.GetMinHighWatermark() == mr.GetPrevHighWatermark()
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
			return env.HealthCheck()
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

			_, _, err = sn.Seal(context.TODO(), sealedLS.LogStreamID, sealedGLSN)
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
					return mr.GetMinHighWatermark() == mr.GetPrevHighWatermark()
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
	nrRep := 2
	nrCli := 5
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}

	Convey("Given Varlog cluster", t, withTestCluster(opts, func(env *VarlogCluster) {
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
							replica := storagenode.Replica{
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

func withTestCluster(opts VarlogClusterOptions, f func(env *VarlogCluster)) func() {
	return func() {
		env := NewVarlogCluster(opts)
		env.Start()

		So(testutil.CompareWaitN(10, func() bool {
			return env.HealthCheck()
		}), ShouldBeTrue)

		mr := env.GetMR()
		So(testutil.CompareWaitN(10, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		// VMS Server
		_, err := env.RunClusterManager([]string{mrAddr}, opts.VMSOpts)
		So(err, ShouldBeNil)

		// VMS Client
		cmCli, err := env.NewClusterManagerClient()
		So(err, ShouldBeNil)

		Reset(func() {
			env.Close()
			cmCli.Close()
			testutil.GC()
		})

		f(env)
	}
}

func TestVarlogManagerServer(t *testing.T) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             3,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}

	Convey("AddStorageNode", t, withTestCluster(opts, func(env *VarlogCluster) {
		nrSN := opts.NrRep

		cmcli := env.GetClusterManagerClient()

		for i := 0; i < nrSN; i++ {
			storageNodeID := types.StorageNodeID(i + 1)
			volume, err := storagenode.NewVolume(t.TempDir())
			So(err, ShouldBeNil)

			snopts := storagenode.DefaultOptions()
			snopts.ListenAddress = "127.0.0.1:0"
			snopts.ClusterID = env.ClusterID
			snopts.StorageNodeID = storageNodeID
			snopts.Logger = env.logger
			snopts.Volumes = map[storagenode.Volume]struct{}{volume: {}}

			sn, err := storagenode.NewStorageNode(&snopts)
			So(err, ShouldBeNil)
			So(sn.Run(), ShouldBeNil)
			env.SNs[storageNodeID] = sn
		}

		defer func() {
			for _, sn := range env.SNs {
				sn.Close()
			}
		}()

		var replicas []*varlogpb.ReplicaDescriptor
		snAddrs := make(map[types.StorageNodeID]string, nrSN)
		for snid, sn := range env.SNs {
			meta, err := sn.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			snAddr := meta.GetStorageNode().GetAddress()
			So(len(snAddr), ShouldBeGreaterThan, 0)
			snAddrs[snid] = snAddr

			rsp, err := cmcli.AddStorageNode(context.TODO(), snAddr)
			So(err, ShouldBeNil)
			snmeta := rsp.GetStorageNode()
			So(snmeta.GetStorageNode().GetStorageNodeID(), ShouldEqual, snid)

			storages := snmeta.GetStorageNode().GetStorages()
			So(len(storages), ShouldBeGreaterThan, 0)
			path := storages[0].GetPath()
			So(len(path), ShouldBeGreaterThan, 0)
			replica := &varlogpb.ReplicaDescriptor{
				StorageNodeID: snid,
				Path:          path,
			}
			replicas = append(replicas, replica)
		}

		Convey("UnregisterStorageNode", func() {
			for snid := range env.SNs {
				_, err := cmcli.UnregisterStorageNode(context.TODO(), snid)
				So(err, ShouldBeNil)

				clusmeta, err := env.GetMR().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(clusmeta.GetStorageNode(snid), ShouldBeNil)
			}

			// AddLogStream: ERROR
			_, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
		})

		Convey("Duplicated AddStorageNode", func() {
			for _, snAddr := range snAddrs {
				_, err := cmcli.AddStorageNode(context.TODO(), snAddr)
				So(err, assert.ShouldWrap, verrors.ErrExist)
				So(errors.Is(err, verrors.ErrExist), ShouldBeTrue)
			}
		})

		Convey("AddLogStream - Simple", func() {
			timeCheckSN := env.SNs[1]
			snmeta, err := timeCheckSN.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			updatedAt := snmeta.GetUpdatedTime()

			rsp, err := cmcli.AddLogStream(context.TODO(), nil)
			So(err, ShouldBeNil)
			logStreamDesc := rsp.GetLogStream()
			So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)
			logStreamID := logStreamDesc.GetLogStreamID()

			snmeta, err = timeCheckSN.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(snmeta.GetUpdatedTime(), ShouldNotEqual, updatedAt)
			updatedAt = snmeta.GetUpdatedTime()

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
			So(clusmeta.GetLogStream(logStreamID), ShouldNotBeNil)

			Convey("UnregisterLogStream ERROR: running log stream", func() {
				_, err := cmcli.UnregisterLogStream(context.TODO(), logStreamID)
				So(err, ShouldNotBeNil)
			})

			Convey("Seal", func() {
				rsp, err := cmcli.Seal(context.TODO(), logStreamID)
				So(err, ShouldBeNil)
				lsmetaList := rsp.GetLogStreams()
				So(len(lsmetaList), ShouldEqual, opts.NrRep)
				So(lsmetaList[0].HighWatermark, ShouldEqual, types.InvalidGLSN)

				// check MR metadata: sealed
				clusmeta, err := env.GetMR().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				lsdesc := clusmeta.GetLogStream(logStreamID)
				So(lsdesc, ShouldNotBeNil)
				So(lsdesc.GetStatus(), ShouldEqual, varlogpb.LogStreamStatusSealed)
				So(len(lsdesc.GetReplicas()), ShouldEqual, opts.NrRep)

				// check SN metadata: sealed
				for _, sn := range env.SNs {
					snmeta, err := sn.GetMetadata(context.TODO())
					So(err, ShouldBeNil)
					So(snmeta.GetLogStreams()[0].GetStatus(), ShouldEqual, varlogpb.LogStreamStatusSealed)
					So(snmeta.GetUpdatedTime(), ShouldNotEqual, updatedAt)
				}

				Convey("UnregisterLogStream OK: sealed log stream", func() {
					_, err := cmcli.UnregisterLogStream(context.TODO(), logStreamID)
					So(err, ShouldBeNil)

					clusmeta, err := env.GetMR().GetMetadata(context.TODO())
					So(err, ShouldBeNil)

					So(clusmeta.GetLogStream(logStreamID), ShouldBeNil)
				})

				Convey("Unseal", func() {
					_, err := cmcli.Unseal(context.TODO(), logStreamID)
					So(err, ShouldBeNil)

					clusmeta, err := env.GetMR().GetMetadata(context.TODO())
					So(err, ShouldBeNil)
					lsdesc := clusmeta.GetLogStream(logStreamID)
					So(lsdesc, ShouldNotBeNil)
					So(lsdesc.GetStatus(), ShouldEqual, varlogpb.LogStreamStatusRunning)
				})
			})
		})

		Convey("AddLogStream - Manual", func() {
			rsp, err := cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldBeNil)
			logStreamDesc := rsp.GetLogStream()
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
			badSNID := replicas[0].GetStorageNodeID()
			badSN := env.SNs[badSNID]
			badLSID := types.LogStreamID(1)
			path := replicas[0].GetPath()
			_, err := badSN.AddLogStream(context.TODO(), badLSID, path)
			So(err, ShouldBeNil)

			// Not registered logstream
			clusmeta, err := env.GetMR().GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(clusmeta.GetLogStream(badLSID), ShouldBeNil)

			// FIXME (jun): This test passes due to seqLSIDGen, but it is very fragile.
			_, err = cmcli.AddLogStream(context.TODO(), replicas)
			So(err, ShouldNotBeNil)
			So(verrors.IsTransient(err), ShouldBeTrue)

			Convey("RemoveLogStreamReplica: garbage log stream can be removed", func() {
				_, err := cmcli.RemoveLogStreamReplica(context.TODO(), badSNID, types.LogStreamID(1))
				So(err, ShouldBeNil)
			})

			Convey("AddLogStream - OK: due to LogStreamIDGenerator.Refresh", func() {
				rsp, err := cmcli.AddLogStream(context.TODO(), replicas)
				So(err, ShouldBeNil)
				logStreamDesc := rsp.GetLogStream()
				So(len(logStreamDesc.GetReplicas()), ShouldEqual, opts.NrRep)

				clusmeta, err := env.GetMR().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(clusmeta.GetLogStream(logStreamDesc.GetLogStreamID()), ShouldNotBeNil)

				Convey("RemoveLogStreamReplica: registered log stream cannot be removed", func() {
					logStreamID := logStreamDesc.GetLogStreamID()
					_, err := cmcli.RemoveLogStreamReplica(context.TODO(), badSNID, logStreamID)
					So(err, ShouldNotBeNil)
				})
			})
		})
	}))
}

func TestVarlogNewMRManager(t *testing.T) {
	Convey("Given that MRManager runs without any running MR", t, func(c C) {
		const (
			clusterID  = types.ClusterID(1)
			mrRPCAddr  = "127.0.0.1:20000"
			mrRAFTAddr = "http://127.0.0.1:30000"
		)
		vmsOpts := vms.DefaultOptions()

		Convey("When MR does not start within specified periods", func(c C) {
			vmsOpts.InitialMRConnRetryCount = 1
			vmsOpts.InitialMRConnRetryBackoff = 100 * time.Millisecond
			vmsOpts.MetadataRepositoryAddresses = []string{mrRPCAddr}

			Convey("Then the MRManager should return an error", func(ctx C) {
				_, err := vms.NewMRManager(context.TODO(), clusterID, vmsOpts.MRManagerOptions, zap.L())
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When MR starts within specified periods", func(c C) {
			vmsOpts.InitialMRConnRetryCount = 5
			vmsOpts.InitialMRConnRetryBackoff = 100 * time.Millisecond
			vmsOpts.MetadataRepositoryAddresses = []string{mrRPCAddr}

			// vms
			mrmC := make(chan vms.MetadataRepositoryManager, 1)
			go func() {
				defer close(mrmC)
				mrm, err := vms.NewMRManager(context.TODO(), clusterID, vmsOpts.MRManagerOptions, zap.L())
				c.So(err, ShouldBeNil)
				if err == nil {
					mrmC <- mrm
				}
			}()

			time.Sleep(10 * time.Millisecond)

			// mr
			mrOpts := &metadata_repository.MetadataRepositoryOptions{
				ClusterID:         clusterID,
				RaftAddress:       mrRAFTAddr,
				Join:              false,
				SnapCount:         uint64(10),
				RaftTick:          vtesting.TestRaftTick(),
				RaftDir:           vtesting.TestRaftDir(),
				RPCTimeout:        vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
				NumRep:            1,
				Peers:             []string{mrRAFTAddr},
				RPCBindAddress:    mrRPCAddr,
				ReporterClientFac: metadata_repository.NewReporterClientFactory(),
				Logger:            zap.L(),
			}
			mr := metadata_repository.NewRaftMetadataRepository(mrOpts)
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
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		vmsOpts.InitialMRConnRetryCount = 3
		opts.VMSOpts = &vmsOpts
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		mr := env.MRs[0]
		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		Convey("When create MRManager with non addrs", func(ctx C) {
			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = nil
			// VMS Server
			_, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with invalid addrs", func(ctx C) {
			// VMS Server
			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{fmt.Sprintf("%s%d", mrAddr, 0)}
			_, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
			Convey("Then it should be fail", func(ctx C) {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When create MRManager with valid addrs", func(ctx C) {
			// VMS Server
			opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{mrAddr}
			mrm, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
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
		vmsOpts := vms.DefaultOptions()
		opts.VMSOpts = &vmsOpts
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		mr := env.MRs[0]
		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		// VMS Server
		opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{mrAddr}
		mrm, err := vms.NewMRManager(context.TODO(), types.ClusterID(0), opts.VMSOpts.MRManagerOptions, zap.L())
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

			oldCL, err := mrc.NewMetadataRepositoryManagementClient(mrAddr)
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
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		opts.VMSOpts = &vmsOpts
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.HealthCheck()
		}), ShouldBeTrue)

		mr := env.GetMR()

		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		snID, err := env.AddSN()
		So(err, ShouldBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		opts.VMSOpts.MRManagerOptions.MetadataRepositoryAddresses = []string{mrAddr}
		mrMgr, err := vms.NewMRManager(context.TODO(), env.ClusterID, opts.VMSOpts.MRManagerOptions, env.logger)
		So(err, ShouldBeNil)

		cmView := mrMgr.ClusterMetadataView()
		snMgr, err := vms.NewStorageNodeManager(context.TODO(), env.ClusterID, cmView, zap.NewNop())
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

type dummyCMView struct {
	clusterID types.ClusterID
	addr      string
}

func (cmView *dummyCMView) ClusterMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	cli, err := mrc.NewMetadataRepositoryClient(cmView.addr)
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
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWaitN(10, func() bool {
			return env.HealthCheck()
		}), ShouldBeTrue)

		mr := env.GetMR()

		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		snID, err := env.AddSN()
		So(err, ShouldBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		cmView := &dummyCMView{
			clusterID: env.ClusterID,
			addr:      mrAddr,
		}
		statRepository := vms.NewStatRepository(cmView)

		metaIndex := statRepository.GetAppliedIndex()
		So(metaIndex, ShouldBeGreaterThan, 0)
		So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
		lsStat := statRepository.GetLogStream(lsID)
		So(lsStat.Replicas, ShouldNotBeNil)

		Convey("When varlog cluster is not changed", func(ctx C) {
			Convey("Then refresh the statRepository and nothing happens", func(ctx C) {
				statRepository.Refresh()
				So(metaIndex, ShouldEqual, statRepository.GetAppliedIndex())
			})
		})

		Convey("When AddSN", func(ctx C) {
			snID2, err := env.AddSN()
			So(err, ShouldBeNil)

			Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
				statRepository.Refresh()
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

					ls.Replicas[0].StorageNodeID = snID2

					err = mr.UpdateLogStream(context.TODO(), ls)
					So(err, ShouldBeNil)

					Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
						statRepository.Refresh()
						So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())

						So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
						So(statRepository.GetStorageNode(snID2), ShouldNotBeNil)
						lsStat := statRepository.GetLogStream(lsID)
						So(lsStat.Replicas, ShouldNotBeNil)

						_, ok := lsStat.Replicas[snID]
						So(ok, ShouldBeFalse)
						_, ok = lsStat.Replicas[snID2]
						So(ok, ShouldBeTrue)
					})
				})
			})
		})

		Convey("When AddLS", func(ctx C) {
			lsID2, err := env.AddLS()
			So(err, ShouldBeNil)

			Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
				statRepository.Refresh()
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
				statRepository.Refresh()
				So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())
				metaIndex := statRepository.GetAppliedIndex()

				So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
				lsStat := statRepository.GetLogStream(lsID)
				So(lsStat.Replicas, ShouldNotBeNil)

				Convey("When UnsealLS", func(ctx C) {
					err := mr.Unseal(context.TODO(), lsID)
					So(err, ShouldBeNil)

					Convey("Then refresh the statRepository and it should be updated", func(ctx C) {
						statRepository.Refresh()
						So(metaIndex, ShouldBeLessThan, statRepository.GetAppliedIndex())

						So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
						lsStat := statRepository.GetLogStream(lsID)
						So(lsStat.Replicas, ShouldNotBeNil)
					})
				})
			})
		})
	})
}

func TestVarlogStatRepositoryReport(t *testing.T) {
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
			return env.HealthCheck()
		}), ShouldBeTrue)

		mr := env.GetMR()

		So(testutil.CompareWaitN(50, func() bool {
			return mr.GetServerAddr() != ""
		}), ShouldBeTrue)
		mrAddr := mr.GetServerAddr()

		snID, err := env.AddSN()
		So(err, ShouldBeNil)

		lsID, err := env.AddLS()
		So(err, ShouldBeNil)

		cmView := &dummyCMView{
			clusterID: env.ClusterID,
			addr:      mrAddr,
		}
		statRepository := vms.NewStatRepository(cmView)

		So(statRepository.GetStorageNode(snID), ShouldNotBeNil)
		lsStat := statRepository.GetLogStream(lsID)
		So(lsStat.Replicas, ShouldNotBeNil)

		Convey("When Report", func(ctx C) {
			sn, _ := env.SNs[snID]

			_, _, err := sn.Seal(context.TODO(), lsID, types.InvalidGLSN)
			So(err, ShouldBeNil)

			snm, err := sn.GetMetadata(context.TODO())
			So(err, ShouldBeNil)

			statRepository.Report(snm)

			Convey("Then it should be updated", func(ctx C) {
				lsStat := statRepository.GetLogStream(lsID)
				So(lsStat.Replicas, ShouldNotBeNil)

				r, ok := lsStat.Replicas[snID]
				So(ok, ShouldBeTrue)

				So(r.Status.Sealed(), ShouldBeTrue)

				Convey("When AddSN and refresh the statRepository", func(ctx C) {
					_, err := env.AddSN()
					So(err, ShouldBeNil)
					statRepository.Refresh()

					Convey("Then reported info should be applied", func(ctx C) {
						lsStat := statRepository.GetLogStream(lsID)
						So(lsStat.Replicas, ShouldNotBeNil)

						r, ok := lsStat.Replicas[snID]
						So(ok, ShouldBeTrue)

						So(r.Status.Sealed(), ShouldBeTrue)
					})
				})
			})
		})
	})
}

func TestVarlogLogStreamSync(t *testing.T) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	nrRep := 2
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}
	opts.VMSOpts.HeartbeatTimeout *= 5
	opts.VMSOpts.Logger = zap.L()

	Convey("Given LogStream", t, withTestCluster(opts, func(env *VarlogCluster) {
		for i := 0; i < nrRep; i++ {
			_, err := env.AddSNByVMS()
			So(err, ShouldBeNil)
		}

		lsID, err := env.AddLSByVMS()
		So(err, ShouldBeNil)

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

		cli, err := env.NewLogIOClient(lsID)
		So(err, ShouldBeNil)
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
				newsn, err := env.AddSNByVMS()
				So(err, ShouldBeNil)

				victim := result[len(result)-1].StorageNodeID

				// test if victim exists in the logstream and newsn does not exist
				// in the log stream
				meta, err := env.GetMR().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				snidmap := make(map[types.StorageNodeID]bool)
				replicas := meta.GetLogStream(lsID).GetReplicas()
				for _, replica := range replicas {
					snidmap[replica.GetStorageNodeID()] = true
				}
				So(snidmap, ShouldNotContainKey, newsn)
				So(snidmap, ShouldContainKey, victim)

				// update LS
				err = env.UpdateLSByVMS(lsID, victim, newsn)
				So(err, ShouldBeNil)

				// test if victim does not exist in the logstream and newsn exists
				// in the log stream
				meta, err = env.GetMR().GetMetadata(context.TODO())
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
						snMeta, err := env.LookupSN(newsn).GetMetadata(context.TODO())
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
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
	}

	Convey("Given LogStream", t, withTestCluster(opts, func(env *VarlogCluster) {
		cmcli := env.GetClusterManagerClient()

		for i := 0; i < nrRep; i++ {
			_, err := env.AddSNByVMS()
			So(err, ShouldBeNil)
		}

		lsID, err := env.AddLSByVMS()
		So(err, ShouldBeNil)

		meta, err := env.GetVMS().Metadata(context.TODO())
		So(err, ShouldBeNil)

		ls := meta.GetLogStream(lsID)
		So(ls, ShouldNotBeNil)

		Convey("When Seal is incomplete", func(ctx C) {
			// control failedSN for making test condition
			var failedSN *storagenode.StorageNode
			for _, sn := range env.SNs {
				failedSN = sn
				break
			}

			// remove replica to make Seal LS imcomplete
			err := failedSN.RemoveLogStream(context.TODO(), lsID)
			So(err, ShouldBeNil)

			rsp, err := cmcli.Seal(context.TODO(), lsID)
			So(err, ShouldBeNil)
			So(len(rsp.GetLogStreams()), ShouldBeLessThan, nrRep)

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
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
	}

	Convey("Given Sealed LogStream", t, withTestCluster(opts, func(env *VarlogCluster) {
		cmcli := env.GetClusterManagerClient()

		for i := 0; i < nrRep; i++ {
			_, err := env.AddSNByVMS()
			So(err, ShouldBeNil)
		}

		lsID, err := env.AddLSByVMS()
		So(err, ShouldBeNil)

		meta, err := env.GetVMS().Metadata(context.TODO())
		So(err, ShouldBeNil)

		ls := meta.GetLogStream(lsID)
		So(ls, ShouldNotBeNil)

		_, err = cmcli.Seal(context.TODO(), lsID)
		So(err, ShouldBeNil)

		Convey("When Unseal is incomplete", func(ctx C) {
			// control failedSN for making test condition
			var failedSN *storagenode.StorageNode
			for _, sn := range env.SNs {
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
	opts := VarlogClusterOptions{
		NrMR:              1,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}

	opts.VMSOpts.GCTimeout = 6 * time.Duration(opts.VMSOpts.ReportInterval) * opts.VMSOpts.Tick

	Convey("Given Varlog cluster", t, withTestCluster(opts, func(env *VarlogCluster) {
		snID, err := env.AddSNByVMS()
		So(err, ShouldBeNil)

		lsID := types.LogStreamID(1)

		Convey("When AddLogStream to SN but do not register MR", func(ctx C) {
			sn := env.LookupSN(snID)
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

func TestVarlogClient(t *testing.T) {
	Convey("Given cluster option", t, func() {
		const (
			nrSN = 3
			nrLS = 3
		)
		vmsOpts := vms.DefaultOptions()
		vmsOpts.HeartbeatTimeout *= 20
		vmsOpts.Logger = zap.L()

		clusterOpts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             3,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
			VMSOpts:           &vmsOpts,
		}

		Convey("and running cluster", withTestCluster(clusterOpts, func(env *VarlogCluster) {
			cmcli := env.GetClusterManagerClient()

			for i := 0; i < nrSN; i++ {
				storageNodeID := types.StorageNodeID(i + 1)
				volume, err := storagenode.NewVolume(t.TempDir())
				So(err, ShouldBeNil)

				snopts := storagenode.DefaultOptions()
				snopts.ListenAddress = "127.0.0.1:0"
				snopts.ClusterID = env.ClusterID
				snopts.StorageNodeID = storageNodeID
				snopts.Logger = env.logger
				snopts.Volumes = map[storagenode.Volume]struct{}{volume: {}}

				sn, err := storagenode.NewStorageNode(&snopts)
				So(err, ShouldBeNil)
				So(sn.Run(), ShouldBeNil)
				env.SNs[storageNodeID] = sn
			}

			var replicas []*varlogpb.ReplicaDescriptor
			snAddrs := make(map[types.StorageNodeID]string, nrSN)
			for snid, sn := range env.SNs {
				meta, err := sn.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				snAddr := meta.GetStorageNode().GetAddress()
				So(len(snAddr), ShouldBeGreaterThan, 0)
				snAddrs[snid] = snAddr

				rsp, err := cmcli.AddStorageNode(context.TODO(), snAddr)
				So(err, ShouldBeNil)
				snmeta := rsp.GetStorageNode()
				So(snmeta.GetStorageNode().GetStorageNodeID(), ShouldEqual, snid)

				storages := snmeta.GetStorageNode().GetStorages()
				So(len(storages), ShouldBeGreaterThan, 0)
				path := storages[0].GetPath()
				So(len(path), ShouldBeGreaterThan, 0)
				replica := &varlogpb.ReplicaDescriptor{
					StorageNodeID: snid,
					Path:          path,
				}
				replicas = append(replicas, replica)
			}

			mrAddr := env.GetMR().GetServerAddr()

			Reset(func() {
				for _, sn := range env.SNs {
					sn.Close()
				}
			})

			Convey("No log stream in the cluster", func() {
				Convey("Append", func() {
					vlg, err := varlog.Open(env.ClusterID, []string{mrAddr}, varlog.WithLogger(zap.L()), varlog.WithOpenTimeout(time.Minute))
					So(err, ShouldBeNil)

					_, err = vlg.Append(context.TODO(), []byte("foo"))
					So(err, ShouldNotBeNil)

					So(vlg.Close(), ShouldBeNil)
				})
			})

			Convey("Log stream in the cluster", func() {
				var lsIDs []types.LogStreamID
				for i := 0; i < nrLS; i++ {
					rsp, err := cmcli.AddLogStream(context.TODO(), nil)
					So(err, ShouldBeNil)
					lsID := rsp.GetLogStream().GetLogStreamID()
					lsIDs = append(lsIDs, lsID)
				}

				vlg, err := varlog.Open(env.ClusterID, []string{mrAddr}, varlog.WithLogger(zap.L()), varlog.WithOpenTimeout(time.Minute))
				So(err, ShouldBeNil)

				Reset(func() {
					So(vlg.Close(), ShouldBeNil)
				})

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
					glsn, err := vlg.Append(context.TODO(), []byte("foo"))
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
					for i := minGLSN; i < minGLSN+types.GLSN(numLogs); i++ {
						glsn, err := vlg.Append(context.TODO(), []byte(strconv.Itoa(int(i))))
						So(err, ShouldBeNil)
						So(glsn, ShouldEqual, types.GLSN(i))
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
					closer, err := vlg.Subscribe(context.TODO(), minGLSN, minGLSN+types.GLSN(numLogs), onNext, varlog.SubscribeOption{})
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
						glsn, err := vlg.Append(context.TODO(), []byte(strconv.Itoa(int(i))))
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
	opts := VarlogClusterOptions{
		NrMR:              3,
		NrRep:             nrRep,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		CollectorName:     "otel",
		VMSOpts:           &vmsOpts,
	}

	Convey("Given Varlog cluster", t, withTestCluster(opts, func(env *VarlogCluster) {
		time.Sleep(10 * time.Minute)
	}))
}
