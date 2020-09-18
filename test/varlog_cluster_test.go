package main

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	"github.com/kakao/varlog/pkg/varlog/util/testutil"
	"github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"
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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.GetMR()

		Convey("When register SN & LS", func(ctx C) {
			err := env.AddSN()
			So(err, ShouldBeNil)

			err = env.AddLS()
			So(err, ShouldBeNil)

			meta, err := mr.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(meta, ShouldNotBeNil)
			So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
			So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

			Convey("Then nrReport of MR should be updated", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					return mr.GetReportCount() > 0
				}, time.Second), ShouldBeTrue)
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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.GetMR()

		err := env.AddSN()
		So(err, ShouldBeNil)

		err = env.AddLS()
		So(err, ShouldBeNil)

		meta, err := mr.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta, ShouldNotBeNil)

		So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
		So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

		Convey("When Append log entry to SN", func(ctx C) {
			ls := meta.LogStreams[0]
			So(len(ls.Replicas), ShouldBeGreaterThan, 0)

			r := ls.Replicas[0]
			sn := env.LookupSN(r.StorageNodeID)
			So(sn, ShouldNotBeNil)

			snMeta, err := sn.GetMetadata(env.ClusterID, storage_node.MetadataTypeHeartbeat)
			So(err, ShouldBeNil)

			cli, err := varlog.NewLogIOClient(snMeta.StorageNode.Address)
			So(err, ShouldBeNil)

			Convey("Then it should return valid GLSN", func(ctx C) {
				for i := 0; i < 100; i++ {
					glsn, err := cli.Append(context.TODO(), ls.LogStreamID, []byte("foo"))
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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.GetMR()

		for i := 0; i < nrRep; i++ {
			err := env.AddSN()
			So(err, ShouldBeNil)
		}

		err := env.AddLS()
		So(err, ShouldBeNil)

		meta, err := mr.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta, ShouldNotBeNil)

		So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
		So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

		Convey("When Append log entry to SN", func(ctx C) {
			ls := meta.LogStreams[0]
			So(len(ls.Replicas), ShouldBeGreaterThan, 0)

			r := ls.Replicas[0]
			sn := env.LookupSN(r.StorageNodeID)
			So(sn, ShouldNotBeNil)

			snMeta, err := sn.GetMetadata(env.ClusterID, storage_node.MetadataTypeHeartbeat)
			So(err, ShouldBeNil)

			cli, err := varlog.NewLogIOClient(snMeta.StorageNode.Address)
			So(err, ShouldBeNil)

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
					rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()

					glsn, err := cli.Append(rctx, ls.LogStreamID, []byte("foo"), backups...)
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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.GetMR()

		err := env.AddSN()
		So(err, ShouldBeNil)

		err = env.AddLS()
		So(err, ShouldBeNil)

		meta, err := mr.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta, ShouldNotBeNil)

		So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
		So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

		Convey("When Append log entry to SN", func(ctx C) {
			ls := meta.LogStreams[0]
			So(len(ls.Replicas), ShouldBeGreaterThan, 0)

			r := ls.Replicas[0]
			sn := env.LookupSN(r.StorageNodeID)
			So(sn, ShouldNotBeNil)

			snMeta, err := sn.GetMetadata(env.ClusterID, storage_node.MetadataTypeHeartbeat)
			So(err, ShouldBeNil)

			errC := make(chan error, 1024)
			glsnC := make(chan types.GLSN, 1024)

			appendf := func(actx context.Context) {
				cli, err := varlog.NewLogIOClient(snMeta.StorageNode.Address)
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
						rctx, cancel := context.WithTimeout(actx, time.Second)
						glsn, err := cli.Append(rctx, ls.LogStreamID, []byte("foo"))
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

			var commit []types.GLSN
			sealedGLSN := types.InvalidGLSN
		Loop:
			for {
				select {
				case glsn := <-glsnC:
					commit = append(commit, glsn)

					if sealedGLSN.Invalid() && glsn > types.GLSN(10) {
						sealedGLSN, err = mr.Seal(context.TODO(), ls.LogStreamID)
						So(err, ShouldBeNil)
					}

					if !sealedGLSN.Invalid() {
						status, hwm, err := sn.Seal(env.ClusterID, r.StorageNodeID, ls.LogStreamID, sealedGLSN)
						So(err, ShouldBeNil)
						if status == varlogpb.LogStreamStatusSealing {
							continue Loop
						}

						So(hwm, ShouldEqual, sealedGLSN)
					}
				case err := <-errC:
					So(err, ShouldEqual, varlog.ErrSealed)
					cnt := len(commit)
					if types.GLSN(cnt) == sealedGLSN {
						break Loop
					}
				}
			}

			sort.Slice(commit, func(i, j int) bool { return commit[i] < commit[j] })
			So(commit[len(commit)-1], ShouldEqual, sealedGLSN)
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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.GetMR()

		err := env.AddSN()
		So(err, ShouldBeNil)

		for i := 0; i < nrLS; i++ {
			err = env.AddLS()
			So(err, ShouldBeNil)
		}

		meta, err := mr.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta, ShouldNotBeNil)

		So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
		So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

		Convey("When Append Log", func(ctx C) {
			r := meta.LogStreams[0].Replicas[0]
			sn := env.LookupSN(r.StorageNodeID)
			So(sn, ShouldNotBeNil)

			snMeta, err := sn.GetMetadata(env.ClusterID, storage_node.MetadataTypeHeartbeat)
			So(err, ShouldBeNil)

			cli, err := varlog.NewLogIOClient(snMeta.StorageNode.Address)
			defer cli.Close()

			glsn := types.InvalidGLSN
			for i := 0; i < 10; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), time.Second)
				defer cancel()
				glsn, err = cli.Append(rctx, meta.LogStreams[0].LogStreamID, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			for i := 0; i < 10; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), time.Second)
				defer cancel()
				glsn, err = cli.Append(rctx, meta.LogStreams[1].LogStreamID, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			hwm := mr.GetHighWatermark()
			So(hwm, ShouldEqual, glsn)

			Convey("Then GLS history of MR should be trimmed", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					return mr.GetMinHighWatermark() == hwm-types.GLSN(1)
				}, time.Second), ShouldBeTrue)
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

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.GetMR()

		err := env.AddSN()
		So(err, ShouldBeNil)

		for i := 0; i < nrLS; i++ {
			err = env.AddLS()
			So(err, ShouldBeNil)
		}

		meta, err := mr.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta, ShouldNotBeNil)

		So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
		So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

		Convey("When Append Log", func(ctx C) {
			r := meta.LogStreams[0].Replicas[0]
			sn := env.LookupSN(r.StorageNodeID)
			So(sn, ShouldNotBeNil)

			snMeta, err := sn.GetMetadata(env.ClusterID, storage_node.MetadataTypeHeartbeat)
			So(err, ShouldBeNil)

			cli, err := varlog.NewLogIOClient(snMeta.StorageNode.Address)
			defer cli.Close()

			glsn := types.InvalidGLSN

			for i := 0; i < 32; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), time.Second)
				defer cancel()
				glsn, err = cli.Append(rctx, meta.LogStreams[i%nrLS].LogStreamID, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			sealedLS := meta.LogStreams[0]
			runningLS := meta.LogStreams[1]

			sealedGLSN, err := mr.Seal(context.TODO(), sealedLS.LogStreamID)
			So(err, ShouldBeNil)

			_, _, err = sn.Seal(env.ClusterID, r.StorageNodeID, sealedLS.LogStreamID, sealedGLSN)
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				rctx, cancel := context.WithTimeout(context.TODO(), time.Second)
				defer cancel()
				glsn, err = cli.Append(rctx, runningLS.LogStreamID, []byte("foo"))
				So(err, ShouldBeNil)
				So(glsn, ShouldNotEqual, types.InvalidGLSN)
			}

			hwm := mr.GetHighWatermark()
			So(hwm, ShouldEqual, glsn)

			Convey("Then GLS history of MR should be trimmed", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					return mr.GetMinHighWatermark() == hwm-types.GLSN(1)
				}, time.Second), ShouldBeTrue)
			})
		})
	})
}
