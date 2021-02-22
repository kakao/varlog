package management

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	assert "github.com/smartystreets/assertions"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/vms"
	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil/ports"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/test"
	"github.daumkakao.com/varlog/varlog/vtesting"
)

func TestVarlogManagerServer(t *testing.T) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.HeartbeatTimeout *= 10
	vmsOpts.Logger = zap.L()
	opts := test.VarlogClusterOptions{
		NrMR:              1,
		NrRep:             3,
		ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		VMSOpts:           &vmsOpts,
	}

	Convey("AddStorageNode", t, test.WithTestCluster(opts, func(env *test.VarlogCluster) {
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
			snopts.Logger = env.Logger()
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
			vmsOpts.InitialMRConnRetryCount = 30
			vmsOpts.InitialMRConnRetryBackoff = 1 * time.Second
			vmsOpts.MetadataRepositoryAddresses = []string{mrRPCAddr}

			// vms
			mrmC := make(chan vms.MetadataRepositoryManager, 1)
			go func() {
				defer close(mrmC)
				mrm, err := vms.NewMRManager(context.TODO(), clusterID, vmsOpts.MRManagerOptions, zap.L())
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
			mrOpts := &metadata_repository.MetadataRepositoryOptions{
				RaftOptions: metadata_repository.RaftOptions{
					Join:      false,
					SnapCount: uint64(10),
					RaftTick:  vtesting.TestRaftTick(),
					RaftDir:   filepath.Join(t.TempDir(), "raftdir"),
					Peers:     []string{mrRAFTAddr},
				},

				ClusterID:         clusterID,
				RaftAddress:       mrRAFTAddr,
				LogDir:            filepath.Join(t.TempDir(), "log"),
				RPCTimeout:        vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
				NumRep:            1,
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
		opts := test.VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		vmsOpts.InitialMRConnRetryCount = 3
		opts.VMSOpts = &vmsOpts
		env := test.NewVarlogCluster(opts)
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
		opts := test.VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		vmsOpts := vms.DefaultOptions()
		opts.VMSOpts = &vmsOpts
		env := test.NewVarlogCluster(opts)
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

			err = mrm.AddPeer(rctx, env.MRIDs[1], env.MRPeers[1], env.MRRPCEndpoints[1])
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(50, func() bool {
				return nmr.IsMember()
			}), ShouldBeTrue)

			rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err = mrm.RemovePeer(rctx, env.MRIDs[0])
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
