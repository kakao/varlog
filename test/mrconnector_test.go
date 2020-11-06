package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/mrc/mrconnector"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/vtesting"
)

func TestMRConnector(t *testing.T) {
	Convey("Given 3 MR nodes", t, func() {
		const (
			clusterID   = types.ClusterID(1)
			numMR       = 3
			portBase    = 10000
			rpcPortBase = 20000
		)

		var peers []string
		var rpcEndpoints []string
		for i := 0; i < numMR; i++ {
			peers = append(peers, fmt.Sprintf("http://127.0.0.1:%d", portBase+i))
			rpcEndpoints = append(rpcEndpoints, fmt.Sprintf("127.0.0.1:%d", rpcPortBase+i))
		}

		mrs := make(map[types.NodeID]metadata_repository.MetadataRepository, numMR)
		for i := 0; i < numMR; i++ {
			nodeID := types.NewNodeIDFromURL(peers[i])
			So(nodeID, ShouldNotEqual, types.InvalidNodeID)

			opts := &metadata_repository.MetadataRepositoryOptions{
				ClusterID:         clusterID,
				RaftAddress:       peers[i],
				Join:              false,
				SnapCount:         10,
				RaftTick:          vtesting.TestRaftTick(),
				RPCTimeout:        vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
				NumRep:            1,
				Peers:             peers,
				RPCBindAddress:    rpcEndpoints[i],
				ReporterClientFac: metadata_repository.NewEmptyReporterClientFactory(),
				Logger:            zap.L(),
			}
			mr := metadata_repository.NewRaftMetadataRepository(opts)
			mrs[nodeID] = mr
			mr.Run()
		}

		for _, rpcEndpoint := range rpcEndpoints {
			mrmcl, err := mrc.NewMetadataRepositoryManagementClient(rpcEndpoint)
			So(err, ShouldBeNil)
			So(mrmcl.Close(), ShouldBeNil)

			mrcl, err := mrc.NewMetadataRepositoryClient(rpcEndpoint)
			So(err, ShouldBeNil)
			So(mrcl.Close(), ShouldBeNil)
		}

		Reset(func() {
			for _, mr := range mrs {
				So(mr.Close(), ShouldBeNil)
			}
			for nodeID := range mrs {
				os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
				os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))
			}
		})

		Convey("When Connector is created", func() {
			mrc, err := mrconnector.New(context.TODO(), rpcEndpoints, mrconnector.WithClusterID(clusterID), mrconnector.WithLogger(zap.L()))
			So(err, ShouldBeNil)

			So(mrc.ConnectedNodeID(), ShouldEqual, types.InvalidNodeID)

			Reset(func() {
				So(mrc.Close(), ShouldBeNil)
			})

			Convey("Then no peers should be connected initially", func() {
				So(mrc.ConnectedNodeID(), ShouldEqual, types.InvalidNodeID)
			})

			Convey("Then calling Client should establish the connection", func() {
				_, err = mrc.Client().GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				_, err = mrc.ManagementClient().GetClusterInfo(context.TODO(), clusterID)
				So(err, ShouldBeNil)
			})

			Convey("Then calling ManagementClient should establish the connection", func() {
				_, err = mrc.ManagementClient().GetClusterInfo(context.TODO(), clusterID)
				So(err, ShouldBeNil)

				_, err = mrc.Client().GetMetadata(context.TODO())
				So(err, ShouldBeNil)
			})

			Convey("And Disconnect is called", func() {
				So(mrc.Disconnect(), ShouldBeNil)
				So(mrc.ConnectedNodeID(), ShouldEqual, types.InvalidNodeID)

				Convey("Then calling Client or ManagementClient should reestablish the connection", func() {
					_, err = mrc.Client().GetMetadata(context.TODO())
					So(err, ShouldBeNil)

					_, err = mrc.ManagementClient().GetClusterInfo(context.TODO(), clusterID)
					So(err, ShouldBeNil)

					So(mrc.ConnectedNodeID(), ShouldNotEqual, types.InvalidNodeID)
				})

			})

			Convey("And connected MR is failed", func() {
				_, err = mrc.Client().GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				nodeID := mrc.ConnectedNodeID()
				So(nodeID, ShouldNotEqual, types.InvalidNodeID)

				mr, ok := mrs[nodeID]
				So(ok, ShouldBeTrue)
				So(mr.Close(), ShouldBeNil)

				Convey("Then the client returned from Client should not be able to request", func() {
					_, err = mrc.Client().GetMetadata(context.TODO())
					So(err, ShouldNotBeNil)

					Convey("Then calling Disconnect and Client should reestablish the connection to the new MR node", func() {
						So(mrc.Disconnect(), ShouldBeNil)
						So(mrc.ConnectedNodeID(), ShouldEqual, types.InvalidNodeID)

						_, err = mrc.Client().GetMetadata(context.TODO())
						So(err, ShouldBeNil)

						_, err = mrc.ManagementClient().GetClusterInfo(context.TODO(), clusterID)
						So(err, ShouldBeNil)

						So(mrc.ConnectedNodeID(), ShouldNotEqual, nodeID)

					})
				})
			})
		})
	})
}
