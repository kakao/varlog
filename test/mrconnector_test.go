package test

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/mrc/mrconnector"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
)

func TestMRConnector(t *testing.T) {
	Convey("Given 3 MR nodes", t, func() {
		opts := VarlogClusterOptions{
			NrMR:              3,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewEmptyReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()

		Reset(func() {
			env.Close()
		})

		for idx, rpcEndpoint := range env.mrRPCEndpoints {
			So(testutil.CompareWaitN(100, func() bool {
				return env.MRs[idx].GetServerAddr() != ""
			}), ShouldBeTrue)

			mrmcl, err := mrc.NewMetadataRepositoryManagementClient(rpcEndpoint)
			So(err, ShouldBeNil)
			So(mrmcl.Close(), ShouldBeNil)

			mrcl, err := mrc.NewMetadataRepositoryClient(rpcEndpoint)
			So(err, ShouldBeNil)
			So(mrcl.Close(), ShouldBeNil)
		}

		Convey("When Connector is created", func() {
			mrc, err := mrconnector.New(context.TODO(), env.mrRPCEndpoints, mrconnector.WithClusterID(env.ClusterID), mrconnector.WithLogger(zap.L()))
			So(err, ShouldBeNil)

			testutil.CompareWaitN(100, func() bool {
				return mrc.NumberOfMR() == env.NrMR
			})

			Reset(func() {
				So(mrc.Close(), ShouldBeNil)
			})

			Convey("Then calling Client should return the connection", func() {
				cl, err := mrc.Client()
				So(err, ShouldBeNil)
				_, err = cl.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				mcl, err := mrc.ManagementClient()
				So(err, ShouldBeNil)
				_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID)
				So(err, ShouldBeNil)
			})

			Convey("Then calling ManagementClient should return the connection", func() {
				mcl, err := mrc.ManagementClient()
				So(err, ShouldBeNil)
				_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID)
				So(err, ShouldBeNil)

				cl, err := mrc.Client()
				So(err, ShouldBeNil)
				_, err = cl.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
			})

			Convey("And the connected client is closed", func() {
				cl, err := mrc.Client()
				So(err, ShouldBeNil)
				So(cl.Close(), ShouldBeNil)

				Convey("Then calling Client or ManagementClient should reestablish the connection", func() {
					cl, err := mrc.Client()
					So(err, ShouldBeNil)
					_, err = cl.GetMetadata(context.TODO())
					So(err, ShouldBeNil)

					mcl, err := mrc.ManagementClient()
					So(err, ShouldBeNil)
					_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID)
					So(err, ShouldBeNil)

					So(mrc.ConnectedNodeID(), ShouldNotEqual, types.InvalidNodeID)
				})

			})

			Convey("And connected MR is failed", func() {
				badCL, err := mrc.Client()
				So(err, ShouldBeNil)

				badCL.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				badNodeID := mrc.ConnectedNodeID()
				So(badNodeID, ShouldNotEqual, types.InvalidNodeID)

				badMR, ok := env.LookupMR(badNodeID)
				So(ok, ShouldBeTrue)
				So(badMR.Close(), ShouldBeNil)

				Convey("Then the client returned from Client should not be able to request", func() {
					_, err := badCL.GetMetadata(context.TODO())
					So(err, ShouldNotBeNil)
					So(badCL.Close(), ShouldBeNil)

					Convey("Then calling Client should reestablish the connection to the new MR node", func() {
						newCL, err := mrc.Client()
						So(err, ShouldBeNil)

						_, err = newCL.GetMetadata(context.TODO())
						So(err, ShouldBeNil)

						newMCL, err := mrc.ManagementClient()
						So(err, ShouldBeNil)

						_, err = newMCL.GetClusterInfo(context.TODO(), env.ClusterID)
						So(err, ShouldBeNil)

						So(mrc.ConnectedNodeID(), ShouldNotEqual, badNodeID)

					})
				})
			})
		})
	})
}
