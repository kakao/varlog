package test

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/mrc/mrconnector"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
)

func TestMRConnector(t *testing.T) {
	Convey("Given 3 MR nodes", t, func() {
		const clusterInfoFetchInterval = 1 * time.Second
		opts := VarlogClusterOptions{
			NrMR:              3,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewEmptyReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()

		Reset(func() {
			env.Close()
			time.Sleep(3 * time.Second)
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
			mrConn, err := mrconnector.New(context.TODO(), env.mrRPCEndpoints,
				mrconnector.WithClusterID(env.ClusterID),
				mrconnector.WithClusterInfoFetchInterval(clusterInfoFetchInterval),
				mrconnector.WithLogger(zap.L()),
			)
			So(err, ShouldBeNil)

			testutil.CompareWaitN(100, func() bool {
				return mrConn.NumberOfMR() == env.NrMR
			})

			Reset(func() {
				So(mrConn.Close(), ShouldBeNil)
			})

			Convey("and N clients use the connector", func() {
				const (
					n = 10
				)

				maybeFail := func(ctx context.Context) {
					if cl, err := mrConn.Client(); err != nil {
						cl.Close()
					} else {
						if _, err := cl.GetMetadata(ctx); err != nil {
							cl.Close()
						}
					}
				}

				nextc := make(chan struct{})
				g, ctx := errgroup.WithContext(context.TODO())
				for i := 0; i < n; i++ {
					g.Go(func() error {
					ClientLoop:
						for {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-nextc:
								break ClientLoop
							default:
								maybeFail(ctx)
							}
						}
						maybeFail(ctx)

						if cl, err := mrConn.Client(); err != nil {
							cl.Close()
							return err
						} else {
							if _, err := cl.GetMetadata(ctx); err != nil {
								cl.Close()
								return err
							}
						}

						return nil
					})
				}

				Convey("Then tolerable MR failures should be okay", func() {
					So(testutil.CompareWaitN(100, func() bool {
						return mrConn.NumberOfMR() == opts.NrMR
					}), ShouldBeTrue)

					for i := 0; i < opts.NrMR-1; i++ {
						time.Sleep(2 * clusterInfoFetchInterval)
						So(env.MRs[i].Close(), ShouldBeNil)
					}
					close(nextc)
					So(g.Wait(), ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						return mrConn.NumberOfMR() == 1
					}), ShouldBeTrue)

					Convey("When the last MR fails", func() {
						So(env.MRs[opts.NrMR-1].Close(), ShouldBeNil)
						Convey("Then MRConnector should not work", func() {
							maybeFail(context.TODO())
							_, err := mrConn.Client()
							So(err, ShouldNotBeNil)
						})
					})

					Convey("And the failed MR is recovered", func() {
						mrIdx := 0
						So(env.RestartMR(mrIdx), ShouldBeNil)
						time.Sleep(2 * clusterInfoFetchInterval)
						So(testutil.CompareWaitN(100, func() bool {
							mrcl, err := mrc.NewMetadataRepositoryClient(env.mrRPCEndpoints[mrIdx])
							ok := err == nil
							if ok {
								mrcl.Close()
							}
							return ok
						}), ShouldBeTrue)

						So(testutil.CompareWaitN(100, func() bool {
							return mrConn.NumberOfMR() == 2
						}), ShouldBeTrue)

						Convey("Then MRConnector should reconnect to recovered MR", func() {
							So(env.MRs[opts.NrMR-1].Close(), ShouldBeNil)
							maybeFail(context.TODO())
							cl, err := mrConn.Client()
							So(err, ShouldBeNil)
							_, err = cl.GetMetadata(context.TODO())
							So(err, ShouldBeNil)
							So(mrConn.NumberOfMR(), ShouldEqual, 1)
						})
					})
				})
			})

			Convey("Then Connector.Client should return the connection", func() {
				cl, err := mrConn.Client()
				So(err, ShouldBeNil)
				_, err = cl.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				mcl, err := mrConn.ManagementClient()
				So(err, ShouldBeNil)
				_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID)
				So(err, ShouldBeNil)
			})

			Convey("Then Connector.ManagementClient should return the connection", func() {
				mcl, err := mrConn.ManagementClient()
				So(err, ShouldBeNil)
				_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID)
				So(err, ShouldBeNil)

				cl, err := mrConn.Client()
				So(err, ShouldBeNil)
				_, err = cl.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
			})

			Convey("And the connected client is closed", func() {
				cl, err := mrConn.Client()
				So(err, ShouldBeNil)
				So(cl.Close(), ShouldBeNil)

				Convey("Then Connector.Client or Connector.ManagementClient should reestablish the connection", func() {
					cl, err := mrConn.Client()
					So(err, ShouldBeNil)
					_, err = cl.GetMetadata(context.TODO())
					So(err, ShouldBeNil)

					mcl, err := mrConn.ManagementClient()
					So(err, ShouldBeNil)
					_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID)
					So(err, ShouldBeNil)

					So(mrConn.ConnectedNodeID(), ShouldNotEqual, types.InvalidNodeID)
				})
			})

			Convey("And connected MR is failed", func() {
				badCL, err := mrConn.Client()
				So(err, ShouldBeNil)

				badCL.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				badNodeID := mrConn.ConnectedNodeID()
				So(badNodeID, ShouldNotEqual, types.InvalidNodeID)

				badMR, ok := env.LookupMR(badNodeID)
				So(ok, ShouldBeTrue)
				So(badMR.Close(), ShouldBeNil)

				Convey("Then the client of failed MR should not request", func() {
					_, err := badCL.GetMetadata(context.TODO())
					So(err, ShouldNotBeNil)
					So(badCL.Close(), ShouldBeNil)

					Convey("Then Connector.Client should reestablish the connection to the new MR node", func() {
						newCL, err := mrConn.Client()
						So(err, ShouldBeNil)

						_, err = newCL.GetMetadata(context.TODO())
						So(err, ShouldBeNil)

						newMCL, err := mrConn.ManagementClient()
						So(err, ShouldBeNil)

						_, err = newMCL.GetClusterInfo(context.TODO(), env.ClusterID)
						So(err, ShouldBeNil)

						So(mrConn.ConnectedNodeID(), ShouldNotEqual, badNodeID)
					})
				})
			})
		})
	})
}
