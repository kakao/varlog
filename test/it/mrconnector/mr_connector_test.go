package test

import (
	"context"
	"net/http"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/mrc/mrconnector"
	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/test/it"
)

func TestMRConnector(t *testing.T) {
	safeMRClose := func(env *it.VarlogCluster, idx int, truncate bool) [2]int {
		if truncate {
			env.CloseMR(t, idx)
		} else {
			So(env.GetMRByIndex(t, idx).Close(), ShouldBeNil)
		}

		numCheckRPC := 0
		ep := env.MRRPCEndpointAtIndex(t, idx)
		So(testutil.CompareWaitN(100, func() bool {
			numCheckRPC++
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()
			// TODO (jun): Use NewConn
			conn, err := rpc.NewBlockingConn(ctx, ep)
			if err == nil {
				So(conn.Close(), ShouldBeNil)
			}
			return err != nil
		}), ShouldBeTrue)

		numCheckRAFT := 0
		peer := env.MRPeerAtIndex(t, idx)
		So(testutil.CompareWaitN(100, func() bool {
			numCheckRAFT++
			_, err := http.Get(peer)
			return err != nil
		}), ShouldBeTrue)

		return [2]int{numCheckRPC, numCheckRAFT}
	}

	// SetDefaultFailureMode(FailureHalts)
	Convey("Given 3 MR nodes", t, func() {
		const clusterInfoFetchInterval = 1 * time.Second
		env := it.NewVarlogCluster(t,
			it.WithMRCount(3),
			it.WithVMSOptions(it.NewTestVMSOptions()),
		)

		Reset(func() {
			for idx := range env.MetadataRepositories() {
				checks := safeMRClose(env, idx, true)
				So(checks[0], ShouldEqual, 1)
				So(checks[1], ShouldEqual, 1)
			}
			env.Close(t)
		})

		for _, rpcEndpoint := range env.MRRPCEndpoints() {
			So(testutil.CompareWaitN(100, func() bool {
				conn, err := rpc.NewConn(context.TODO(), rpcEndpoint)
				if err != nil {
					return false
				}
				defer conn.Close()

				client := grpc_health_v1.NewHealthClient(conn.Conn)
				rsp, err := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
				status := rsp.GetStatus()
				ok := err == nil && status == grpc_health_v1.HealthCheckResponse_SERVING
				if !ok {
					return false
				}

				// NOTE (jun): Does not HealthCheck imply this?
				mcl, err := mrc.NewMetadataRepositoryManagementClientFromRpcConn(conn)
				if err != nil {
					return false
				}
				clusinfo, err := mcl.GetClusterInfo(context.TODO(), env.ClusterID())
				if err != nil {
					return false
				}
				members := clusinfo.GetClusterInfo().GetMembers()
				if len(members) != env.NumberOfMetadataRepositories() {
					return false
				}
				for _, member := range members {
					if member.GetEndpoint() == "" {
						return false
					}
				}
				return true

			}), ShouldBeTrue)
		}

		Convey("When Connector is created", func() {
			mrConn, err := mrconnector.New(context.TODO(),
				mrconnector.WithSeed(env.MRRPCEndpoints()),
				mrconnector.WithClusterID(env.ClusterID()),
				mrconnector.WithUpdateInterval(clusterInfoFetchInterval),
				mrconnector.WithLogger(zap.L()),
			)
			So(err, ShouldBeNil)

			testutil.CompareWaitN(100, func() bool {
				return mrConn.NumberOfMR() == env.NumberOfMetadataRepositories()
			})

			Reset(func() {
				So(mrConn.Close(), ShouldBeNil)
			})

			Convey("and N clients use the connector", func() {
				const (
					n = 10
				)

				maybeFail := func(ctx context.Context) {
					if cl, err := mrConn.Client(); err == nil {
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
						return mrConn.NumberOfMR() == env.NumberOfMetadataRepositories()
					}), ShouldBeTrue)

					for i := 1; i < env.NumberOfMetadataRepositories(); i++ {
						// Close MR: env.MRs[1], env.MRs[2]
						time.Sleep(2 * clusterInfoFetchInterval)
						checks := safeMRClose(env, i, false)
						So(checks[0], ShouldEqual, 1)
						So(checks[1], ShouldEqual, 1)
					}
					close(nextc)
					So(g.Wait(), ShouldBeNil)

					So(testutil.CompareWaitN(100, func() bool {
						// Active MR: env.MRs[0]
						mrs := mrConn.ActiveMRs()
						return len(mrs) == 1 && mrs[env.MetadataRepositoryIDAtIndex(t, 0)] != ""
					}), ShouldBeTrue)
					So(mrConn.NumberOfMR(), ShouldEqual, 1)

					Convey("When the active MR (idx=0) fails", func() {
						So(env.GetMR(t).Close(), ShouldBeNil)
						Convey("Then MRConnector should not work", func() {
							maybeFail(context.TODO())
							_, err := mrConn.Client()
							So(err, ShouldNotBeNil)
						})
					})

					Convey("And the failed MR (idx=1) is recovered", func() {
						So(mrConn.NumberOfMR(), ShouldEqual, 1)
						// Recover MR: env.MRs[1]
						mrIdx := 1
						env.RestartMR(t, mrIdx)
						time.Sleep(2 * clusterInfoFetchInterval)

						So(testutil.CompareWaitN(100, func() bool {
							ep := env.MRRPCEndpointAtIndex(t, mrIdx)
							conn, err := rpc.NewConn(context.TODO(), ep)
							if err != nil {
								return false
							}
							defer conn.Close()

							client := grpc_health_v1.NewHealthClient(conn.Conn)
							rsp, err := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
							status := rsp.GetStatus()
							return err == nil && status == grpc_health_v1.HealthCheckResponse_SERVING
						}), ShouldBeTrue)

						So(testutil.CompareWaitN(100, func() bool {
							// Active MR: env.MRs[0], env.MRs[1]
							mrs := mrConn.ActiveMRs()
							nodeID0 := env.MetadataRepositoryIDAtIndex(t, 0)
							nodeID1 := env.MetadataRepositoryIDAtIndex(t, 1)
							return len(mrs) == 2 && mrs[nodeID0] != "" && mrs[nodeID1] != ""
						}), ShouldBeTrue)

						Convey("Then MRConnector should reconnect to recovered MR", func() {
							// env.MRs[0] failed
							checks := safeMRClose(env, 0, false)
							So(checks[0], ShouldEqual, 1)
							So(checks[1], ShouldEqual, 1)
							maybeFail(context.TODO())

							cl, err := mrConn.Client()
							So(err, ShouldBeNil)
							_, err = cl.GetMetadata(context.TODO())
							So(err, ShouldBeNil)

							// Active MR: env.MRs[1]
							mrs := mrConn.ActiveMRs()
							So(mrs, ShouldHaveLength, 1)
							So(mrs, ShouldContainKey, env.MetadataRepositoryIDAtIndex(t, 1))
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
				_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID())
				So(err, ShouldBeNil)
			})

			Convey("Then Connector.ManagementClient should return the connection", func() {
				mcl, err := mrConn.ManagementClient()
				So(err, ShouldBeNil)
				_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID())
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
					_, err = mcl.GetClusterInfo(context.TODO(), env.ClusterID())
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

						_, err = newMCL.GetClusterInfo(context.TODO(), env.ClusterID())
						So(err, ShouldBeNil)

						So(mrConn.ConnectedNodeID(), ShouldNotEqual, badNodeID)
					})
				})
			})
		})
	})
}
