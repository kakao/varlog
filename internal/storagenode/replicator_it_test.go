package storagenode

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/netutil"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil/conveyutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestReplicatorClientReplicatorService(t *testing.T) {
	Convey("Given that a ReplicatorService is running", t, func() {
		const (
			storageNodeID = types.StorageNodeID(1)
			logStreamID   = types.LogStreamID(1)
		)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lse := NewMockLogStreamExecutor(ctrl)
		lse.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
			func(lsid types.LogStreamID) (LogStreamExecutor, bool) {
				if lsid == lse.LogStreamID() {
					return lse, true
				}
				return nil, false
			},
		).AnyTimes()

		rs := NewReplicatorService(types.StorageNodeID(1), lseGetter, nil)

		Convey("And a ReplicatorClient tries to replicate data to it", conveyutil.WithServiceServer(rs, func(server *grpc.Server, addr string) {
			rc, err := NewReplicatorClient(storageNodeID, logStreamID, addr, zap.NewNop())
			So(err, ShouldBeNil)

			ctx, cancel := context.WithCancel(context.TODO())
			So(rc.Run(ctx), ShouldBeNil)

			Reset(func() {
				So(rc.Close(), ShouldBeNil)
				cancel()

				rc.(*replicatorClient).muErrCs.Lock()
				mlen := len(rc.(*replicatorClient).errCs)
				rc.(*replicatorClient).muErrCs.Unlock()
				So(mlen, ShouldBeZeroValue)
			})

			Convey("When the ReplicatorService is stopped before replying to the request", func() {
				Convey("Then the channel returned from ReplicatorClient.Replicate() should send an error", func() {
					wait := make(chan struct{})
					defer close(wait)
					lse.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
						func(context.Context, types.LLSN, []byte) error {
							<-wait
							return nil
						},
					).MaxTimes(1)
					errC := rc.Replicate(context.TODO(), types.LLSN(0), []byte("foo"))
					server.Stop()
					err := <-errC
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the context of ReplicatorClient is canceled", func() {
				Convey("Then the channel returned from ReplicatorClient.Replicate() should send an error", func() {
					wait := make(chan struct{})
					defer close(wait)
					lse.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
						func(context.Context, types.LLSN, []byte) error {
							<-wait
							return nil
						},
					).MaxTimes(1)
					errC := rc.Replicate(context.TODO(), types.LLSN(0), []byte("foo"))
					cancel()
					err := <-errC
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the underlying LogStreamExecutor.Replicate() in the ReplicatorService returns error", func() {
				Convey("Then the channel returned from ReplicatorClient.Replicate() should send an error", func() {
					lse.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(varlog.ErrInternal)
					errC := rc.Replicate(context.TODO(), types.LLSN(0), []byte("foo"))
					err := <-errC
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the underlying LogStreamExecutor.Replicate() in the ReplicatorService succeeds", func() {
				Convey("Then the channel returned from ReplicatorClient.Replicate() should send an nil", func() {
					lse.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
					errC := rc.Replicate(context.TODO(), types.LLSN(0), []byte("foo"))
					err := <-errC
					So(err, ShouldBeNil)
					// NOTE: Force to stop grpc server - GracefulStop in Reset
					// function of conveyutil waits for closing all connections
					// by the clients. It results in hang the test code in
					// stream gRPC.
					server.Stop()
				})
			})
		}))
	})
}

func TestReplicatorIntegration(t *testing.T) {
	Convey("Given that many LSEs for the same LS are running", t, func(c C) {
		const (
			numLSs      = 2
			numSNs      = 3
			repeat      = 100
		)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		logger := zap.L()

		// single instance per SN (variable[storageNodeIdx])
		var rsList []*ReplicatorService
		var servers []*grpc.Server
		var lseGetterList []*MockLogStreamExecutorGetter
		var addrList []string

		for snIdx := 0; snIdx < numSNs; snIdx++ {
			// StorageNodeID
			storageNodeID := types.StorageNodeID(snIdx + 1)

			// LSEGetter (like StorageNode)
			lseGetter := NewMockLogStreamExecutorGetter(ctrl)
			lseGetterList = append(lseGetterList, lseGetter)

			// ReplicatorService
			rs := NewReplicatorService(storageNodeID, lseGetter, nil)
			rsList = append(rsList, rs)

			// RPC Server
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			So(err, ShouldBeNil)
			addrs, err := netutil.GetListenerAddrs(lis.Addr())
			So(err, ShouldBeNil)
			addrList = append(addrList, addrs[0])

			// Register ReplicatorService
			server := grpc.NewServer()
			rs.Register(server)

			servers = append(servers, server)
			go func() {
				err = server.Serve(lis)
				c.So(err, ShouldBeNil)
				logger.Info("StorageNode is stopped", zap.Any("snid", rs.storageNodeID))
			}()
		}

		// for the same LogStream (variable[logStreamIdx][storageNodeIdx])
		var replicasList [][]Replica
		var lseLists [][]*MockLogStreamExecutor

		for lsIdx := 0; lsIdx < numLSs; lsIdx++ {
			logStreamID := types.LogStreamID(lsIdx + 1)

			// List of LogStreamExecutors for the same LogStream
			var lseList []*MockLogStreamExecutor
			var replicas []Replica

			for snIdx := 0; snIdx < numSNs; snIdx++ {
				// StorageNodeID
				storageNodeID := types.StorageNodeID(snIdx + 1)

				// LogStreamExecutor
				lse := NewMockLogStreamExecutor(ctrl)
				lse.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()
				lseList = append(lseList, lse)

				// Replica Info
				replicas = append(replicas, Replica{
					StorageNodeID: storageNodeID,
					LogStreamID:   logStreamID,
					Address:       addrList[snIdx],
				})

			}

			lseLists = append(lseLists, lseList)
			replicasList = append(replicasList, replicas)
		}

		for snIdx := 0; snIdx < numSNs; snIdx++ {
			// LSEGetter (like StorageNode)
			lseGetter := lseGetterList[snIdx]
			var lseList []*MockLogStreamExecutor
			for lsIdx := 0; lsIdx < numLSs; lsIdx++ {
				lse := lseLists[lsIdx][snIdx]
				lseList = append(lseList, lse)
			}
			lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
				func(lsid types.LogStreamID) (LogStreamExecutor, bool) {
					for _, lse := range lseList {
						if lse.LogStreamID() == lsid {
							return lse, true
						}
					}
					return nil, false
				},
			).AnyTimes()
		}

		var replicatorList []Replicator
		for lsIdx := 0; lsIdx < numLSs; lsIdx++ {
			logStreamID := types.LogStreamID(lsIdx + 1)
			replicator := NewReplicator(logStreamID, logger)
			replicatorList = append(replicatorList, replicator)
			replicator.Run(context.TODO())
		}

		Reset(func() {
			// Replicator (Primary)
			for _, replicator := range replicatorList {
				replicator.Close()
			}

			// ReplicatorService gRPC Server (Backup)
			for _, server := range servers {
				server.Stop()
			}
		})

		Convey("Replicate logs, kill a server, and then replicate a log", func() {
			// Replicator logs
			for llsn := types.MinLLSN; llsn < types.LLSN(repeat); llsn++ {
				data := fmt.Sprintf("log_%v", llsn)
				for lsIdx, lseList := range lseLists {
					for _, lse := range lseList {
						lse.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
							func(ctx context.Context, aLLSN types.LLSN, aData []byte) error {
								c.So(aLLSN, ShouldEqual, llsn)
								c.So(string(aData), ShouldEqual, data)
								return nil
							},
						)
					}

					replicator := replicatorList[lsIdx]
					replicas := replicasList[lsIdx]
					errC := replicator.Replicate(context.TODO(), llsn, []byte(data), replicas)
					So(<-errC, ShouldBeNil)
				}

			}

			// first LS is failed
			servers[0].Stop()

			for lsIdx, lseList := range lseLists {
				for _, lse := range lseList {
					lse.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
						func(ctx context.Context, llsn types.LLSN, data []byte) error {
							return nil
						},
					).MaxTimes(1)
				}

				replicator := replicatorList[lsIdx]
				replicas := replicasList[lsIdx]

				errC := replicator.Replicate(context.TODO(), types.LLSN(repeat), []byte("never"), replicas)
				So(<-errC, ShouldNotBeNil)
			}
		})

	})
}

func TestReplicatorClientReplicatorServiceReplicator(t *testing.T) {
	Convey("Given that a ReplicatorService is running", t, func() {
		const logStreamID = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		replicas := []Replica{}

		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
			func(lsid types.LogStreamID) (LogStreamExecutor, bool) {
				if lsid == logStreamID {
					return lse1, true
				}
				return nil, false
			},
		).AnyTimes()

		rs1 := NewReplicatorService(types.StorageNodeID(1), lseGetter, nil)

		Convey("And another Replicator is running", conveyutil.WithServiceServer(rs1, func(server *grpc.Server, addr string) {
			replicas = append(replicas, Replica{
				StorageNodeID: types.StorageNodeID(1),
				LogStreamID:   logStreamID,
				Address:       addr,
			})

			rc2 := NewMockReplicatorClient(ctrl)
			rc2.EXPECT().PeerStorageNodeID().Return(types.StorageNodeID(2)).AnyTimes()
			rc2.EXPECT().Close().AnyTimes()

			Convey("And a Replicator tries to replicate data to them", func() {
				replicas = append(replicas, Replica{
					StorageNodeID: types.StorageNodeID(2),
					LogStreamID:   logStreamID,
					Address:       "1.2.3.4:5", // fake address
				})

				r := NewReplicator(logStreamID, zap.NewNop())
				r.(*replicator).mtxRcm.Lock()
				r.(*replicator).rcm[rc2.PeerStorageNodeID()] = rc2
				r.(*replicator).mtxRcm.Unlock()

				ctx, cancel := context.WithCancel(context.TODO())
				r.Run(ctx)

				Reset(func() {
					r.Close()
					cancel()
				})

				Convey("When no replicas are given to the Replicator", func() {
					Convey("Then the Replicator should return a channel having an error", func() {
						errC := r.Replicate(context.TODO(), types.LLSN(0), []byte("foo"), nil)
						So(<-errC, ShouldNotBeNil)

						errC = r.Replicate(context.TODO(), types.LLSN(0), []byte("foo"), []Replica{})
						So(<-errC, ShouldNotBeNil)
					})
				})

				Convey("When some or all of the replicas are not connected", func() {
					Convey("Then the Replicator should return a channel having an error", func() {
						Convey("This isn't yet implemented", nil)
					})
				})

				Convey("When some or all of the replicas failed to replicate data", func() {
					Convey("Then the Replicator should return a channel having an error", func() {
						lse1.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
						errC2 := make(chan error, 1)
						errC2 <- varlog.ErrInternal
						close(errC2)
						rc2.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(errC2)
						errC := r.Replicate(context.TODO(), types.LLSN(0), []byte("foo"), replicas)
						So(<-errC, ShouldNotBeNil)

					})
				})

				Convey("When some or all of the replicas are timed out", func() {
					Convey("Then the Replicator should return a channel having an error", func() {
						Convey("This isn't yet implemented", nil)
					})
				})

				Convey("When all of the replicas succeed to replicate data", func() {
					Convey("Then the Replicator should return a channel having nil", func() {
						lse1.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
						errC2 := make(chan error, 1)
						errC2 <- nil
						close(errC2)
						rc2.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).Return(errC2)
						errC := r.Replicate(context.TODO(), types.LLSN(0), []byte("foo"), replicas)
						So(<-errC, ShouldBeNil)
					})
				})
			})
		}))
	})
}
