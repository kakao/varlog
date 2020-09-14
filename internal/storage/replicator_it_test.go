package storage

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/testutil/conveyutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// See https://github.com/smartystreets/goconvey/issues/220.
func init() {
	/*
	   The default failure mode is FailureHalts, which causes test execution
	   within a `Convey` block to halt at the first failure. You could use
	   that mode if the test were re-worked to aggregate all results into
	   a collection that was verified after all goroutines have finished.
	   But, as the code stands, you need to use the FailureContinues mode.

	   The following line sets the failure mode for all tests in the package:
	*/

	SetDefaultFailureMode(FailureContinues)
}

func TestReplicatorClientReplicatorService(t *testing.T) {
	Convey("Given that a ReplicatorService is running", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lse := NewMockLogStreamExecutor(ctrl)
		lse.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		rs := NewReplicatorService(types.StorageNodeID(1), lse)

		Convey("And a ReplicatorClient tries to replicate data to it", conveyutil.WithServiceServer(rs, func(server *grpc.Server, addr string) {
			rc, err := NewReplicatorClient(addr, zap.NewNop())
			So(err, ShouldBeNil)

			ctx, cancel := context.WithCancel(context.TODO())
			So(rc.Run(ctx), ShouldBeNil)

			Reset(func() {
				So(rc.Close(), ShouldBeNil)
				cancel()

				rc.(*replicatorClient).mu.RLock()
				mlen := len(rc.(*replicatorClient).m)
				rc.(*replicatorClient).mu.RUnlock()
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

func TestReplicatorClientReplicatorServiceReplicator(t *testing.T) {
	Convey("Given that a ReplicatorService is running", t, func() {
		const lsID = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		replicas := []Replica{}

		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(lsID).AnyTimes()
		rs1 := NewReplicatorService(types.StorageNodeID(1), lse1)

		Convey("And another Replicator is running", conveyutil.WithServiceServer(rs1, func(server *grpc.Server, addr string) {
			replicas = append(replicas, Replica{
				StorageNodeID: types.StorageNodeID(1),
				LogStreamID:   lsID,
				Address:       addr,
			})

			rc2 := NewMockReplicatorClient(ctrl)
			rc2.EXPECT().Close().AnyTimes()

			Convey("And a Replicator tries to replicate data to them", func() {
				replicas = append(replicas, Replica{
					StorageNodeID: types.StorageNodeID(2),
					LogStreamID:   lsID,
					Address:       "1.2.3.4:5", // fake address
				})

				r := NewReplicator(zap.NewNop())
				r.(*replicator).mtxRcm.Lock()
				r.(*replicator).rcm[types.StorageNodeID(2)] = rc2
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
					Convey("Then the Replicator should return a channel haning an error", func() {
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
					Convey("Then the Replicator should return a channel haning an error", func() {
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
