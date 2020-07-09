package storage

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"github.daumkakao.com/varlog/varlog/proto/storage_node/mock"
)

func TestReplicatorClient(t *testing.T) {
	Convey("ReplicatorClient", t, func() {
		const N = 100

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rpcConn := varlog.RpcConn{}
		rc, err := NewReplicatorClientFromRpcConn(&rpcConn)
		So(err, ShouldBeNil)
		mockClient := mock.NewMockReplicatorServiceClient(ctrl)
		rc.(*replicatorClient).rpcClient = mockClient
		mockStream := mock.NewMockReplicatorService_ReplicateClient(ctrl)

		Convey("it should be run and closed", func() {
			mockStream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			mockStream.EXPECT().CloseSend().MinTimes(1)
			mockClient.EXPECT().Replicate(gomock.Any()).DoAndReturn(
				func(context.Context) (pb.ReplicatorService_ReplicateClient, error) {
					return mockStream, nil
				},
			)
			err := rc.Run(context.TODO())
			So(err, ShouldBeNil)

			err = rc.Close()
			So(err, ShouldBeNil)
		})

		Convey("it should not be run if client stream is failed", func() {
			mockClient.EXPECT().Replicate(gomock.Any()).Return(nil, varlog.ErrInternal)
			err := rc.Run(context.TODO())
			So(err, ShouldNotBeNil)
		})

		Convey("it should not replicate when not running", func() {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*10)
			defer cancel()
			errC := rc.Replicate(ctx, types.LLSN(0), []byte("never"))
			err := <-errC
			So(err, ShouldResemble, context.DeadlineExceeded)
		})

		Convey("it should not replicate when closed", func() {
			mockStream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
			mockStream.EXPECT().CloseSend().MinTimes(1)
			mockClient.EXPECT().Replicate(gomock.Any()).DoAndReturn(
				func(context.Context) (pb.ReplicatorService_ReplicateClient, error) {
					return mockStream, nil
				},
			)
			err := rc.Run(context.TODO())
			So(err, ShouldBeNil)

			err = rc.Close()
			So(err, ShouldBeNil)

			ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*10)
			defer cancel()
			errC := rc.Replicate(ctx, types.LLSN(0), []byte("never"))
			err = <-errC
			So(err, ShouldResemble, context.DeadlineExceeded)
		})

		Convey("it should not replicate data when occurred send error", func() {
			mockClient.EXPECT().Replicate(gomock.Any()).DoAndReturn(
				func(context.Context) (pb.ReplicatorService_ReplicateClient, error) {
					return mockStream, nil
				},
			)

			stop := make(chan struct{})
			mockStream.EXPECT().Send(gomock.Any()).DoAndReturn(
				func(*pb.ReplicationRequest) error {
					defer close(stop)
					return varlog.ErrInternal
				},
			).AnyTimes()
			mockStream.EXPECT().Recv().DoAndReturn(func() (*pb.ReplicationResponse, error) {
				<-stop
				return nil, io.EOF
			}).AnyTimes()
			mockStream.EXPECT().CloseSend().MinTimes(1)

			err := rc.Run(context.TODO())
			So(err, ShouldBeNil)

			errC := rc.Replicate(context.TODO(), types.LLSN(0), []byte("never"))
			err = <-errC
			So(err, ShouldNotBeNil)

			err = rc.Close()
			So(err, ShouldBeNil)
		})

		Convey("it should not replicate data when occurred receive error", func() {
			mockClient.EXPECT().Replicate(gomock.Any()).DoAndReturn(
				func(context.Context) (pb.ReplicatorService_ReplicateClient, error) {
					return mockStream, nil
				},
			)

			stop := make(chan struct{})
			mockStream.EXPECT().Send(gomock.Any()).DoAndReturn(
				func(*pb.ReplicationRequest) error {
					defer close(stop)
					return nil
				},
			).AnyTimes()
			mockStream.EXPECT().Recv().DoAndReturn(
				func() (*pb.ReplicationResponse, error) {
					<-stop
					return nil, varlog.ErrInternal
				},
			).AnyTimes()
			mockStream.EXPECT().CloseSend().MinTimes(1)

			err := rc.Run(context.TODO())
			So(err, ShouldBeNil)

			errC := rc.Replicate(context.TODO(), types.LLSN(0), []byte("never"))
			err = <-errC
			So(err, ShouldNotBeNil)

			err = rc.Close()
			So(err, ShouldBeNil)
		})

		Convey("it should replicate data", func() {
			mockClient.EXPECT().Replicate(gomock.Any()).DoAndReturn(
				func(context.Context) (pb.ReplicatorService_ReplicateClient, error) {
					return mockStream, nil
				},
			)
			var step uint32 = 0
			mockStream.EXPECT().Send(gomock.Any()).DoAndReturn(
				func(*pb.ReplicationRequest) error {
					defer atomic.AddUint32(&step, 1)
					for atomic.LoadUint32(&step)%2 == 1 {
						time.Sleep(time.Millisecond)
					}
					return nil
				},
			).AnyTimes()
			mockStream.EXPECT().Recv().DoAndReturn(
				func() (*pb.ReplicationResponse, error) {
					if atomic.LoadUint32(&step) >= 2*N {
						return nil, io.EOF
					}

					defer atomic.AddUint32(&step, 1)
					for atomic.LoadUint32(&step)%2 == 0 {
						time.Sleep(time.Millisecond)
					}

					return &pb.ReplicationResponse{
						StorageNodeID: types.StorageNodeID(0),
						LogStreamID:   types.LogStreamID(0),
						LLSN:          types.LLSN(atomic.LoadUint32(&step) / 2),
					}, nil
				},
			).AnyTimes()
			mockStream.EXPECT().CloseSend().MinTimes(1)

			err := rc.Run(context.TODO())
			So(err, ShouldBeNil)

			for i := 0; i < N; i++ {
				errC := rc.Replicate(context.TODO(), types.LLSN(i), []byte("log_001"))
				err = <-errC
				So(err, ShouldBeNil)
			}

			err = rc.Close()
			So(err, ShouldBeNil)
		})

		Reset(func() {
			rc.Close()
		})

	})
}
