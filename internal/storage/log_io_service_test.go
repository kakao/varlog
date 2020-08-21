package storage

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"github.daumkakao.com/varlog/varlog/proto/storage_node/mock"
)

func setLseGetter(lseGetterMock *MockLogStreamExecutorGetter, lses ...LogStreamExecutor) {
	lseGetterMock.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
		func(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
			for _, lse := range lses {
				if lse.LogStreamID() == logStreamID {
					return lse, true
				}
			}
			return nil, false
		},
	).AnyTimes()
	lseGetterMock.EXPECT().GetLogStreamExecutors().Return(lses).AnyTimes()
}

func TestStorageNodeServiceAppend(t *testing.T) {
	Convey("Append", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const logStreamID = types.LogStreamID(1)
		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		s := NewLogIOService(types.StorageNodeID(1), lseGetter)

		Convey("it should return error if the LogStream does not exist", func() {
			setLseGetter(lseGetter)
			_, err := s.Append(context.TODO(), &pb.AppendRequest{
				LogStreamID: logStreamID,
				Payload:     []byte("never"),
			})
			So(err, ShouldNotBeNil)

		})

		Convey("it should not write a log entry if the LogStreamExecutor is failed", func() {
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()
			setLseGetter(lseGetter, lse)
			lse.EXPECT().Append(gomock.Any(), gomock.Any()).Return(types.GLSN(0), varlog.ErrInternal)
			_, err := s.Append(context.TODO(), &pb.AppendRequest{
				LogStreamID: logStreamID,
				Payload:     []byte("never"),
			})
			So(err, ShouldNotBeNil)
		})

		Convey("it should write a log entry", func() {
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()
			setLseGetter(lseGetter, lse)
			lse.EXPECT().Append(gomock.Any(), gomock.Any()).Return(types.GLSN(10), nil)
			rsp, err := s.Append(context.TODO(), &pb.AppendRequest{
				LogStreamID: logStreamID,
				Payload:     []byte("log"),
			})
			So(err, ShouldBeNil)
			So(rsp.GetGLSN(), ShouldEqual, types.GLSN(10))
		})

	})
}

func TestStorageNodeServiceRead(t *testing.T) {
	Convey("Read", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const logStreamID = types.LogStreamID(1)
		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		s := NewLogIOService(types.StorageNodeID(1), lseGetter)

		Convey("it should return error if the LogStream does not exist", func() {
			setLseGetter(lseGetter)
			_, err := s.Read(context.TODO(), &pb.ReadRequest{
				LogStreamID: logStreamID,
				GLSN:        types.GLSN(10),
			})
			So(err, ShouldNotBeNil)

		})

		Convey("it should not read a log entry if the LogStreamExecutor is failed", func() {
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()
			setLseGetter(lseGetter, lse)
			lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil, varlog.ErrInternal)
			_, err := s.Read(context.TODO(), &pb.ReadRequest{
				LogStreamID: logStreamID,
				GLSN:        types.GLSN(10),
			})
			So(err, ShouldNotBeNil)
		})

		Convey("it should read a log entry", func() {
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(logStreamID).AnyTimes()
			setLseGetter(lseGetter, lse)
			lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return([]byte("log"), nil)
			rsp, err := s.Read(context.TODO(), &pb.ReadRequest{
				LogStreamID: logStreamID,
				GLSN:        types.GLSN(10),
			})
			So(err, ShouldBeNil)
			So(rsp.GetPayload(), ShouldResemble, []byte("log"))
		})
	})
}

func TestStorageNodeServiceSubscribe(t *testing.T) {
	Convey("Given LogIOService.Subscribe", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		s := NewLogIOService(types.StorageNodeID(1), lseGetter)

		Convey("When requested LogStreamID is not in the StorageNode", func() {
			setLseGetter(lseGetter)
			Convey("Then LogIOService.Subscribe should return an error", func() {
				err := s.Subscribe(&pb.SubscribeRequest{}, mock.NewMockLogIO_SubscribeServer(ctrl))
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestStorageNodeServiceTrim(t *testing.T) {
	Convey("Trim", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		const nrLSEs = 10

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		s := NewLogIOService(types.StorageNodeID(1), lseGetter)

		Convey("it should return the number of log entries removed", func() {
			var lses []LogStreamExecutor
			for i := 0; i < nrLSEs; i++ {
				lse := NewMockLogStreamExecutor(ctrl)
				lse.EXPECT().LogStreamID().Return(types.LogStreamID(i)).AnyTimes()
				lses = append(lses, lse)
				lse.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(10), nil)
			}
			setLseGetter(lseGetter, lses...)
			rsp, err := s.Trim(context.TODO(), &pb.TrimRequest{
				GLSN: types.GLSN(10000),
			})
			So(err, ShouldBeNil)
			So(rsp.GetNumTrimmed(), ShouldEqual, 10*nrLSEs)
		})

	})
}
