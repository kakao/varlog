package storage

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil/conveyutil"
	"google.golang.org/grpc"
)

func TestLogIOClientLogIOServiceAppend(t *testing.T) {
	Convey("Given that a LogIOService is running", t, func() {
		const lsid = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		service := NewLogIOService(types.StorageNodeID(0))
		lse := NewMockLogStreamExecutor(ctrl)
		service.m.Lock()
		service.lseM[lsid] = lse
		service.m.Unlock()

		Convey("And a LogIOClient tries to append a log entry to a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			cli, err := varlog.NewLogIOClient(addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(cli.Close(), ShouldBeNil)
			})

			Convey("When the LogStream is not registered", func() {
				Convey("Then the LogIOClient should return an error", func() {
					_, err = cli.Append(context.TODO(), types.LogStreamID(2), nil)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the underlying LogStreamExecutor is timed out", func() {
				lse.EXPECT().Append(gomock.Any(), gomock.Any()).Return(types.InvalidGLSN, context.DeadlineExceeded)
				Convey("Then the LogIOClient should return timeout error", func() {
					_, err := cli.Append(context.TODO(), lsid, nil)
					// TODO: below code is not ok
					// So(varlog.ToErr(ctx, err), ShouldResemble, context.DeadlineExceeded)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the LogIOClient is timed out", func() {
				stop := make(chan struct{})
				defer close(stop)
				lse.EXPECT().Append(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, []byte) (types.GLSN, error) {
						<-stop
						return types.InvalidGLSN, varlog.ErrInternal
					},
				).MaxTimes(1)

				Convey("Then the LogIOClient should return timeout error", func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
					defer cancel()
					_, err := cli.Append(ctx, lsid, nil)
					So(varlog.ToErr(ctx, err), ShouldResemble, context.DeadlineExceeded)
				})
			})

			Convey("When the underlying LogStreamExecutor returns an error", func() {
				lse.EXPECT().Append(gomock.Any(), gomock.Any()).Return(types.GLSN(0), varlog.ErrInternal)
				Convey("Then the LogIOClient should return an error", func() {
					_, err := cli.Append(context.TODO(), lsid, []byte("foo"))
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the underlying LogStreamExecutor appends the log entry", func() {
				const eglsn = types.GLSN(1)
				lse.EXPECT().Append(gomock.Any(), gomock.Any()).Return(eglsn, nil)

				Convey("Then the LogIOClient should return the GLSN for the log entry", func() {
					aglsn, err := cli.Append(context.TODO(), lsid, []byte("foo"))
					So(err, ShouldBeNil)
					So(aglsn, ShouldEqual, eglsn)
				})
			})
		}))
	})
}

func TestLogIOClientLogIOServiceRead(t *testing.T) {
	Convey("Given that a LogIOService is running", t, func() {
		const lsid = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		service := NewLogIOService(types.StorageNodeID(0))
		lse := NewMockLogStreamExecutor(ctrl)
		service.m.Lock()
		service.lseM[lsid] = lse
		service.m.Unlock()

		Convey("And a LogIOClient tries to read a log entry from a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			cli, err := varlog.NewLogIOClient(addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(cli.Close(), ShouldBeNil)
			})

			Convey("When the LogStream is not registered", func() {
				Convey("Then the LogIOClient should return an error", func() {
					_, err = cli.Read(context.TODO(), types.LogStreamID(2), types.GLSN(0))
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the underlying LogStreamExecutor is timed out", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil, context.DeadlineExceeded)
				Convey("Then the LogIOClient should return timeout error", func() {
					_, err := cli.Read(context.TODO(), lsid, types.MinGLSN)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the LogIOClient is timed out", func() {
				stop := make(chan struct{})
				defer close(stop)
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, types.GLSN) ([]byte, error) {
						<-stop
						return nil, varlog.ErrInternal
					},
				).MaxTimes(1)

				Convey("Then the LogIOClient should return timeout error", func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
					defer cancel()
					_, err := cli.Read(ctx, lsid, types.MinGLSN)
					So(varlog.ToErr(ctx, err), ShouldResemble, context.DeadlineExceeded)
				})
			})

			Convey("When the underlying LogStreamExecutor returns ErrTrimmed", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil, varlog.ErrTrimmed)

				Convey("Then the LogIOClient should return ErrTrimmed error", func() {
					_, err := cli.Read(context.TODO(), lsid, types.GLSN(0))
					So(err, ShouldResemble, varlog.ErrTrimmed)
				})
			})

			Convey("When the underlying LogStreamExecutor returns ErrUndeciadable", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil, varlog.ErrUndecidable)

				Convey("Then the LogIOClient should return ErrUndecidable error", func() {
					_, err := cli.Read(context.TODO(), lsid, types.GLSN(0))
					So(err, ShouldResemble, varlog.ErrUndecidable)
				})
			})

			Convey("when the underlying LogStreamExecutor reads the log entry", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return([]byte("foo"), nil)

				Convey("Then the LogIOClient should return the log entry", func() {
					ent, err := cli.Read(context.TODO(), lsid, types.GLSN(0))
					So(err, ShouldBeNil)
					So(ent.Data, ShouldResemble, []byte("foo"))
				})
			})
		}))
	})
}

func TestLogIOClientLogIOServiceSubscirbe(t *testing.T) {
	Convey("Given that a LogIOService is running", t, func() {
		const lsid = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		service := NewLogIOService(types.StorageNodeID(0))
		lse := NewMockLogStreamExecutor(ctrl)
		service.m.Lock()
		service.lseM[lsid] = lse
		service.m.Unlock()

		Convey("And a LogIOClient tries to subscribe to log entries of a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			cli, err := varlog.NewLogIOClient(addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(cli.Close(), ShouldBeNil)
			})

			Convey("When the LogStream is not registered", func() {
				Convey("Then the LogIOClient should return an error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("When the underlying LogStreamExecutor is timed out", func() {
				Convey("Then the LogIOClient should return timeout error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("When the LogIOClient is timed out", func() {
				Convey("Then the LogIOClient should return timeout error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("When the underlying LogStreamExecutor scans log entries out of order", func() {
				Convey("Then the channel returned from the LogIOClient has an ErrUnordered error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("When the underlying LogStreamExecutor scans log entries", func() {
				Convey("Then the channel returned from the LogIOClient has the log entries", func() {
					Convey("This isn't yet implemented", nil)
				})
			})
		}))
	})
}

func TestLogIOClientLogIOServiceTrim(t *testing.T) {
	Convey("Given that a LogIOService is running", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		service := NewLogIOService(types.StorageNodeID(0))
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()
		service.m.Lock()
		service.lseM[lse1.LogStreamID()] = lse1
		service.lseM[lse2.LogStreamID()] = lse2
		service.m.Unlock()

		Convey("And a LogIOClient tries to trim log entries of a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {

			cli, err := varlog.NewLogIOClient(addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(cli.Close(), ShouldBeNil)
			})

			Convey("When the underlying LogStreamExecutor is timed out", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), context.DeadlineExceeded)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), context.DeadlineExceeded)
				Convey("Then the LogIOClient should return timeout error", func() {
					_, err := cli.Trim(context.TODO(), types.MinGLSN, false)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the LogIOClient is timed out", func() {
				stop := make(chan struct{})
				defer close(stop)
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, types.GLSN, bool) (uint64, error) {
						<-stop
						return uint64(0), varlog.ErrInternal
					},
				).MaxTimes(1)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, types.GLSN, bool) (uint64, error) {
						<-stop
						return uint64(0), varlog.ErrInternal
					},
				).MaxTimes(1)

				Convey("Then the LogIOClient should return timeout error", func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
					defer cancel()
					_, err := cli.Trim(ctx, types.MinGLSN, false)
					So(varlog.ToErr(ctx, err), ShouldResemble, context.DeadlineExceeded)
				})
			})

			Convey("When some of the underlying LogStreamExecutor return errors", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), varlog.ErrInternal)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(10), nil)
				Convey("Then the LogIOClient should return an error", func() {
					_, err := cli.Trim(context.TODO(), types.GLSN(20), false)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the request is in asynchronous mode", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), nil)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), nil)
				Convey("Then the number of trimmed log entries should be zero", func() {
					num, err := cli.Trim(context.TODO(), types.GLSN(20), true)
					So(err, ShouldBeNil)
					So(num, ShouldBeZeroValue)
				})
			})

			Convey("When the request is in synchronous mode", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(5), nil)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(5), nil)
				Convey("Then the number of trimmed log entries should be equal to the value which is trimmed by the underlying LogStreamExecutor", func() {
					num, err := cli.Trim(context.TODO(), types.GLSN(20), false)
					So(err, ShouldBeNil)
					So(num, ShouldEqual, uint64(10))
				})
			})
		}))
	})
}
