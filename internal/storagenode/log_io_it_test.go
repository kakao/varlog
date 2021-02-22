package storagenode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil/conveyutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

func TestLogIOClientLogIOServiceAppend(t *testing.T) {
	Convey("Given that a LogIOService is running", t, func() {
		const lsid = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		service := NewLogIOService(types.StorageNodeID(0), lseGetter, newNopTelmetryStub(), nil)
		lse := NewMockLogStreamExecutor(ctrl)

		lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
			func(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
				if logStreamID == lsid {
					return lse, true
				}
				return nil, false
			},
		).AnyTimes()

		Convey("And a LogIOClient tries to append a log entry to a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			cli, err := logc.NewLogIOClient(context.TODO(), addr)
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

			Convey("When the LogIOClient is timed out", func() {
				stop := make(chan struct{})
				defer close(stop)
				lse.EXPECT().Append(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, []byte) (types.GLSN, error) {
						<-stop
						return types.InvalidGLSN, verrors.ErrInternal
					},
				).MaxTimes(1)

				Convey("Then the LogIOClient should return timeout error", func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
					defer cancel()
					_, err := cli.Append(ctx, lsid, nil)
					So(errors.Is(err, context.DeadlineExceeded), ShouldBeTrue)
				})
			})

			Convey("When the underlying LogStreamExecutor returns an error", func() {
				lse.EXPECT().Append(gomock.Any(), gomock.Any()).Return(types.GLSN(0), verrors.ErrInternal)
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

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		service := NewLogIOService(types.StorageNodeID(0), lseGetter, newNopTelmetryStub(), nil)
		lse := NewMockLogStreamExecutor(ctrl)

		lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
			func(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
				if logStreamID == lsid {
					return lse, true
				}
				return nil, false
			},
		).AnyTimes()

		Convey("And a LogIOClient tries to read a log entry from a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			cli, err := logc.NewLogIOClient(context.TODO(), addr)
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

			Convey("When the LogIOClient is timed out", func() {
				stop := make(chan struct{})
				defer close(stop)
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, types.GLSN) ([]byte, error) {
						<-stop
						return nil, verrors.ErrInternal
					},
				).MaxTimes(1)

				Convey("Then the LogIOClient should return timeout error", func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
					defer cancel()
					_, err := cli.Read(ctx, lsid, types.MinGLSN)
					So(errors.Is(err, context.DeadlineExceeded), ShouldBeTrue)
				})
			})

			Convey("When the underlying LogStreamExecutor returns ErrTrimmed", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(types.InvalidLogEntry, verrors.ErrTrimmed)

				Convey("Then the LogIOClient should return ErrTrimmed error", func() {
					_, err := cli.Read(context.TODO(), lsid, types.GLSN(0))
					// TODO: check errors! Does client need to know the sentry
					// error?
					// So(err, ShouldResemble, verrors.ErrTrimmed)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the underlying LogStreamExecutor returns ErrUndeciadable", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(types.InvalidLogEntry, verrors.ErrUndecidable)

				Convey("Then the LogIOClient should return ErrUndecidable error", func() {
					_, err := cli.Read(context.TODO(), lsid, types.GLSN(0))
					// TODO: check errors! Does client need to know the sentry
					// error?
					// So(err, ShouldResemble, verrors.ErrUndecidable)
					So(err, ShouldNotBeNil)
				})
			})

			Convey("when the underlying LogStreamExecutor reads the log entry", func() {
				lse.EXPECT().Read(gomock.Any(), gomock.Any()).Return(types.LogEntry{Data: []byte("foo")}, nil)

				Convey("Then the LogIOClient should return the log entry", func() {
					ent, err := cli.Read(context.TODO(), lsid, types.GLSN(0))
					So(err, ShouldBeNil)
					So(ent.Data, ShouldResemble, []byte("foo"))
				})
			})
		}))
	})
}

func TestLogIOClientLogIOServiceSubscribe(t *testing.T) {
	Convey("Given that a LogIOService is running", t, func() {
		const lsid = types.LogStreamID(1)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		service := NewLogIOService(types.StorageNodeID(0), lseGetter, newNopTelmetryStub(), nil)
		lse := NewMockLogStreamExecutor(ctrl)

		lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
			func(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
				if logStreamID == lsid {
					return lse, true
				}
				return nil, false
			},
		).AnyTimes()

		Convey("And a LogIOClient tries to subscribe to log entries of a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {
			cli, err := logc.NewLogIOClient(context.TODO(), addr)
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

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		service := NewLogIOService(types.StorageNodeID(0), lseGetter, newNopTelmetryStub(), nil)
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()

		lseGetter.EXPECT().GetLogStreamExecutor(gomock.Any()).DoAndReturn(
			func(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
				if logStreamID == lse1.LogStreamID() {
					return lse1, true
				}
				if logStreamID == lse2.LogStreamID() {
					return lse2, true
				}
				return nil, false
			},
		).AnyTimes()
		lseGetter.EXPECT().GetLogStreamExecutors().Return([]LogStreamExecutor{lse1, lse2}).AnyTimes()

		Convey("And a LogIOClient tries to trim log entries of a LogStream in the LogIOService", conveyutil.WithServiceServer(service, func(server *grpc.Server, addr string) {

			cli, err := logc.NewLogIOClient(context.TODO(), addr)
			So(err, ShouldBeNil)

			Reset(func() {
				So(cli.Close(), ShouldBeNil)
			})

			Convey("When the LogIOClient is timed out", func() {
				stop := make(chan struct{})
				defer close(stop)
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, types.GLSN) error {
						<-stop
						return verrors.ErrInternal
					},
				).MaxTimes(1)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, types.GLSN) error {
						<-stop
						return verrors.ErrInternal
					},
				).MaxTimes(1)

				Convey("Then the LogIOClient should return timeout error", func() {
					ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
					defer cancel()
					err := cli.Trim(ctx, types.MinGLSN)
					So(errors.Is(err, context.DeadlineExceeded), ShouldBeTrue)
				})
			})

			Convey("When some of the underlying LogStreamExecutor return errors", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(nil)
				Convey("Then the LogIOClient should return an error", func() {
					err := cli.Trim(context.TODO(), types.GLSN(20))
					So(err, ShouldNotBeNil)
				})
			})

			Convey("When the request is in asynchronous mode", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(nil)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(nil)
				Convey("Then the number of trimmed log entries should be zero", func() {
					err := cli.Trim(context.TODO(), types.GLSN(20))
					So(err, ShouldBeNil)
				})
			})

			Convey("When the request is in synchronous mode", func() {
				lse1.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(nil)
				lse2.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(nil)
				Convey("Then the number of trimmed log entries should be equal to the value which is trimmed by the underlying LogStreamExecutor", func() {
					err := cli.Trim(context.TODO(), types.GLSN(20))
					So(err, ShouldBeNil)
				})
			})
		}))
	})
}
