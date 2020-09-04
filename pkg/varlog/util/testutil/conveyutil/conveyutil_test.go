package conveyutil

import (
	"context"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/netutil"
	"google.golang.org/grpc"
)

func checkConnection(ctx context.Context, addr string, t *testing.T) {
	t.Log("Dialing")
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
	So(err, ShouldBeNil)

	t.Log("Closing")
	So(conn.Close(), ShouldBeNil)
}

func TestWithServiceServer(t *testing.T) {
	Convey("Given a service", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := storage.NewMockLogStreamExecutorGetter(ctrl)
		s := storage.NewLogIOService(types.StorageNodeID(1), lseGetter)
		lis, err := net.Listen("tcp", ":0")
		So(err, ShouldBeNil)

		server := grpc.NewServer()
		s.Register(server)

		Reset(func() {
			server.GracefulStop()
		})

		Convey("When (*grpc.Server).Serve() is called before stopping", func() {
			Convey("Then it should have no error", func(c C) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() {
					t.Log("Serving")
					err := server.Serve(lis)
					c.So(err, ShouldBeNil)
					cancel()
				}()

				addr, err := netutil.GetListenerLocalAddr(lis)
				So(err, ShouldBeNil)
				checkConnection(ctx, addr, t)
			})
		})

		Convey("When (*grpc.Server).Serve() is called after stopping", func() {
			Convey("Then it should have an error", func() {
				server.GracefulStop()
				err := server.Serve(lis)
				So(err, ShouldNotBeNil)
			})
		})
	})
}
