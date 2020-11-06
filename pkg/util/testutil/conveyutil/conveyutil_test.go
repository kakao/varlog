package conveyutil

import (
	"context"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/netutil"
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

		lseGetter := storagenode.NewMockLogStreamExecutorGetter(ctrl)
		s := storagenode.NewLogIOService(types.StorageNodeID(1), lseGetter, nil)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
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

				addrs, err := netutil.GetListenerAddrs(lis.Addr())
				So(err, ShouldBeNil)
				addr := addrs[0]
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
