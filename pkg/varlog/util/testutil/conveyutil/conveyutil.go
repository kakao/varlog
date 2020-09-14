package conveyutil

import (
	"context"

	"github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/netutil"
	"google.golang.org/grpc"
)

type service interface {
	Register(s *grpc.Server)
}

func WithServiceServer(s service, testf func(server *grpc.Server, addr string)) func(c convey.C) {
	return func(c convey.C) {
		lis, err := netutil.Listen("tcp", "127.0.0.1:0")
		convey.So(err, convey.ShouldBeNil)
		addrs, err := netutil.GetListenerAddrs(lis.Addr())
		convey.So(err, convey.ShouldBeNil)
		addr := addrs[0]

		server := grpc.NewServer()
		s.Register(server)

		go func() {
			err := server.Serve(lis)
			if err != nil {
				c.Printf("quit grpc server: %v - stopped server before starting", err)
			}
		}()

		// block until the grpc server is ready without calling neither any RPCs nor tests
		// addr := testutil.GetLocalAddress(lis)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
		convey.So(err, convey.ShouldBeNil)
		convey.So(conn.Close(), convey.ShouldBeNil)

		convey.Reset(func() {
			server.GracefulStop()
		})

		testf(server, addr)
	}
}
