package conveyutil

import (
	"context"
	"net"

	"github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
	"google.golang.org/grpc"
)

type service interface {
	Register(s *grpc.Server)
}

func WithServiceServer(s service, testf func(server *grpc.Server, addr string)) func(c convey.C) {
	return func(c convey.C) {
		lis, err := net.Listen("tcp", ":0")
		convey.So(err, convey.ShouldBeNil)
		server := grpc.NewServer()
		s.Register(server)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			err := server.Serve(lis)
			if err != nil {
				c.Printf("quit grpc server: %v - stopped server before starting", err)
			}
			cancel()
		}()

		addr := testutil.GetLocalAddress(lis)
		// block until the grpc server is ready without calling neither any RPCs nor tests
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
		convey.So(err, convey.ShouldBeNil)
		convey.So(conn.Close(), convey.ShouldBeNil)

		convey.Reset(func() {
			cancel()
			server.GracefulStop()
		})

		testf(server, addr)
	}
}
