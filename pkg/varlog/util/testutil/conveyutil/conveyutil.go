package conveyutil

import (
	"net"

	"github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
	"google.golang.org/grpc"
)

type service interface {
	Register(s *grpc.Server)
}

func WithServiceServer(s service, testf func(serverAddr string)) func(c convey.C) {
	return func(c convey.C) {
		lis, err := net.Listen("tcp", ":0")
		convey.So(err, convey.ShouldBeNil)
		server := grpc.NewServer()
		s.Register(server)
		go func() {
			err := server.Serve(lis)
			c.So(err, convey.ShouldBeNil)
		}()
		defer server.GracefulStop()

		testf(testutil.GetLocalAddress(lis))
	}
}
