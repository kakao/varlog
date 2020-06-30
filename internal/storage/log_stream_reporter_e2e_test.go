package storage

import (
	"context"
	"fmt"
	"net"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	"google.golang.org/grpc"
)

func TestLogStreamReporterConnect(t *testing.T) {
	Convey("LogStreamReporter's service should listen to new connections from clients", t, func(c C) {
		const storageNodeID = types.StorageNodeID(0)

		lis, err := net.Listen("tcp", ":0")
		So(err, ShouldBeNil)

		addr := lis.Addr()
		tcpAddr := addr.(*net.TCPAddr)
		address := fmt.Sprintf("localhost:%d", tcpAddr.Port)
		So(address, ShouldNotEndWith, "localhost:")

		server := grpc.NewServer()
		service := NewLogStreamReporterService(NewLogStreamReporter(storageNodeID))
		service.Register(server)
		go func() {
			err := server.Serve(lis)
			c.So(err, ShouldBeNil)
		}()
		defer server.GracefulStop()

		Convey("LogStreamReporter's client should connect to its service", func() {
			cli, err := NewLogStreamReporterClient(address)
			So(err, ShouldBeNil)

			Convey("LogStreamReporterClient should receive reports from service by using GetReport", func() {
				lls, err := cli.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(lls, ShouldNotBeNil)
				So(lls.GetStorageNodeID(), ShouldEqual, storageNodeID)
			})

			Convey("LogStreamReporterClient should send commit to service by using Commit", func() {
				err := cli.Commit(context.TODO(), &pb.GlobalLogStreamDescriptor{})
				So(err, ShouldBeNil)
			})

			Reset(func() {
				So(cli.Close(), ShouldBeNil)
			})
		})
	})
}
