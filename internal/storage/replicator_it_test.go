package storage

import (
	"context"
	"fmt"
	"net"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
	"google.golang.org/grpc"
)

type dummyLSE struct {
	latestLLSN types.AtomicLLSN
}

func (e *dummyLSE) LogStreamID() types.LogStreamID { return types.LogStreamID(1) }
func (e *dummyLSE) Run(ctx context.Context)        {}
func (e *dummyLSE) Close()                         {}
func (e *dummyLSE) Read(ctx context.Context, glsn types.GLSN) ([]byte, error) {
	return nil, nil
}
func (e *dummyLSE) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
	return nil, nil
}
func (e *dummyLSE) Replicate(ctx context.Context, llsn types.LLSN, data []byte) error {
	e.latestLLSN.Store(llsn)
	return nil
}
func (e *dummyLSE) Append(ctx context.Context, data []byte, replicas ...Replica) (types.GLSN, error) {
	return 0, nil
}
func (e *dummyLSE) Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error) {
	return 0, nil
}
func (e *dummyLSE) GetReport() UncommittedLogStreamStatus {
	panic("not implemented")
}
func (e *dummyLSE) Commit(CommittedLogStreamStatus) error {
	panic("not implemented")
}

func TestReplicatorRPCConnection(t *testing.T) {
	Convey("Given ReplicatorService", t, func(c C) {
		const (
			storageNodeID = types.StorageNodeID(1)
			logStreamID   = types.LogStreamID(1)
		)

		lis, err := net.Listen("tcp", ":0")
		So(err, ShouldBeNil)

		addr := lis.Addr()
		tcpAddr := addr.(*net.TCPAddr)
		address := fmt.Sprintf("localhost:%d", tcpAddr.Port)
		So(address, ShouldNotEndWith, "localhost:")

		lse := &dummyLSE{}
		server := grpc.NewServer()
		service := NewReplicatorService(storageNodeID, logStreamID, lse)
		service.Register(server)
		go func() {
			err := server.Serve(lis)
			c.So(err, ShouldBeNil)
		}()
		defer server.GracefulStop()

		Convey("ReplicatorClient should be created", func() {
			rc, err := NewReplicatorClient(address)
			So(err, ShouldBeNil)

			Convey("ReplicatorClient should be closed", func() {
				err := rc.Close()
				So(err, ShouldBeNil)
			})

			Convey("ReplicatorClient should be run", func() {
				err := rc.Run(context.TODO())
				So(err, ShouldBeNil)
				err = rc.Close()
				So(err, ShouldBeNil)
			})

			Convey("ReplicatorClient should not be affected unexpected duplicated run", func() {
				err := rc.Run(context.TODO())
				So(err, ShouldBeNil)
				p1 := &rc.(*replicatorClient).stream
				err = rc.Run(context.TODO())
				So(err, ShouldBeNil)
				p2 := &rc.(*replicatorClient).stream
				So(p1, ShouldEqual, p2)
				err = rc.Close()
				So(err, ShouldBeNil)
			})

			Convey("ReplicatorClient should send data and ReplicatorService should receive it", func() {
				err := rc.Run(context.TODO())
				So(err, ShouldBeNil)

				for i := 0; i < 100; i++ {
					errC := rc.Replicate(context.TODO(), types.LLSN(i), []byte("test"))
					err = <-errC
					So(err, ShouldBeNil)
					So(lse.latestLLSN.Load(), ShouldEqual, types.LLSN(i))
				}

				err = rc.Close()
				So(err, ShouldBeNil)
			})

		})
	})
}
