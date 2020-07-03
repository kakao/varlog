package storage

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
	"google.golang.org/grpc"
)

func TestReplicator(t *testing.T) {
	Convey("Given new Replicator", t, func() {
		r := NewReplicator()
		So(r, ShouldNotBeNil)

		Convey("It should be run", func() {
			ctx := context.TODO()
			r.Run(ctx)
			So(r.cancel, ShouldNotBeNil)
			r.Close()
		})

		Convey("Replicate call without replica should run error", func() {
			r.Run(context.TODO())
			So(r.cancel, ShouldNotBeNil)

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
			defer cancel()
			errC := r.Replicate(ctx, types.LLSN(0), []byte("never happen"), nil)
			err := <-errC
			So(err, ShouldNotBeNil)

			r.Close()
		})

		Convey("Replicate call should not process after closing replicator", func() {
			r.Run(context.TODO())
			So(r.cancel, ShouldNotBeNil)
			r.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
			defer cancel()
			errC := r.Replicate(ctx, types.LLSN(0), []byte("never happen"), []Replica{{}})
			err := <-errC
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, context.DeadlineExceeded)
		})
	})
}

func testHelerGetAddress(lis net.Listener) string {
	addr := lis.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	address := fmt.Sprintf("localhost:%d", tcpAddr.Port)
	So(address, ShouldNotEndWith, "localhost:")
	return address
}

func TestReplicatorReplicate(t *testing.T) {
	const (
		nrBackups   = 10
		logStreamID = types.LogStreamID(1)
	)
	Convey("Given ReplicatorServices for backups", t, func(c C) {
		lses := []*dummyLSE{}
		rss := []*ReplicatorService{}
		servers := []*grpc.Server{}
		addrs := []string{}
		for i := 0; i < nrBackups; i++ {
			lse := &dummyLSE{}
			rs := NewReplicatorService(types.StorageNodeID(i+1), logStreamID, lse)
			lses = append(lses, lse)
			rss = append(rss, rs)

			lis, err := net.Listen("tcp", ":0")
			So(err, ShouldBeNil)

			addrs = append(addrs, testHelerGetAddress(lis))

			server := grpc.NewServer()
			rs.Register(server)
			servers = append(servers, server)

			go func(lis net.Listener, server *grpc.Server) {
				err := server.Serve(lis)
				c.So(err, ShouldBeNil)
			}(lis, server)
		}

		defer func() {
			for _, server := range servers {
				server.GracefulStop()
			}
		}()

		replicas := []Replica{}
		for i := 0; i < nrBackups; i++ {
			replicas = append(replicas, Replica{
				StorageNodeID: types.StorageNodeID(i + 1),
				LogStreamID:   logStreamID,
				Address:       addrs[i],
			})
		}

		Convey("Replication should copy data to all backups", func() {
			r := NewReplicator()
			r.Run(context.TODO())

			for i := 0; i < 100; i++ {
				data := fmt.Sprintf("log-%03d", i)
				errC := r.Replicate(context.TODO(), types.LLSN(i), []byte(data), replicas)
				err := <-errC
				So(err, ShouldBeNil)
				for j := 0; j < nrBackups; j++ {
					So(lses[j].latestLLSN.Load(), ShouldEqual, types.LLSN(i))
				}
			}

			r.Close()

		})
	})
}
