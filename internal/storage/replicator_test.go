package storage

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"google.golang.org/grpc"
)

func TestReplicator(t *testing.T) {
	Convey("Replicator", t, func() {
		const numRCs = 2
		const N = 1000

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rcs := make([]*MockReplicatorClient, numRCs)
		for i := 0; i < numRCs; i++ {
			rcs[i] = NewMockReplicatorClient(ctrl)
		}

		r := NewReplicator()

		Convey("it should be run and closed", func() {
			r.Run(context.TODO())
			for i := 0; i < numRCs; i++ {
				r.(*replicator).rcm[types.StorageNodeID(i)] = rcs[i]
				rcs[i].EXPECT().Close().AnyTimes()
			}
		})

		Convey("replicate operation with nil replica should run error", func() {
			r.Run(context.TODO())
			errC := r.Replicate(context.TODO(), types.LLSN(0), []byte("never"), nil)
			err := <-errC
			So(err, ShouldNotBeNil)
		})

		Convey("replicate operation should not work with not running replicator", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
			defer cancel()
			errC := r.Replicate(ctx, types.LLSN(0), []byte("never"), []Replica{
				{},
			})
			err := <-errC
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, context.DeadlineExceeded)
		})

		Convey("replicate operation should not work with closing replicator", func() {
			r.Run(context.TODO())
			r.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
			defer cancel()
			errC := r.Replicate(ctx, types.LLSN(0), []byte("never"), []Replica{
				{},
			})
			err := <-errC
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, context.DeadlineExceeded)
		})

		Convey("replicate operation should work well", func() {
			for i := 0; i < numRCs; i++ {

				rcs[i].EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(context.Context, types.LLSN, []byte) <-chan error {
						c := make(chan error, 1)
						c <- nil
						return c
					}).AnyTimes()
				rcs[i].EXPECT().Close().AnyTimes()
				r.(*replicator).rcm[types.StorageNodeID(i)] = rcs[i]
			}
			r.Run(context.TODO())
			for j := 0; j < N; j++ {
				replicas := make([]Replica, numRCs)
				for i := 0; i < numRCs; i++ {
					replicas[i] = Replica{
						StorageNodeID: types.StorageNodeID(i),
						LogStreamID:   types.LogStreamID(0),
					}
				}
				errC := r.Replicate(context.TODO(), types.LLSN(0), []byte("log"), replicas)
				err := <-errC
				So(err, ShouldBeNil)
			}
		})

		Reset(func() {
			r.Close()
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
