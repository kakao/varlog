package storage

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"go.uber.org/zap"
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

		r := NewReplicator(zap.NewNop())

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
