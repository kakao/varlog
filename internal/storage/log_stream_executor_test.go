package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kakao/varlog/pkg/varlog"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
)

func TestLogStreamExecutorNew(t *testing.T) {
	Convey("LogStreamExecutor", t, func() {
		Convey("it should not be created with nil storage", func() {
			_, err := NewLogStreamExecutor(types.LogStreamID(0), nil)
			So(err, ShouldNotBeNil)
		})

		Convey("it should not be sealed at first", func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storage := NewMockStorage(ctrl)
			lse, err := NewLogStreamExecutor(types.LogStreamID(0), storage)
			So(err, ShouldBeNil)
			So(lse.(*logStreamExecutor).isSealed(), ShouldBeFalse)
		})
	})
}

func TestLogStreamExecutorRunClose(t *testing.T) {
	Convey("LogStreamExecutor", t, func() {
		Convey("it should be run and closed", func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storage := NewMockStorage(ctrl)
			lse, err := NewLogStreamExecutor(types.LogStreamID(0), storage)
			So(err, ShouldBeNil)
			lse.Run(context.TODO())
			lse.Close()
		})
	})
}

func TestLogStreamExecutorOperations(t *testing.T) {
	Convey("LogStreamExecutor", t, func() {
		const logStreamID = types.LogStreamID(0)
		const N = 1000

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := NewMockStorage(ctrl)
		replicator := NewMockReplicator(ctrl)

		lse, err := NewLogStreamExecutor(types.LogStreamID(0), storage)
		So(err, ShouldBeNil)
		lse.Run(context.TODO())

		Convey("read operation should reply uncertainness if it doesn't know", func() {
			_, err := lse.Read(context.TODO(), types.GLSN(0))
			So(err, ShouldEqual, varlog.ErrUndecidable)
		})

		Convey("read operation should reply error when the requested GLSN was already deleted", func() {
			const trimGLSN = types.GLSN(5)
			storage.EXPECT().Delete(gomock.Any()).Return(uint64(trimGLSN)+1, nil)
			nr, err := lse.Trim(context.TODO(), trimGLSN, false)
			So(err, ShouldBeNil)
			So(nr, ShouldEqual, uint64(trimGLSN)+1)
			for trimmedGLSN := types.GLSN(0); trimmedGLSN <= trimGLSN; trimmedGLSN++ {
				isTrimmed, nonTrimmedGLSNBegin := lse.(*logStreamExecutor).isTrimmed(trimmedGLSN)
				So(isTrimmed, ShouldBeTrue)
				So(nonTrimmedGLSNBegin, ShouldEqual, trimGLSN+1)

				_, err := lse.Read(context.TODO(), trimmedGLSN)
				So(err, ShouldEqual, varlog.ErrTrimmed)
			}
		})

		Convey("read operation should reply written data", func() {
			storage.EXPECT().Read(gomock.Any()).Return([]byte("log"), nil)
			lse.(*logStreamExecutor).learnedGLSNBegin = 0
			lse.(*logStreamExecutor).learnedGLSNEnd = 10
			data, err := lse.Read(context.TODO(), types.GLSN(0))
			So(err, ShouldBeNil)
			So(string(data), ShouldEqual, "log")
		})

		Convey("append operation should not write data when sealed", func() {
			lse.(*logStreamExecutor).seal()
			_, err := lse.Append(context.TODO(), []byte("never"))
			So(err, ShouldEqual, varlog.ErrSealed)
		})

		Convey("append operation should not write data when the storage is failed", func() {
			storage.EXPECT().Write(gomock.Any(), gomock.Any()).Return(varlog.ErrInternal)
			_, err := lse.Append(context.TODO(), []byte("never"))
			So(err, ShouldNotBeNil)
			sealed := lse.(*logStreamExecutor).isSealed()
			So(sealed, ShouldBeTrue)
		})

		Convey("append operation should not write data when the replication is failed", func() {
			storage.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil)
			c := make(chan error, 1)
			c <- varlog.ErrInternal
			replicator.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(c)
			replicator.EXPECT().Close().AnyTimes()
			lse.(*logStreamExecutor).replicator = replicator
			_, err := lse.Append(context.TODO(), []byte("never"), Replica{})
			So(err, ShouldNotBeNil)
		})

		Convey("append operation should write data", func() {
			waitCommitDone := func(knownNextGLSN types.GLSN) {
				for {
					lse.(*logStreamExecutor).mu.RLock()
					updatedKnownNextGLSN := lse.(*logStreamExecutor).knownNextGLSN
					lse.(*logStreamExecutor).mu.RUnlock()
					if knownNextGLSN != updatedKnownNextGLSN {
						break
					}
					time.Sleep(time.Millisecond)
				}
			}
			waitWriteDone := func(uncommittedLLSNEnd types.LLSN) {
				for uncommittedLLSNEnd == lse.(*logStreamExecutor).uncommittedLLSNEnd.Load() {
					time.Sleep(time.Millisecond)
				}
			}

			storage.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			storage.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			for i := types.GLSN(0); i < N; i++ {
				lse.(*logStreamExecutor).mu.RLock()
				knownNextGLSN := lse.(*logStreamExecutor).knownNextGLSN
				lse.(*logStreamExecutor).mu.RUnlock()
				uncommittedLLSNEnd := lse.(*logStreamExecutor).uncommittedLLSNEnd.Load()
				var wg sync.WaitGroup
				wg.Add(1)
				go func(uncommittedLLSNEnd types.LLSN, knownNextGLSN types.GLSN) {
					defer wg.Done()
					waitWriteDone(uncommittedLLSNEnd)
					t.Log("LSR committing")
					lse.Commit(CommittedLogStreamStatus{
						LogStreamID:        logStreamID,
						NextGLSN:           i + 1,
						PrevNextGLSN:       i,
						CommittedGLSNBegin: i,
						CommittedGLSNEnd:   i + 1,
					})
					waitCommitDone(knownNextGLSN)
				}(uncommittedLLSNEnd, knownNextGLSN)
				glsn, err := lse.Append(context.TODO(), []byte("log"))
				So(err, ShouldBeNil)
				So(glsn, ShouldEqual, i)
				wg.Wait()
				t.Logf("wrote - %d", i)
			}
		})

		Reset(func() {
			lse.Close()
		})
	})
}
