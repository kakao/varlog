package storage

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
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
					updatedKnownNextGLSN := lse.(*logStreamExecutor).knownHighWatermark
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
			for i := types.MinGLSN; i < N; i++ {
				lse.(*logStreamExecutor).mu.RLock()
				knownNextGLSN := lse.(*logStreamExecutor).knownHighWatermark
				lse.(*logStreamExecutor).mu.RUnlock()
				uncommittedLLSNEnd := lse.(*logStreamExecutor).uncommittedLLSNEnd.Load()
				var wg sync.WaitGroup
				wg.Add(1)
				go func(uncommittedLLSNEnd types.LLSN, knownNextGLSN types.GLSN) {
					defer wg.Done()
					waitWriteDone(uncommittedLLSNEnd)
					lse.Commit(CommittedLogStreamStatus{
						LogStreamID:         logStreamID,
						HighWatermark:       i + 1,
						PrevHighWatermark:   i,
						CommittedGLSNOffset: i,
						CommittedGLSNLength: 1,
					})
					waitCommitDone(knownNextGLSN)
				}(uncommittedLLSNEnd, knownNextGLSN)
				glsn, err := lse.Append(context.TODO(), []byte("log"))
				So(err, ShouldBeNil)
				So(glsn, ShouldEqual, i)
				wg.Wait()
			}
		})

		Reset(func() {
			lse.Close()
		})
	})
}

func TestLogStreamExecutorAppend(t *testing.T) {
	Convey("Given that a LogStreamExecutor.Append is called", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storage := NewMockStorage(ctrl)
		replicator := NewMockReplicator(ctrl)
		lse, err := NewLogStreamExecutor(types.LogStreamID(1), storage)
		So(err, ShouldBeNil)

		lse.(*logStreamExecutor).replicator = replicator
		replicator.EXPECT().Run(gomock.Any()).AnyTimes()
		replicator.EXPECT().Close().AnyTimes()

		lse.Run(context.TODO())

		Reset(func() {
			lse.Close()
		})

		Convey("When the context passed to the Append is cancelled", func() {
			storage.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).MaxTimes(1)

			rC := make(chan error, 1)
			rC <- nil
			replicator.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(rC).MaxTimes(1)

			// FIXME: This is a very ugly test because it is not deterministic.
			Convey("Then the LogStreamExecutor should return cancellation error", func(c C) {
				ctx, cancel := context.WithCancel(context.TODO())
				stop := make(chan struct{})
				go func() {
					_, err := lse.Append(ctx, nil, Replica{})
					c.So(err, ShouldResemble, context.Canceled)
					close(stop)
				}()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
				cancel()
				<-stop
			})
		})

		Convey("When the appendC in the LogStreamExecutor is blocked", func() {
			lse.Close()

			Convey("And the Append is blocked more than configured", func() {
				Convey("Then the LogStreamExecutor should return timeout error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("And the context passed to the Append is cancelled", func() {
				ctx, cancel := context.WithCancel(context.TODO())
				cancel()
				Convey("Then the LogStreamExecutor should return cancellation error", func() {
					_, err := lse.Append(ctx, nil)
					So(err, ShouldResemble, context.Canceled)
				})
			})
		})

		Convey("When the Storage.Write operation is blocked", func() {
			stop := make(chan struct{})
			block := func(f func()) {
				storage.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
					func(types.LLSN, []byte) error {
						f()
						<-stop
						return nil
					},
				)
			}

			Reset(func() {
				close(stop)
			})

			Convey("And the Append is blocked more than configured", func() {
				Convey("Then the LogStreamExecutor should return timeout error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("And the context passed to the Append is cancelled", func() {
				ctx, cancel := context.WithCancel(context.TODO())
				block(func() {
					cancel()
				})

				Convey("Then the LogStreamExecutor should return cancellation error", func() {
					_, err := lse.Append(ctx, nil)
					So(err, ShouldResemble, context.Canceled)
				})
			})
		})

		Convey("When the replication is blocked", func() {
			storage.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			stop := make(chan struct{})
			block := func(f func()) {
				replicator.EXPECT().Replicate(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
				).DoAndReturn(
					func(context.Context, types.LLSN, []byte, []Replica) <-chan error {
						f()
						<-stop
						c := make(chan error, 1)
						c <- nil
						return c
					},
				)
			}

			Reset(func() {
				close(stop)
			})

			Convey("And the Append is blocked more than configured", func() {
				Convey("Then the LogStreamExecutor should return timeout error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("And the context passed to the Append is cancelled", func() {
				ctx, cancel := context.WithCancel(context.TODO())
				block(func() {
					cancel()
				})

				Convey("Then the LogStreamExecutor should return cancellation error", func() {
					_, err := lse.Append(ctx, nil, Replica{})
					So(err, ShouldResemble, context.Canceled)
				})
			})
		})

		Convey("When the commit is not notified", func() {
			storage.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			replicator.EXPECT().Replicate(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			).DoAndReturn(
				func(context.Context, types.LLSN, []byte, []Replica) <-chan error {
					defer func() {
						lse.Commit(CommittedLogStreamStatus{
							LogStreamID:         lse.LogStreamID(),
							HighWatermark:       types.MinGLSN,
							PrevHighWatermark:   types.InvalidGLSN,
							CommittedGLSNOffset: types.MinGLSN,
							CommittedGLSNLength: 1,
						})
					}()
					c := make(chan error, 1)
					c <- nil
					return c
				},
			).AnyTimes()

			stop := make(chan struct{})
			block := func(f func()) {
				storage.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(
					func(types.LLSN, types.GLSN) error {
						f()
						<-stop
						return nil
					},
				)
			}

			Reset(func() {
				close(stop)
			})

			Convey("And the Append is blocked more than configured", func() {
				Convey("Then the LogStreamExecutor should return timeout error", func() {
					Convey("This isn't yet implemented", nil)
				})
			})

			Convey("And the context passed to the Append is cancelled", func() {
				ctx, cancel := context.WithCancel(context.TODO())
				block(func() {
					cancel()
				})

				Convey("Then the LogStreamExecutor should return cancellation error", func(c C) {
					wait := make(chan struct{})
					go func() {
						_, err := lse.Append(ctx, nil, Replica{})
						c.So(err, ShouldResemble, context.Canceled)
						close(wait)
					}()
					<-wait
				})
			})
		})
	})
}

func TestLogStreamExecutorRead(t *testing.T) {
	Convey("Given that a LogStreamExecutor.Read is called", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storage := NewMockStorage(ctrl)
		lse, err := NewLogStreamExecutor(types.LogStreamID(1), storage)
		So(err, ShouldBeNil)

		lse.Run(context.TODO())

		Reset(func() {
			lse.Close()
		})

		Convey("When the context passed to the Read is cancelled", func() {
			lse.(*logStreamExecutor).learnedGLSNEnd.Store(types.MaxGLSN)

			stop := make(chan struct{})
			storage.EXPECT().Read(gomock.Any()).DoAndReturn(func(types.GLSN) ([]byte, error) {
				<-stop
				return []byte("foo"), nil
			}).MaxTimes(1)

			Reset(func() {
				close(stop)
			})

			Convey("Then the LogStreamExecutor should return cancellation error", func(c C) {
				wait := make(chan struct{})
				ctx, cancel := context.WithCancel(context.TODO())
				go func() {
					_, err := lse.Read(ctx, types.MinGLSN)
					c.So(err, ShouldResemble, context.Canceled)
					close(wait)
				}()
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
				cancel()
				<-wait
			})
		})

		Convey("When the operation is blocked more than configured", func() {
			Convey("Then the LogStreamExecutor should return timeout error", func() {
				Convey("This isn't yet implemented", nil)
			})
		})
	})
}

func TestLogStreamExecutorTrim(t *testing.T) {
	Convey("Given that a LogStreamExecutor.Trim is called", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storage := NewMockStorage(ctrl)
		lse, err := NewLogStreamExecutor(types.LogStreamID(1), storage)
		So(err, ShouldBeNil)

		Convey("When the context passed to the Trim is cancelled before enqueueing the trimTask", func() {
			Convey("Then the LogStreamExecutor should return cancellation error", func() {
				ctx, cancel := context.WithCancel(context.TODO())
				cancel()

				var err error
				_, err = lse.Trim(ctx, types.MinGLSN, true)
				So(err, ShouldResemble, context.Canceled)

				_, err = lse.Trim(ctx, types.MinGLSN, false)
				So(err, ShouldResemble, context.Canceled)
			})
		})
	})
}

func TestLogStreamExecutorReplicate(t *testing.T) {
	Convey("Given that LogStreamExecutor.Replicate is called", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storage := NewMockStorage(ctrl)
		lse, err := NewLogStreamExecutor(types.LogStreamID(1), storage)
		So(err, ShouldBeNil)

		Convey("When the context passed to Replicate is canceled before calling storage.Write", func() {
			ctx, cancel := context.WithCancel(context.TODO())
			cancel()

			Convey("Then the Replicate should return cancellation error", func() {
				err := lse.Replicate(ctx, types.MinLLSN, []byte("foo"))
				So(err, ShouldResemble, context.Canceled)
			})
		})
	})
}

func TestLogStreamExecutorSubscribe(t *testing.T) {
	Convey("Given LogStreamExecutor.Subscribe", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storage := NewMockStorage(ctrl)
		lse, err := NewLogStreamExecutor(types.LogStreamID(1), storage)
		So(err, ShouldBeNil)

		lse.Run(context.TODO())

		Reset(func() {
			lse.Close()
		})

		Convey("When the GLSN passed to it is less than LowWatermark", func() {
			Convey("Then the LogStreamExecutor.Subscribe should return an error", func() {
				// ErrAlreadyTrimmed
				Convey("Not yet implemented", nil)
			})
		})

		Convey("When Storage.Scan returns an error", func() {
			storage.EXPECT().Scan(gomock.Any()).Return(nil, varlog.ErrInternal)
			Convey("Then the LogStreamExecutor.Subscribe should return a channel that has the error", func() {
				c, err := lse.Subscribe(context.TODO(), types.MinGLSN)
				So(err, ShouldBeNil)
				So((<-c).err, ShouldNotBeNil)
			})
		})

		Convey("When Storage.Scan returns a valid scanner", func() {
			scanner := NewMockScanner(ctrl)
			storage.EXPECT().Scan(gomock.Any()).Return(scanner, nil)

			Convey("And the Scanner.Next returns an error", func() {
				scanner.EXPECT().Next().Return(varlog.InvalidLogEntry, varlog.ErrInternal)

				Convey("Then the LogStreamExecutor.Subscribe should return a channel that has the error", func() {
					c, err := lse.Subscribe(context.TODO(), types.MinGLSN)
					So(err, ShouldBeNil)
					So((<-c).err, ShouldNotBeNil)

				})
			})

			Convey("And the Scannext.Next returns log entries out of order", func() {
				const repeat = 3
				var cs []*gomock.Call
				for i := 0; i < repeat; i++ {
					logEntry := varlog.LogEntry{
						LLSN: types.MinLLSN + types.LLSN(i),
						GLSN: types.MinGLSN + types.GLSN(i),
					}
					if i == repeat-1 {
						logEntry.LLSN += types.LLSN(1)
					}
					c := scanner.EXPECT().Next().Return(logEntry, nil)
					cs = append(cs, c)
				}
				for i := len(cs) - 1; i > 0; i-- {
					cs[i].After(cs[i-1])
				}
				Convey("Then the LogStreamExecutor.Subscribe should return a channel that has the error", func() {
					c, err := lse.Subscribe(context.TODO(), types.MinGLSN)
					So(err, ShouldBeNil)
					for i := 0; i < repeat-1; i++ {
						So((<-c).err, ShouldBeNil)
					}
					So((<-c).err, ShouldNotBeNil)
				})
			})
		})

	})
}
