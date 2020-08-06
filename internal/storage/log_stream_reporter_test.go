package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

func TestLogStreamReporterRunClose(t *testing.T) {
	Convey("LogStreamReporter should be run and closed", t, func() {
		lsr := NewLogStreamReporter(types.StorageNodeID(0))
		So(func() { lsr.Run(context.TODO()) }, ShouldNotPanic)
		So(func() { lsr.Close() }, ShouldNotPanic)
		So(func() { lsr.Close() }, ShouldNotPanic)
	})
}

func TestLogStreamReporterRegisterLogStreamExecutor(t *testing.T) {
	Convey("LogStreamReporter", t, func() {
		lsr := NewLogStreamReporter(types.StorageNodeID(0))

		Convey("it should not register nil executor", func() {
			err := lsr.RegisterLogStreamExecutor(nil)
			So(err, ShouldNotBeNil)
		})

		Convey("it should not register already existing LSE", func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var err error
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
			err = lsr.RegisterLogStreamExecutor(lse)
			So(err, ShouldBeNil)
			err = lsr.RegisterLogStreamExecutor(lse)
			So(err, ShouldResemble, varlog.ErrExist)
		})
	})
}

func TestLogStreamReporterGetReport(t *testing.T) {
	Convey("Given a LogStreamReporter", t, func() {
		const N = 3

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsr := NewLogStreamReporter(types.StorageNodeID(0)).(*logStreamReporter)
		lsr.Run(context.TODO())

		lseList := []LogStreamExecutor{}
		for i := 1; i <= N; i++ {
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(types.LogStreamID(i)).AnyTimes()
			So(lsr.RegisterLogStreamExecutor(lse), ShouldBeNil)
			lseList = append(lseList, lse)
		}

		Reset(func() {
			lsr.Close()
		})

		// The zero value of KnownNextGLSN of LogStreamExecutor means that the LogStream is
		// just added to StorageNode.
		Convey("When KnownNextGLSNs of every LogStreamExecutors are zero", func() {
			for _, lse := range lseList {
				lse.(*MockLogStreamExecutor).EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:          lse.LogStreamID(),
						KnownNextGLSN:        types.GLSN(0),
						UncommittedLLSNBegin: types.LLSN(0),
						UncommittedLLSNEnd:   types.LLSN(10),
					},
				)
			}

			Convey("Then KnownNextGLSN of the report should be zero", func() {
				knownNextGLSN, reports := lsr.GetReport()
				So(knownNextGLSN, ShouldEqual, types.GLSN(0))
				So(len(reports), ShouldEqual, len(lseList))

				Convey("And the report should be stored in history", func() {
					lsr.Close()
					r, ok := lsr.history[knownNextGLSN]
					So(ok, ShouldBeTrue)
					So(r, ShouldResemble, reports)
				})
			})
		})

		Convey("When KnownNextGLSNs of every LogStreamExecutors are different", func() {
			for i, lse := range lseList {
				lse.(*MockLogStreamExecutor).EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:          lse.LogStreamID(),
						KnownNextGLSN:        types.GLSN(10 * i),
						UncommittedLLSNBegin: types.LLSN(0),
						UncommittedLLSNEnd:   types.LLSN(10),
					},
				)
			}

			Convey("Then the KnownNextGLSN of the report should be minimum, not zero", func() {
				knownNextGLSN, reports := lsr.GetReport()
				So(knownNextGLSN, ShouldEqual, types.GLSN(10))
				So(len(reports), ShouldEqual, len(lseList))

				Convey("And the report should be stored in history", func() {
					lsr.Close()
					r, ok := lsr.history[knownNextGLSN]
					So(ok, ShouldBeTrue)
					So(r, ShouldResemble, reports)
				})
			})
		})

		Convey("When KnownNextGLSN of the report is computed again", func() {
			for i, lse := range lseList {
				first := lse.(*MockLogStreamExecutor).EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:          lse.LogStreamID(),
						KnownNextGLSN:        types.GLSN(10),
						UncommittedLLSNBegin: types.LLSN(0),
						UncommittedLLSNEnd:   types.LLSN(10),
					},
				)
				lse.(*MockLogStreamExecutor).EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:          lse.LogStreamID(),
						KnownNextGLSN:        types.GLSN(10),
						UncommittedLLSNBegin: types.LLSN(0),
						UncommittedLLSNEnd:   types.LLSN(10 * i),
					},
				).After(first)
			}

			Convey("Then the history should have the past non-empty report with the same KnownNextGLSN", func() {
				knownNextGLSN1, reports1 := lsr.GetReport()
				So(len(reports1), ShouldEqual, len(lseList))
				So(knownNextGLSN1, ShouldEqual, types.GLSN(10))

				knownNextGLSN2, reports2 := lsr.GetReport()
				So(len(reports2), ShouldEqual, len(lseList))
				So(knownNextGLSN2, ShouldEqual, types.GLSN(10))

				So(knownNextGLSN1, ShouldEqual, knownNextGLSN2)
				So(reports1, ShouldResemble, reports2)
			})
		})

		Convey("When KnownNextGLSN of the report is computed", func() {
			for _, lse := range lseList {
				first := lse.(*MockLogStreamExecutor).EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:          lse.LogStreamID(),
						KnownNextGLSN:        types.GLSN(10),
						UncommittedLLSNBegin: types.LLSN(0),
						UncommittedLLSNEnd:   types.LLSN(10),
					},
				)
				lse.(*MockLogStreamExecutor).EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:          lse.LogStreamID(),
						KnownNextGLSN:        types.GLSN(100),
						UncommittedLLSNBegin: types.LLSN(10),
						UncommittedLLSNEnd:   types.LLSN(20),
					},
				).After(first)
			}

			Convey("Then past reports whose KnownNextGLSNs are less than the KnownNextGLSN just computed should be deleted", func() {
				knownNextGLSN1, reports1 := lsr.GetReport()
				So(len(reports1), ShouldEqual, len(lseList))
				So(knownNextGLSN1, ShouldEqual, types.GLSN(10))

				knownNextGLSN2, reports2 := lsr.GetReport()
				So(len(reports2), ShouldEqual, len(lseList))
				So(knownNextGLSN2, ShouldEqual, types.GLSN(100))

				lsr.Close()
				_, ok := lsr.history[knownNextGLSN2]
				So(ok, ShouldBeTrue)
				_, ok = lsr.history[knownNextGLSN1]
				So(ok, ShouldBeFalse)
			})
		})
	})
}

func TestLogStreamReporterCommit(t *testing.T) {
	Convey("LogStreamReporter", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsr := NewLogStreamReporter(types.StorageNodeID(0)).(*logStreamReporter)
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()

		Convey("it should reject a commit result whose prevNextGLSN is not equal to own knownNextGLSN",
			func() {
				lsr.knownNextGLSN.Store(10)
				lsr.Commit(types.GLSN(10), types.GLSN(5), nil)
				So(len(lsr.commitC), ShouldEqual, 0)

				lsr.Commit(types.GLSN(20), types.GLSN(15), nil)
				So(len(lsr.commitC), ShouldEqual, 0)
			},
		)

		Convey("it should reject an empty commit result", func() {
			lsr.knownNextGLSN.Store(10)

			lsr.Commit(types.GLSN(15), types.GLSN(10), nil)
			So(len(lsr.commitC), ShouldEqual, 0)

			lsr.Commit(types.GLSN(15), types.GLSN(10), []CommittedLogStreamStatus{})
			So(len(lsr.commitC), ShouldEqual, 0)
		})

		Convey("it should change knownNextGLSN after call LSE.Commit", func() {
			lsr.RegisterLogStreamExecutor(lse1)
			lsr.RegisterLogStreamExecutor(lse2)

			lsr.Run(context.TODO())
			defer lsr.Close()

			lsr.knownNextGLSN.Store(10)
			oldKnownNextGLSN := lsr.knownNextGLSN.Load()
			var wg sync.WaitGroup
			wg.Add(2)
			lse1.EXPECT().Commit(gomock.Any()).DoAndReturn(func(CommittedLogStreamStatus) error {
				defer wg.Done()
				return nil
			}).AnyTimes()
			lse2.EXPECT().Commit(gomock.Any()).DoAndReturn(func(CommittedLogStreamStatus) error {
				defer wg.Done()
				return nil
			}).AnyTimes()

			lsr.Commit(types.GLSN(20), types.GLSN(10), []CommittedLogStreamStatus{
				{
					LogStreamID:        lse1.LogStreamID(),
					NextGLSN:           types.GLSN(20),
					PrevNextGLSN:       types.GLSN(10),
					CommittedGLSNBegin: types.GLSN(100),
					CommittedGLSNEnd:   types.GLSN(105),
				},
				{
					LogStreamID:        lse2.LogStreamID(),
					NextGLSN:           types.GLSN(20),
					PrevNextGLSN:       types.GLSN(10),
					CommittedGLSNBegin: types.GLSN(105),
					CommittedGLSNEnd:   types.GLSN(110),
				},
			})
			wg.Wait()
			for oldKnownNextGLSN == lsr.knownNextGLSN.Load() {
				time.Sleep(time.Millisecond)
			}
			So(lsr.knownNextGLSN.Load(), ShouldEqual, types.GLSN(20))
		})
	})
}
