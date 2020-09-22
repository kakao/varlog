package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
	"go.uber.org/zap"
)

func TestLogStreamReporterRunClose(t *testing.T) {
	Convey("LogStreamReporter should be run and closed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lsr := NewLogStreamReporter(zap.NewNop(), types.StorageNodeID(0), lseGetter, &DefaultLogStreamReporterOptions)
		So(func() { lsr.Run(context.TODO()) }, ShouldNotPanic)
		So(func() { lsr.Close() }, ShouldNotPanic)
		So(func() { lsr.Close() }, ShouldNotPanic)
	})
}

func TestLogStreamReporterReport(t *testing.T) {
	Convey("LogStreamReporter.report", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// 2 LogStreamExecutors
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()

		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lseGetter.EXPECT().GetLogStreamExecutors().Return([]LogStreamExecutor{lse1, lse2}).Times(3)

		logger, err := zap.NewDevelopment()
		So(err, ShouldBeNil)

		var t *lsrReportTask
		lsr := NewLogStreamReporter(logger, types.StorageNodeID(1), lseGetter, &DefaultLogStreamReporterOptions).(*logStreamReporter)

		// This map is write-once and immutable for each HWM, LSID pair
		// map<"HWM_LSID", UncommittedLLSNOffset>
		invariants := make(map[string]types.LLSN)
		key := func(hwm types.GLSN, lsid types.LogStreamID) string {
			return fmt.Sprintf("%v_%v", hwm, lsid)
		}
		checkInvariants := func(reports map[types.LogStreamID]UncommittedLogStreamStatus) {
			for _, report := range reports {
				k := key(t.knownHighWatermark, report.LogStreamID)
				if _, ok := invariants[k]; !ok {
					invariants[k] = report.UncommittedLLSNOffset
				}
				So(invariants[k], ShouldEqual, report.UncommittedLLSNOffset)
			}
		}

		// HWM=0
		// LSE1: no progress
		lse1.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse1.LogStreamID(),
			KnownHighWatermark:    0,
			UncommittedLLSNOffset: 1,
			UncommittedLLSNLength: 0,
		})
		// LSE2: no progress
		lse2.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse2.LogStreamID(),
			KnownHighWatermark:    0,
			UncommittedLLSNOffset: 1,
			UncommittedLLSNLength: 0,
		})
		t = &lsrReportTask{done: make(chan struct{})}
		lsr.report(t)
		So(t.knownHighWatermark, ShouldEqual, 0)
		So(t.reports[0].KnownHighWatermark, ShouldEqual, 0)
		So(t.reports[1].KnownHighWatermark, ShouldEqual, 0)
		checkInvariants(t.reports)

		// Commit (HWM=1)
		// LSE1: #append=1, but slow-updater (KnownHighWatermark is still 0)
		lse1.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse1.LogStreamID(),
			KnownHighWatermark:    0,
			UncommittedLLSNOffset: 1,
			UncommittedLLSNLength: 1,
		})
		// LSE2: no progress, but learned that highWatermark is 1 by MR
		lse2.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse2.LogStreamID(),
			KnownHighWatermark:    1,
			UncommittedLLSNOffset: 1,
			UncommittedLLSNLength: 0,
		})
		t = &lsrReportTask{done: make(chan struct{})}
		lsr.report(t)
		So(t.knownHighWatermark, ShouldEqual, 0)
		checkInvariants(t.reports)

		// LSE1: now updated (at last, KnownHighWatermark is 1)
		lse1.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse1.LogStreamID(),
			KnownHighWatermark:    1,
			UncommittedLLSNOffset: 2,
			UncommittedLLSNLength: 0,
		})
		// LSE2: no progress
		lse2.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse2.LogStreamID(),
			KnownHighWatermark:    1,
			UncommittedLLSNOffset: 1,
			UncommittedLLSNLength: 0,
		})
		t = &lsrReportTask{done: make(chan struct{})}
		lsr.report(t)
		So(t.knownHighWatermark, ShouldEqual, 1)
		checkInvariants(t.reports)

		// Newbie (LSE3) comes.
		lse3 := NewMockLogStreamExecutor(ctrl)
		lse3.EXPECT().LogStreamID().Return(types.LogStreamID(3)).AnyTimes()
		lseGetter.EXPECT().GetLogStreamExecutors().Return([]LogStreamExecutor{lse1, lse2, lse3}).AnyTimes()

		// LSE1: append, but slow-updater
		lse1.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse1.LogStreamID(),
			KnownHighWatermark:    1,
			UncommittedLLSNOffset: 2,
			UncommittedLLSNLength: 1,
		})
		// LSE2: append
		lse2.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse2.LogStreamID(),
			KnownHighWatermark:    3,
			UncommittedLLSNOffset: 2,
			UncommittedLLSNLength: 0,
		})
		// LSE3: newbie
		lse3.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
			LogStreamID:           lse3.LogStreamID(),
			KnownHighWatermark:    0,
			UncommittedLLSNOffset: 1,
			UncommittedLLSNLength: 0,
		})
		t = &lsrReportTask{done: make(chan struct{})}
		lsr.report(t)
		So(t.knownHighWatermark, ShouldEqual, 1)
		checkInvariants(t.reports)

		/*
			// LSE1: stil not updated (slow-updater)
			lse1.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
				LogStreamID:           lse1.LogStreamID(),
				KnownHighWatermark:    1,
				UncommittedLLSNOffset: 2,
				UncommittedLLSNLength: 1,
			})
			// LSE2: append more
			lse2.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
				LogStreamID:           lse2.LogStreamID(),
				KnownHighWatermark:    3,
				UncommittedLLSNOffset: 2,
				UncommittedLLSNLength: 1,
			})
			// LSE3: newbie
			lse3.EXPECT().GetReport().Return(UncommittedLogStreamStatus{
				LogStreamID:           lse3.LogStreamID(),
				KnownHighWatermark:    0,
				UncommittedLLSNOffset: 1,
				UncommittedLLSNLength: 1,
			})
		*/
	})
}

func TestLogStreamReporterGetReportTimeout(t *testing.T) {
	Convey("Given LogStremReporter", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		opts := DefaultLogStreamReporterOptions
		opts.ReportCTimeout = time.Duration(0)
		opts.ReportWaitTimeout = time.Duration(0)

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lsr := NewLogStreamReporter(zap.NewNop(), types.StorageNodeID(0), lseGetter, &opts)

		lse := NewMockLogStreamExecutor(ctrl)
		lse.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		setLseGetter(lseGetter, lse)

		lsr.Run(context.TODO())

		Reset(func() {
			lsr.Close()
		})

		Convey("When LogStreamReporter.GetReport is timed out", func() {
			wait := make(chan struct{})
			lse.EXPECT().GetReport().DoAndReturn(
				func() UncommittedLogStreamStatus {
					<-wait
					return UncommittedLogStreamStatus{}
				},
			).MaxTimes(1)

			Convey("Then LogStreamReporter.GetReport should return timeout error", func() {
				_, _, err := lsr.GetReport(context.TODO())
				So(err, ShouldResemble, context.DeadlineExceeded)
				close(wait)
			})
		})
	})
}

func TestLogStreamReporterGetReport(t *testing.T) {
	Convey("Given a LogStreamReporter", t, func() {
		const N = 3

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lsr := NewLogStreamReporter(zap.NewNop(), types.StorageNodeID(0), lseGetter, &DefaultLogStreamReporterOptions).(*logStreamReporter)
		lsr.Run(context.TODO())

		var lseList []*MockLogStreamExecutor
		for i := 1; i <= N; i++ {
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(types.LogStreamID(i)).AnyTimes()
			lseList = append(lseList, lse)
		}
		setLseGetter(lseGetter, lseList...)

		Reset(func() {
			lsr.Close()
		})

		// The zero value of KnownHighWatermark of LogStreamExecutor means that
		// the LogStream is just added to StorageNode.
		Convey("When KnownHighWatermark of every LogStreamExecutors are zero", func() {
			for _, lse := range lseList {
				lse.EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:           lse.LogStreamID(),
						KnownHighWatermark:    types.InvalidGLSN,
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: 10,
					},
				)
			}

			Convey("Then KnownHighWatermark of the report should be zero", func() {
				knownHighWatermark, reports, err := lsr.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(knownHighWatermark, ShouldEqual, types.InvalidGLSN)
				So(len(reports), ShouldEqual, len(lseList))

				Convey("And the report should be stored in history", func() {
					lsr.Close()
					r, ok := lsr.history[knownHighWatermark]
					So(ok, ShouldBeTrue)
					So(r, ShouldResemble, reports)
				})
			})
		})

		Convey("When KnownHighWatermark of every LogStreamExecutors are different", func() {
			for i, lse := range lseList {
				lse.EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:           lse.LogStreamID(),
						KnownHighWatermark:    types.GLSN(10 * i),
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: 10,
					},
				)
			}

			Convey("Then the KnownHighWatermark of the report should be minimum", func() {
				knownHighWatermark, reports, err := lsr.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(knownHighWatermark, ShouldEqual, types.GLSN(0))
				So(len(reports), ShouldEqual, len(lseList))

				Convey("And the report should be stored in history", func() {
					lsr.Close()
					r, ok := lsr.history[knownHighWatermark]
					So(ok, ShouldBeTrue)
					So(r, ShouldResemble, reports)
				})
			})
		})

		Convey("When KnownNextGLSN of the report is computed again", func() {
			for i, lse := range lseList {
				first := lse.EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:           lse.LogStreamID(),
						KnownHighWatermark:    types.GLSN(10),
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: 10,
					},
				)
				lse.EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:           lse.LogStreamID(),
						KnownHighWatermark:    types.GLSN(10),
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: uint64(10 * i),
					},
				).After(first)
			}

			Convey("Then the history should have the past non-empty report with the same KnownHighWatermark", func() {
				knownHighWatermark1, reports1, err := lsr.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(len(reports1), ShouldEqual, len(lseList))
				So(knownHighWatermark1, ShouldEqual, types.GLSN(10))

				knownHighWatermark2, reports2, err := lsr.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(len(reports2), ShouldEqual, len(lseList))
				So(knownHighWatermark2, ShouldEqual, types.GLSN(10))

				So(knownHighWatermark1, ShouldEqual, knownHighWatermark2)
			})
		})

		Convey("When KnownHighWatermark of the report is computed", func() {
			for _, lse := range lseList {
				first := lse.EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:           lse.LogStreamID(),
						KnownHighWatermark:    types.GLSN(10),
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: 10,
					},
				)
				lse.EXPECT().GetReport().Return(
					UncommittedLogStreamStatus{
						LogStreamID:           lse.LogStreamID(),
						KnownHighWatermark:    types.GLSN(100),
						UncommittedLLSNOffset: types.LLSN(10),
						UncommittedLLSNLength: 20,
					},
				).After(first)
			}

			Convey("Then past reports whose KnownHighWatermark are less than the KnownHighWatermark just computed should be deleted", func() {
				knownHighWatermark1, reports1, err := lsr.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(len(reports1), ShouldEqual, len(lseList))
				So(knownHighWatermark1, ShouldEqual, types.GLSN(10))

				knownHighWatermark2, reports2, err := lsr.GetReport(context.TODO())
				So(err, ShouldBeNil)
				So(len(reports2), ShouldEqual, len(lseList))
				So(knownHighWatermark2, ShouldEqual, types.GLSN(100))

				lsr.Close()
				_, ok := lsr.history[knownHighWatermark2]
				So(ok, ShouldBeTrue)
				_, ok = lsr.history[knownHighWatermark1]
				So(ok, ShouldBeFalse)
			})
		})
	})
}

func TestLogStreamReporterCommit(t *testing.T) {
	Convey("LogStreamReporter", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lseGetter := NewMockLogStreamExecutorGetter(ctrl)
		lsr := NewLogStreamReporter(zap.NewNop(), types.StorageNodeID(0), lseGetter, &DefaultLogStreamReporterOptions).(*logStreamReporter)
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()

		setLseGetter(lseGetter, lse1, lse2)

		Convey("it should reject an empty commit result", func() {
			err := lsr.Commit(context.TODO(), types.GLSN(15), types.GLSN(10), nil)
			So(err, ShouldNotBeNil)
			So(len(lsr.commitC), ShouldEqual, 0)

			err = lsr.Commit(context.TODO(), types.GLSN(15), types.GLSN(10), []CommittedLogStreamStatus{})
			So(err, ShouldNotBeNil)
			So(len(lsr.commitC), ShouldEqual, 0)
		})
	})
}
