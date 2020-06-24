package storage

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

const storageNodeID = types.StorageNodeID(0)

type dummyLogStreamExecutor struct {
	id             types.LogStreamID
	knownNextGLSN  types.GLSN
	committedBegin types.LLSN
	committedEnd   types.LLSN
	uncommittedEnd types.LLSN
}

func (e *dummyLogStreamExecutor) GetLogStreamStatus() UncommittedLogStreamStatus {
	return UncommittedLogStreamStatus{
		LogStreamID:          e.id,
		KnownNextGLSN:        e.knownNextGLSN,
		UncommittedLLSNBegin: e.committedEnd,
		UncommittedLLSNEnd:   e.uncommittedEnd,
	}
}

func (e *dummyLogStreamExecutor) CommitLogStreamStatusResult(s CommittedLogStreamStatus) {
	e.knownNextGLSN = s.NextGLSN
	e.committedEnd += types.LLSN(s.CommittedGLSNEnd - s.CommittedGLSNBegin)
}

func TestRegisterLogStream(t *testing.T) {
	Convey("Registering a log stream without LogStreamExecutor should return an error", t, func() {
		reporter := NewLogStreamReporter(storageNodeID)
		err := reporter.RegisterLogStreamExecutor(types.LogStreamID(0), nil)
		So(err, ShouldNotBeNil)
	})

	Convey("Registering the same log stream more than twice should return an error", t, func() {
		var err error
		reporter := NewLogStreamReporter(storageNodeID)
		err = reporter.RegisterLogStreamExecutor(types.LogStreamID(0), &dummyLogStreamExecutor{})
		So(err, ShouldBeNil)
		err = reporter.RegisterLogStreamExecutor(types.LogStreamID(0), &dummyLogStreamExecutor{})
		So(err, ShouldNotBeNil)
	})
}

func TestGetReport(t *testing.T) {
	Convey("GetReport should collect statuses about uncommitted log entries from each LogStreamExecutor", t, func() {
		const nextGLSN = types.GLSN(1)
		reporter := NewLogStreamReporter(storageNodeID)
		reporter.knownNextGLSN = nextGLSN
		var err error

		lse1 := &dummyLogStreamExecutor{
			id:             types.LogStreamID(0),
			knownNextGLSN:  nextGLSN,
			committedBegin: types.LLSN(0),
			committedEnd:   types.LLSN(5),
			uncommittedEnd: types.LLSN(10),
		}
		err = reporter.RegisterLogStreamExecutor(lse1.id, lse1)
		So(err, ShouldBeNil)

		lse2 := &dummyLogStreamExecutor{
			id:             types.LogStreamID(1),
			knownNextGLSN:  nextGLSN,
			committedBegin: types.LLSN(10),
			committedEnd:   types.LLSN(15),
			uncommittedEnd: types.LLSN(20),
		}
		err = reporter.RegisterLogStreamExecutor(lse2.id, lse2)
		So(err, ShouldBeNil)

		lse3 := &dummyLogStreamExecutor{
			id:             types.LogStreamID(2),
			knownNextGLSN:  nextGLSN,
			committedBegin: types.LLSN(20),
			committedEnd:   types.LLSN(25),
			uncommittedEnd: types.LLSN(30),
		}
		err = reporter.RegisterLogStreamExecutor(lse3.id, lse3)
		So(err, ShouldBeNil)

		So(func() { reporter.GetReport() }, ShouldNotPanic)
		knownNextGLSN, reports := reporter.GetReport()
		So(knownNextGLSN, ShouldEqual, nextGLSN)
		So(len(reports), ShouldEqual, 3)

		numCommitted := types.GLSN(2)
		commitResults := []CommittedLogStreamStatus{
			{
				LogStreamID:        lse1.id,
				NextGLSN:           nextGLSN + numCommitted,
				PrevNextGLSN:       nextGLSN,
				CommittedGLSNBegin: 10,
				CommittedGLSNEnd:   10,
			},
			{
				LogStreamID:        lse2.id,
				NextGLSN:           nextGLSN + numCommitted,
				PrevNextGLSN:       nextGLSN,
				CommittedGLSNBegin: 10,
				CommittedGLSNEnd:   11,
			},
			{
				LogStreamID:        lse3.id,
				NextGLSN:           nextGLSN + numCommitted,
				PrevNextGLSN:       nextGLSN,
				CommittedGLSNBegin: 11,
				CommittedGLSNEnd:   12,
			},
		}
		reporter.Commit(nextGLSN+numCommitted, nextGLSN, commitResults)
		So(lse1.knownNextGLSN, ShouldEqual, nextGLSN+numCommitted)
		So(lse2.knownNextGLSN, ShouldEqual, nextGLSN+numCommitted)
		So(lse3.knownNextGLSN, ShouldEqual, nextGLSN+numCommitted)
		So(lse1.committedEnd, ShouldEqual, types.LLSN(5))
		So(lse2.committedEnd, ShouldEqual, types.LLSN(16))
		So(lse3.committedEnd, ShouldEqual, types.LLSN(26))
	})
}
