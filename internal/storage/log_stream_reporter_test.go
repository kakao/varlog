package storage

import (
	"context"
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

func (e *dummyLogStreamExecutor) Run(ctx context.Context) {}
func (e *dummyLogStreamExecutor) Close()                  {}
func (e *dummyLogStreamExecutor) Read(ctx context.Context, glsn types.GLSN) ([]byte, error) {
	return nil, nil
}
func (e *dummyLogStreamExecutor) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
	return nil, nil
}
func (e *dummyLogStreamExecutor) Append(ctx context.Context, data []byte) (types.GLSN, error) {
	return 0, nil
}
func (e *dummyLogStreamExecutor) Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error) {
	return 0, nil
}

func (e *dummyLogStreamExecutor) GetReport() UncommittedLogStreamStatus {
	return UncommittedLogStreamStatus{
		LogStreamID:          e.id,
		KnownNextGLSN:        e.knownNextGLSN,
		UncommittedLLSNBegin: e.committedEnd,
		UncommittedLLSNEnd:   e.uncommittedEnd,
	}
}

func (e *dummyLogStreamExecutor) Commit(s CommittedLogStreamStatus) error {
	e.knownNextGLSN = s.NextGLSN
	e.committedEnd += types.LLSN(s.CommittedGLSNEnd - s.CommittedGLSNBegin)
	return nil
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

func WithLogStreamReporter(nrLSE int, knownNextGLSN types.GLSN, f func(reporter *LogStreamReporter)) func() {
	return func() {
		reporter := NewLogStreamReporter(storageNodeID)
		reporter.knownNextGLSN.Store(knownNextGLSN)
		lseList := []*dummyLogStreamExecutor{}
		for i := 0; i < nrLSE; i++ {
			lse := &dummyLogStreamExecutor{
				id:             types.LogStreamID(i),
				knownNextGLSN:  knownNextGLSN,
				committedBegin: types.LLSN(0),
				committedEnd:   types.LLSN(0),
				uncommittedEnd: types.LLSN(0),
			}
			lseList = append(lseList, lse)
			err := reporter.RegisterLogStreamExecutor(lse.id, lse)
			So(err, ShouldBeNil)
		}
		reporter.Run(context.TODO())
		defer reporter.Close()
		f(reporter)
	}
}

func TestGetReport(t *testing.T) {
	const (
		nrLSE              = 10
		firstKnownNextGLSN = types.GLSN(10)
	)

	Convey("Given a LogStreamExecutor", t, WithLogStreamReporter(nrLSE, firstKnownNextGLSN, func(reporter *LogStreamReporter) {
		Convey("GetReport should return reports from all LogStreamExecutor", func() {
			knownNextGLSN, reports := reporter.GetReport()
			So(knownNextGLSN, ShouldEqual, firstKnownNextGLSN)
			So(len(reports), ShouldEqual, nrLSE)
		})

		Convey("verifyCommit received PrevNextGLSN equal to KnownNextGLSN of LogStreamReporter should return true", func() {
			prevNextGLSN := firstKnownNextGLSN
			ok := reporter.verifyCommit(prevNextGLSN)
			So(ok, ShouldBeTrue)
		})
		Convey("verifyCommit received PrevNextGLSN not equal to KnownNextGLSN of LogStreamReporter should return true", func() {
			prevNextGLSN := firstKnownNextGLSN - 1
			ok := reporter.verifyCommit(prevNextGLSN)
			So(ok, ShouldBeFalse)

			prevNextGLSN = firstKnownNextGLSN + 1
			ok = reporter.verifyCommit(prevNextGLSN)
			So(ok, ShouldBeFalse)
		})

	}))
}
