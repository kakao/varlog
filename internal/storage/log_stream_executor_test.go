package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

type dummyStorage struct {
	logEntries     [][]byte
	committedGLSNs []types.GLSN
}

func (s *dummyStorage) Read(glsn types.GLSN) ([]byte, error) {
	for glsn > s.committedGLSNs[len(s.committedGLSNs)-1] {
		time.Sleep(10 * time.Millisecond)
	}

	for i := len(s.committedGLSNs) - 1; i >= 0; i-- {
		if glsn == s.committedGLSNs[i] {
			return s.logEntries[i], nil
		}
		if glsn < s.committedGLSNs[i] {
			break
		}
	}

	return nil, fmt.Errorf("no entry")
}

func (s *dummyStorage) Scan(glsn types.GLSN) (Scanner, error) {
	return nil, nil
}

func (s *dummyStorage) Write(llsn types.LLSN, data []byte) error {
	if llsn != types.LLSN(len(s.logEntries)) {
		return fmt.Errorf("bad storage status")
	}
	s.logEntries = append(s.logEntries, data)
	return nil
}

func (s *dummyStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	if llsn != types.LLSN(len(s.committedGLSNs)) {
		return fmt.Errorf("bad storage status")
	}
	s.committedGLSNs = append(s.committedGLSNs, glsn)
	return nil
}

func (s *dummyStorage) Delete(glsn types.GLSN) (uint64, error) {
	return 0, nil
}

func TestLogStreamExecutorAppendAndRead(t *testing.T) {
	const (
		storageNodeID = types.StorageNodeID(0)
		logStreamID1  = types.LogStreamID(1)
		logStreamID2  = types.LogStreamID(2)
	)

	Convey("LogStreamExecutor should append the log entry and read it", t, func() {
		var glsn types.GLSN
		var err error
		var data []byte
		ctx := context.TODO()

		executor1 := NewLogStreamExecutor(logStreamID1, &dummyStorage{})
		executor2 := NewLogStreamExecutor(logStreamID2, &dummyStorage{})
		reporter := NewLogStreamReporter(storageNodeID)

		err = reporter.RegisterLogStreamExecutor(logStreamID1, executor1)
		So(err, ShouldBeNil)
		err = reporter.RegisterLogStreamExecutor(logStreamID2, executor2)
		So(err, ShouldBeNil)

		executor1.Run(context.TODO())
		executor2.Run(context.TODO())
		defer func() {
			executor1.Close()
			executor2.Close()
		}()

		var pending = func(e LogStreamExecutor) bool {
			executor := e.(*logStreamExecutor)
			executor.cwm.mu.RLock()
			defer executor.cwm.mu.RUnlock()
			return len(executor.cwm.m) > 0
		}

		nextGLSN := types.GLSN(0)

		go func() {
			for !pending(executor1) {
				time.Sleep(1 * time.Millisecond)
			}

			reporter.Commit(nextGLSN+1, nextGLSN, []CommittedLogStreamStatus{
				{
					LogStreamID:        logStreamID1,
					NextGLSN:           nextGLSN + 1,
					PrevNextGLSN:       nextGLSN,
					CommittedGLSNBegin: 0,
					CommittedGLSNEnd:   1,
				},
				{
					LogStreamID:        logStreamID2,
					NextGLSN:           nextGLSN + 1,
					PrevNextGLSN:       nextGLSN,
					CommittedGLSNBegin: 0,
					CommittedGLSNEnd:   0,
				},
			})
		}()
		glsn, err = executor1.Append(ctx, []byte("log_001"))
		So(err, ShouldBeNil)
		So(glsn, ShouldEqual, types.GLSN(0))
		data, err = executor1.Read(ctx, glsn)
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, "log_001")

		nextGLSN++

		go func() {
			for !pending(executor1) || !pending(executor2) {
				time.Sleep(1 * time.Millisecond)
			}

			reporter.Commit(nextGLSN+2, nextGLSN, []CommittedLogStreamStatus{
				{
					LogStreamID:        logStreamID1,
					NextGLSN:           nextGLSN + 2,
					PrevNextGLSN:       nextGLSN,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
				},
				{
					LogStreamID:        logStreamID2,
					NextGLSN:           nextGLSN + 2,
					PrevNextGLSN:       nextGLSN,
					CommittedGLSNBegin: 2,
					CommittedGLSNEnd:   3,
				},
			})
		}()

		type appendResult struct {
			glsn types.GLSN
			err  error
		}
		c1 := make(chan appendResult)
		go func() {
			ar := appendResult{}
			ar.glsn, ar.err = executor1.Append(ctx, []byte("log_002"))
			c1 <- ar
		}()

		c2 := make(chan appendResult)
		go func() {
			ar := appendResult{}
			ar.glsn, ar.err = executor2.Append(ctx, []byte("log_003"))
			c2 <- ar
		}()

		var ar appendResult
		ar = <-c1
		So(ar.err, ShouldBeNil)
		So(ar.glsn, ShouldEqual, types.GLSN(1))

		ar = <-c2
		So(ar.err, ShouldBeNil)
		So(ar.glsn, ShouldEqual, types.GLSN(2))

		data, err = executor1.Read(ctx, types.GLSN(1))
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, "log_002")

		data, err = executor2.Read(ctx, types.GLSN(2))
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, "log_003")
	})
}
