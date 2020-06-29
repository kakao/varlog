package storage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
)

type failedStorage struct {
	nrRead   int32
	nrScan   int32
	nrWrite  int32
	nrCommit int32
	nrDelete int32
}

func (s *failedStorage) Read(glsn types.GLSN) ([]byte, error) {
	atomic.AddInt32(&s.nrRead, 1)
	return nil, varlog.ErrInternal
}

func (s *failedStorage) Scan(glsn types.GLSN) (Scanner, error) {
	atomic.AddInt32(&s.nrScan, 1)
	return nil, varlog.ErrInternal
}

func (s *failedStorage) Write(llsn types.LLSN, data []byte) error {
	atomic.AddInt32(&s.nrWrite, 1)
	return varlog.ErrInternal
}

func (s *failedStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	atomic.AddInt32(&s.nrCommit, 1)
	return varlog.ErrInternal
}

func (s *failedStorage) Delete(glsn types.GLSN) (uint64, error) {
	atomic.AddInt32(&s.nrDelete, 1)
	return 0, varlog.ErrInternal
}

type dummyStorage struct {
	logEntries     [][]byte
	committedGLSNs []types.GLSN
	nrRead         int32
	nrScan         int32
	nrWrite        int32
	nrCommit       int32
	nrDelete       int32
}

func (s *dummyStorage) Read(glsn types.GLSN) ([]byte, error) {
	atomic.AddInt32(&s.nrRead, 1)
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
	atomic.AddInt32(&s.nrScan, 1)
	return nil, nil
}

func (s *dummyStorage) Write(llsn types.LLSN, data []byte) error {
	atomic.AddInt32(&s.nrWrite, 1)
	if llsn != types.LLSN(len(s.logEntries)) {
		return fmt.Errorf("bad storage status")
	}
	s.logEntries = append(s.logEntries, data)
	return nil
}

func (s *dummyStorage) Commit(llsn types.LLSN, glsn types.GLSN) error {
	atomic.AddInt32(&s.nrCommit, 1)
	if llsn != types.LLSN(len(s.committedGLSNs)) {
		return fmt.Errorf("bad storage status")
	}
	s.committedGLSNs = append(s.committedGLSNs, glsn)
	return nil
}

func (s *dummyStorage) Delete(glsn types.GLSN) (uint64, error) {
	atomic.AddInt32(&s.nrDelete, 1)
	return 0, nil
}

func TestLogStreamExecutorWithFailedStorage(t *testing.T) {
	Convey("Given LogStreamExecutor with failed Storage", t, func() {
		executor := NewLogStreamExecutor(types.LogStreamID(1), &failedStorage{})
		executor.Run(context.TODO())
		defer executor.Close()

		Convey("Reading a log entry should return an error", func() {
			So(executor.(*logStreamExecutor).isSealed(), ShouldBeFalse)
			_, err := executor.Read(context.TODO(), 0)
			So(err, ShouldNotBeNil)
		})

		Convey("Appending a log entry should return an error, and the LogStreamExecutor should be sealed", func() {
			So(executor.(*logStreamExecutor).isSealed(), ShouldBeFalse)
			_, err := executor.Append(context.TODO(), nil)
			So(err, ShouldNotBeNil)
			So(executor.(*logStreamExecutor).isSealed(), ShouldBeTrue)
		})

		Convey("Committing should make it sealed", func() {
			So(executor.(*logStreamExecutor).isSealed(), ShouldBeFalse)
			executor.Commit(CommittedLogStreamStatus{
				LogStreamID:        executor.(*logStreamExecutor).logStreamID,
				NextGLSN:           1,
				PrevNextGLSN:       0,
				CommittedGLSNBegin: 0,
				CommittedGLSNEnd:   1,
			})
			nrCommit := &executor.(*logStreamExecutor).storage.(*failedStorage).nrCommit
			for atomic.LoadInt32(nrCommit) < 1 {
				time.Sleep(time.Millisecond * 10)
			}
			So(executor.(*logStreamExecutor).isSealed(), ShouldBeTrue)
		})
	})
}

func TestSealedLogStreamExecutor(t *testing.T) {
	Convey("With Sealed LogStreamExecutor", t, func() {
		storage := &dummyStorage{}
		executor := NewLogStreamExecutor(types.LogStreamID(1), storage)
		executor.Run(context.TODO())
		defer executor.Close()

		executor.(*logStreamExecutor).sealed = 1
		So(storage.Write(types.LLSN(0), []byte("log_001")), ShouldBeNil)
		So(storage.Commit(types.LLSN(0), types.GLSN(0)), ShouldBeNil)
		executor.(*logStreamExecutor).learnedGLSNEnd.Store(1)

		Convey("Read should return written log entry", func() {
			data, err := executor.Read(context.TODO(), types.GLSN(0))
			So(err, ShouldBeNil)
			So(string(data), ShouldEqual, "log_001")
		})

		Convey("Append should return an error", func() {
			_, err := executor.Append(context.TODO(), []byte("log_001"))
			So(err, ShouldNotBeNil)
		})
	})
}

func TestLogStreamExecutorAppendAndRead(t *testing.T) {
	const (
		storageNodeID = types.StorageNodeID(0)
		logStreamID1  = types.LogStreamID(1)
		logStreamID2  = types.LogStreamID(2)
	)

	sleepUntil := func(storage *dummyStorage, pred func(*dummyStorage) bool) {
		for !pred(storage) {
			time.Sleep(1 * time.Millisecond)
		}
	}

	sleepUntilTargetNrWrite := func(storage *dummyStorage, targetNrWrite int32) {
		sleepUntil(storage, func(s *dummyStorage) bool {
			return atomic.LoadInt32(&s.nrWrite) == targetNrWrite
		})
	}

	Convey("LogStreamExecutor should append the log entry and read it", t, func() {
		var glsn types.GLSN
		var err error
		var data []byte
		ctx := context.TODO()

		reporter := NewLogStreamReporter(storageNodeID)
		reporter.Run(ctx)
		defer reporter.Close()

		storage1 := &dummyStorage{}
		executor1 := NewLogStreamExecutor(logStreamID1, storage1)
		executor1.Run(context.TODO())
		defer executor1.Close()
		err = reporter.RegisterLogStreamExecutor(logStreamID1, executor1)
		So(err, ShouldBeNil)

		storage2 := &dummyStorage{}
		executor2 := NewLogStreamExecutor(logStreamID2, storage2)
		executor2.Run(context.TODO())
		defer executor2.Close()
		err = reporter.RegisterLogStreamExecutor(logStreamID2, executor2)
		So(err, ShouldBeNil)

		nextGLSN := types.GLSN(0)

		go func() {
			sleepUntilTargetNrWrite(storage1, 1)
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
			sleepUntilTargetNrWrite(storage1, 2)
			sleepUntilTargetNrWrite(storage2, 1)
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
