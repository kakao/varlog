package logstream

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/stopchannel"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

// reportCommitBase represents the base of the report and commit. All
// reportCommitBase across all replicas in the same log stream should be
// consistent during successful operations.
//
// Internally, it reflects the last commit message sent from the metadata
// repository.
// The commitVersion is the version of the commit message.
// The highWatermark is the global high watermark, the maximum GLSN included in
// the commit message.
// The uncommittedLLSNBegin is the first LLSN that the replica will commit,
// computed by both offset and length of committed logs in the commit message.
// If the reportCommitBase is invalid, it cannot reflect the last commit
// message. Usually, it happens since the replica failed during
// synchronization.
type reportCommitBase struct {
	// NOTE: When reportCommitBase is operated by atomic, it escapes to the heap. See
	// https://github.com/golang/go/issues/16241.
	// It is not difficult to be used with sync.Pool, since putting the object into the pool is guarded
	// against to the atomic.Load. For these reasons, the shared mutex is used.
	mu                   sync.RWMutex
	commitVersion        types.Version
	highWatermark        types.GLSN
	uncommittedLLSNBegin types.LLSN
	invalid              bool
}

// logStreamContext represents the context of a log stream replica - the last
// commit context and positions of committed and uncommitted logs.
type logStreamContext struct {
	base               reportCommitBase // base of report and commit in the log stream
	uncommittedLLSNEnd types.AtomicLLSN // expected LLSN to be written
	localWatermarks    struct {
		low  atomic.Value // local low watermark; varlogpb.LogEntryMeta
		high atomic.Value // local high watermark; varlogpb.LogEntryMeta
	}
}

// newLogStreamContext creates a new log stream context.
func newLogStreamContext() *logStreamContext {
	lsc := &logStreamContext{}
	lsc.storeReportCommitBase(types.InvalidVersion, types.InvalidGLSN, types.MinLLSN, false)
	lsc.uncommittedLLSNEnd.Store(types.MinLLSN)
	lsc.localWatermarks.low.Store(varlogpb.LogEntryMeta{
		LLSN: types.InvalidLLSN,
		GLSN: types.InvalidGLSN,
	})
	lsc.localWatermarks.high.Store(varlogpb.LogEntryMeta{
		LLSN: types.InvalidLLSN,
		GLSN: types.InvalidGLSN,
	})
	return lsc
}

// reportCommitBase returns the base of report and commit in the log stream.
func (lsc *logStreamContext) reportCommitBase() (commitVersion types.Version, highWatermark types.GLSN, uncommittedLLSNBegin types.LLSN, invalid bool) {
	lsc.base.mu.RLock()
	commitVersion = lsc.base.commitVersion
	highWatermark = lsc.base.highWatermark
	uncommittedLLSNBegin = lsc.base.uncommittedLLSNBegin
	invalid = lsc.base.invalid
	lsc.base.mu.RUnlock()
	return
}

// storeReportCommitBase stores the base of report and commit in the log stream.
func (lsc *logStreamContext) storeReportCommitBase(commitVersion types.Version, highWatermark types.GLSN, uncommittedLLSNBegin types.LLSN, invalid bool) {
	lsc.base.mu.Lock()
	lsc.base.commitVersion = commitVersion
	lsc.base.highWatermark = highWatermark
	lsc.base.uncommittedLLSNBegin = uncommittedLLSNBegin
	lsc.base.invalid = invalid
	lsc.base.mu.Unlock()
}

// localLowWatermark returns the local low watermark.
func (lsc *logStreamContext) localLowWatermark() varlogpb.LogEntryMeta {
	return lsc.localWatermarks.low.Load().(varlogpb.LogEntryMeta)
}

// localHighWatermark returns the local high watermark.
func (lsc *logStreamContext) localHighWatermark() varlogpb.LogEntryMeta {
	return lsc.localWatermarks.high.Load().(varlogpb.LogEntryMeta)
}

// setLocalLowWatermark sets the local low watermark.
func (lsc *logStreamContext) setLocalLowWatermark(localLWM varlogpb.LogEntryMeta) {
	lsc.localWatermarks.low.Store(localLWM)
}

// setLocalHighWatermark sets the local high watermark.
func (lsc *logStreamContext) setLocalHighWatermark(localHWM varlogpb.LogEntryMeta) {
	lsc.localWatermarks.high.Store(localHWM)
}

// decidableCondition is a wrapper of condition variable to wait for new logs committed.
type decidableCondition struct {
	// FIXME (jun): There is no reason to use shared mutex. Use mutex.
	mu sync.RWMutex
	cv *sync.Cond

	lsc *logStreamContext

	// If owner of decidableCondition (i.e., LSE) is about to close, use destroy
	sc *stopchannel.StopChannel
}

// newDecidableCondition creates a new decidableCondition.
func newDecidableCondition(lsc *logStreamContext) *decidableCondition {
	dc := &decidableCondition{
		lsc: lsc,
		sc:  stopchannel.New(),
	}
	dc.cv = sync.NewCond(&dc.mu)
	return dc
}

// decidable returns true if the decidableCondition is decidable.
// The caller can decide whether the logs are stored in the log stream or not if this method returns true. Otherwise,
// the caller cannot guarantee the existence of the log in the log stream.
func (dc *decidableCondition) decidable(glsn types.GLSN) bool {
	_, highWatermark, _, _ := dc.lsc.reportCommitBase()
	return glsn <= highWatermark
}

// NOTE: Canceling ctx is not a guarantee that this waitC is wakeup immediately.
func (dc *decidableCondition) waitC(ctx context.Context, glsn types.GLSN) error {
	dc.cv.L.Lock()
	defer dc.cv.L.Unlock()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dc.sc.StopC():
			return errors.WithStack(verrors.ErrClosed)
		default:
		}
		if dc.decidable(glsn) {
			return nil
		}
		dc.cv.Wait()
	}
}

// change tells all waiters of decidableCondition that they need to check decidableCondition again.
// The argument f is a function that modifies log stream context.
func (dc *decidableCondition) change(f func()) {
	dc.cv.L.Lock()
	defer dc.cv.L.Unlock()
	f()
	dc.cv.Broadcast()
}

// destroy removes decidableCondition.
// It wakes up all waiters that waiting for the decidableCondition. The waiters will get an error - verrors.ErrClosed.
func (dc *decidableCondition) destroy() {
	dc.change(func() {
		dc.sc.Stop()
	})
}
