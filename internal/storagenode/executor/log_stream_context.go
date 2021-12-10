package executor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/storagenode/stopchannel"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

// NOTE: When reportCommitBase is operated by atomic, it escapes to the heap. See
// https://github.com/golang/go/issues/16241.
// It is not difficult to be used with sync.Pool, since putting the object into the pool is guarded
// against to the atomic.Load. For these reasons, the shared mutex is used.
type reportCommitBase struct {
	mu                   sync.RWMutex
	commitVersion        types.Version
	highWatermark        types.GLSN
	uncommittedLLSNBegin types.LLSN
}

type logStreamContext struct {
	base reportCommitBase

	uncommittedLLSNEnd types.AtomicLLSN

	commitProgress struct {
		committedLLSNEnd types.LLSN
		mu               sync.RWMutex
	}

	localWatermarks struct {
		low  atomic.Value // types.AtomicGLSN -> varlogpb.LogEntryMeta
		high atomic.Value // types.AtomicGLSN -> varlogpb.LogEntryMeta
	}
}

func newLogStreamContext() *logStreamContext {
	lsc := &logStreamContext{}

	lsc.storeReportCommitBase(types.InvalidVersion, types.MinGLSN, types.MinLLSN)

	lsc.uncommittedLLSNEnd.Store(types.MinLLSN)

	lsc.commitProgress.mu.Lock()
	lsc.commitProgress.committedLLSNEnd = types.MinLLSN
	lsc.commitProgress.mu.Unlock()

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

func (lsc *logStreamContext) reportCommitBase() (commitVersion types.Version, highWatermark types.GLSN, uncommittedLLSNBegin types.LLSN) {
	lsc.base.mu.RLock()
	commitVersion = lsc.base.commitVersion
	highWatermark = lsc.base.highWatermark
	uncommittedLLSNBegin = lsc.base.uncommittedLLSNBegin
	lsc.base.mu.RUnlock()
	return
}

func (lsc *logStreamContext) storeReportCommitBase(commitVersion types.Version, highWatermark types.GLSN, uncommittedLLSNBegin types.LLSN) {
	lsc.base.mu.Lock()
	lsc.base.commitVersion = commitVersion
	lsc.base.highWatermark = highWatermark
	lsc.base.uncommittedLLSNBegin = uncommittedLLSNBegin
	lsc.base.mu.Unlock()
}

func (lsc *logStreamContext) localLowWatermark() varlogpb.LogEntryMeta {
	return lsc.localWatermarks.low.Load().(varlogpb.LogEntryMeta)
}

func (lsc *logStreamContext) localHighWatermark() varlogpb.LogEntryMeta {
	return lsc.localWatermarks.high.Load().(varlogpb.LogEntryMeta)
}

func (lsc *logStreamContext) setLocalLowWatermark(localLWM varlogpb.LogEntryMeta) {
	lsc.localWatermarks.low.Store(localLWM)
}

func (lsc *logStreamContext) setLocalHighWatermark(localHWM varlogpb.LogEntryMeta) {
	lsc.localWatermarks.high.Store(localHWM)
}

type decidableCondition struct {
	cv   *sync.Cond
	lock sync.RWMutex
	lsc  *logStreamContext

	// If owner of decidableCondition (i.e., LSE) is about to close, use destroy
	sc *stopchannel.StopChannel
}

func newDecidableCondition(lsc *logStreamContext) *decidableCondition {
	dc := &decidableCondition{
		lsc: lsc,
		sc:  stopchannel.New(),
	}
	dc.cv = sync.NewCond(&dc.lock)
	return dc
}

// If true, the LSE must know the log entry is in this LSE or not.
// If false, the LSE can't guarantee whether the log entry is in this LSE or not.
func (dc *decidableCondition) decidable(glsn types.GLSN) bool {
	_, highWatermark, _ := dc.lsc.reportCommitBase()
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

func (dc *decidableCondition) change(f func()) {
	dc.cv.L.Lock()
	defer dc.cv.L.Unlock()
	f()
	dc.cv.Broadcast()
}

func (dc *decidableCondition) destroy() {
	dc.change(func() {
		dc.sc.Stop()
	})
}
