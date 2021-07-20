package executor

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/stopchannel"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

// NOTE: When reportCommitBase is operated by atomic, it escapes to the heap. See
// https://github.com/golang/go/issues/16241.
// It is not difficult to be used with sync.Pool, since putting the object into the pool is guarded
// against to the atomic.Load. For these reasons, the shared mutex is used.
type reportCommitBase struct {
	mu                   sync.RWMutex
	globalHighWatermark  types.GLSN
	uncommittedLLSNBegin types.LLSN
}

type logStreamContext struct {
	base reportCommitBase

	uncommittedLLSNEnd types.AtomicLLSN

	commitProgress struct {
		committedLLSNEnd types.LLSN
		mu               sync.RWMutex
	}

	localGLSN struct {
		localLowWatermark  types.AtomicGLSN
		localHighWatermark types.AtomicGLSN
	}
}

func newLogStreamContext() *logStreamContext {
	lsc := &logStreamContext{}

	lsc.storeReportCommitBase(types.InvalidGLSN, types.MinLLSN)

	lsc.uncommittedLLSNEnd.Store(types.MinLLSN)

	lsc.commitProgress.mu.Lock()
	lsc.commitProgress.committedLLSNEnd = types.MinLLSN
	lsc.commitProgress.mu.Unlock()

	lsc.localGLSN.localLowWatermark.Store(types.InvalidGLSN)
	lsc.localGLSN.localHighWatermark.Store(types.InvalidGLSN)

	return lsc
}

func (lsc *logStreamContext) reportCommitBase() (globalHighWatermark types.GLSN, uncommittedLLSNBegin types.LLSN) {
	lsc.base.mu.RLock()
	globalHighWatermark = lsc.base.globalHighWatermark
	uncommittedLLSNBegin = lsc.base.uncommittedLLSNBegin
	lsc.base.mu.RUnlock()
	return
}

func (lsc *logStreamContext) storeReportCommitBase(globalHighWatermark types.GLSN, uncommittedLLSNBegin types.LLSN) {
	lsc.base.mu.Lock()
	lsc.base.globalHighWatermark = globalHighWatermark
	lsc.base.uncommittedLLSNBegin = uncommittedLLSNBegin
	lsc.base.mu.Unlock()
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
	globalHighWatermark, _ := dc.lsc.reportCommitBase()
	return glsn <= globalHighWatermark
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
	//dc.lock.RLock()
	//defer dc.lock.RUnlock()
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
