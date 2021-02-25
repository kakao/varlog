package storagenode

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/util/telemetry/trace"
)

type commitWatcher struct {
	done chan struct{}
	err  error
	once sync.Once

	// notification channel for closing lse
	stopc <-chan struct{}
}

func newCommitWatcher(stopc <-chan struct{}) *commitWatcher {
	return &commitWatcher{
		done:  make(chan struct{}),
		stopc: stopc,
	}
}

func (w *commitWatcher) notify(err error) {
	w.once.Do(func() {
		w.err = err
		close(w.done)
	})
}

func (w *commitWatcher) watch(ctx context.Context) error {
	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-w.stopc:
		return errors.WithStack(errLSEClosed)
	}
}

type appendTask struct {
	data     []byte
	replicas []Replica

	watcher *commitWatcher

	llsn types.LLSN
	glsn types.GLSN

	mu      sync.RWMutex
	primary bool
	written bool

	span               trace.Span
	writeCompletedTime atomicutil.AtomicTime
	commitWaitTime     atomicutil.AtomicDuration

	atTrk *appendTaskTracker
}

func newAppendTask(data []byte, replicas []Replica, llsn types.LLSN, atTrk *appendTaskTracker, stopc <-chan struct{}) *appendTask {
	return &appendTask{
		data:     data,
		replicas: replicas,
		llsn:     llsn,
		watcher:  newCommitWatcher(stopc),
		primary:  llsn == types.InvalidLLSN,
		atTrk:    atTrk,
	}
}

func (t *appendTask) wait(ctx context.Context) error {
	return t.watcher.watch(ctx)
}

func (t *appendTask) notify(err error) {
	t.watcher.notify(err)
}

func (t *appendTask) markWritten(llsn types.LLSN) {
	t.mu.Lock()
	t.written = true
	t.llsn = llsn
	t.mu.Unlock()
}

func (t *appendTask) isPrimary() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.primary
}

func (t *appendTask) getLLSN() types.LLSN {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.llsn
}

func (t *appendTask) setGLSN(glsn types.GLSN) {
	t.mu.Lock()
	t.glsn = glsn
	t.mu.Unlock()
}

func (t *appendTask) getGLSN() types.GLSN {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.glsn
}

func (t *appendTask) getParams() (types.LLSN, []byte, []Replica) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.llsn, t.data, t.replicas
}

func (t *appendTask) close() {
	t.mu.RLock()
	written := t.written
	llsn := t.llsn
	t.mu.RUnlock()
	if written {
		t.atTrk.untrack(llsn)
	}
}

type appendTaskTracker struct {
	mu sync.RWMutex
	m  map[types.LLSN]*appendTask
}

func newAppendTracker() appendTaskTracker {
	return appendTaskTracker{m: make(map[types.LLSN]*appendTask)}
}

func (trk *appendTaskTracker) get(llsn types.LLSN) (*appendTask, bool) {
	trk.mu.RLock()
	t, ok := trk.m[llsn]
	trk.mu.RUnlock()
	return t, ok
}

func (trk *appendTaskTracker) track(llsn types.LLSN, t *appendTask) {
	trk.mu.Lock()
	trk.m[llsn] = t
	trk.mu.Unlock()
}

func (trk *appendTaskTracker) untrack(llsn types.LLSN) {
	trk.mu.Lock()
	delete(trk.m, llsn)
	trk.mu.Unlock()
}

func (trk *appendTaskTracker) foreach(f func(*appendTask)) {
	trk.mu.Lock()
	defer trk.mu.Unlock()
	for _, appendT := range trk.m {
		f(appendT)
	}
}
