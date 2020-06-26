package storage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
)

type SubscribeResult struct {
	data []byte
	err  error
}

type LogStreamExecutor interface {
	Run(ctx context.Context)
	Close()

	Read(ctx context.Context, glsn types.GLSN) ([]byte, error)
	Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error)
	Append(ctx context.Context, data []byte) (types.GLSN, error)
	Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error)

	GetReport() UncommittedLogStreamStatus
	Commit(CommittedLogStreamStatus) error
}

type readTask struct {
	glsn types.GLSN
	data []byte
	err  error
	done chan struct{}
}

type subscribeTask struct {
	glsn types.GLSN
	data []byte
	err  error
}

type appendTask struct {
	data []byte
	llsn types.LLSN
	glsn types.GLSN
	err  error
	done chan struct{}
}

type trimTask struct {
	glsn  types.GLSN
	async bool
	nr    uint64
	err   error
	done  chan struct{}
}

type commitWaitMap struct {
	m  map[types.LLSN]*appendTask
	mu sync.RWMutex
}

func newCommitWaitMap() *commitWaitMap {
	return &commitWaitMap{
		m: make(map[types.LLSN]*appendTask),
	}
}

func (cwm *commitWaitMap) get(llsn types.LLSN) (*appendTask, bool) {
	cwm.mu.RLock()
	defer cwm.mu.RUnlock()
	t, ok := cwm.m[llsn]
	return t, ok
}

func (cwm *commitWaitMap) put(llsn types.LLSN, t *appendTask) {
	cwm.mu.Lock()
	defer cwm.mu.Unlock()
	cwm.m[llsn] = t
}

func (cwm *commitWaitMap) del(llsn types.LLSN) {
	cwm.mu.Lock()
	defer cwm.mu.Unlock()
	delete(cwm.m, llsn)
}

type logStreamExecutor struct {
	logStreamID types.LogStreamID
	storage     Storage

	once       sync.Once
	trimC      chan *trimTask
	replicateC chan struct{}
	cancel     context.CancelFunc

	// lock guard for knownNextGLSN and uncommittedLLSNBegin
	mu sync.RWMutex

	// knownNextGLSN
	// read by get_report
	// write by commit
	knownNextGLSN types.GLSN // set by Commit

	// committedLLSNEnd
	// read & write by Commit
	// NO NEED TO LATCH
	committedLLSNEnd types.LLSN // increased only by Commit

	// uncommittedLLSNBegin
	// read: ready by get_report
	// write: increased by commit
	uncommittedLLSNBegin types.LLSN // increased only by Commit

	// uncommittedLLSNEnd
	// read: read by get_report
	// write: increased by append
	// use atomic
	// uncommittedLLSNEnd types.LLSN
	uncommittedLLSNEnd types.AtomicLLSN

	// atomic access
	// committedGLSNBegin   types.GLSN // R: read & subscribe W: Trim
	// lastCommittedGLSN types.GLSN // R: read W: Commit
	committedGLSNBegin types.AtomicGLSN

	committedGLSNEnd types.AtomicGLSN

	mtxAppend sync.Mutex
	mtxCommit sync.Mutex

	cwm *commitWaitMap

	sealed int32
}

func NewLogStreamExecutor(logStreamID types.LogStreamID, storage Storage) LogStreamExecutor {
	lse := &logStreamExecutor{
		logStreamID: logStreamID,
		storage:     storage,
		cwm:         newCommitWaitMap(),
		replicateC:  make(chan struct{}),
	}
	return lse
}

func (e *logStreamExecutor) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	go e.dispatchReplicateC(ctx)
	go e.dispatchTrimC(ctx)
}

func (e *logStreamExecutor) Close() {
	e.cancel()
}

func (e *logStreamExecutor) isSealed() bool {
	return atomic.LoadInt32(&e.sealed) > 0
}

func (e *logStreamExecutor) seal() {
	atomic.StoreInt32(&e.sealed, 1)
}

func (e *logStreamExecutor) dispatchReplicateC(ctx context.Context) {
	for {
		select {
		case <-e.replicateC:
		case <-ctx.Done():
			return
		}
	}
}

func (e *logStreamExecutor) dispatchTrimC(ctx context.Context) {
	for {
		select {
		case t := <-e.trimC:
			e.trim(t)
		case <-ctx.Done():
			return
		}
	}
}

// Read reads log entry located at given GLSN. Multiple read operations can be processed
// simultaneously regardless of other operations.
//
// FIXME: This is dummy implementation.
// - spinining early-read to decide NOENT or OK
// - read aggregation to minimize I/O
// - cache or not (use memstore?)
func (e *logStreamExecutor) Read(ctx context.Context, glsn types.GLSN) ([]byte, error) {
	// TODO: mutex or atomic
	if ok, _ := e.isTrimmed(glsn); ok {
		return nil, varlog.ErrTrimmed
	}
	if e.isUncommitted(glsn) {
		return nil, varlog.ErrNotYetCommitted
	}

	done := make(chan struct{})
	task := &readTask{
		glsn: glsn,
		done: done,
	}
	go e.read(task)
	select {
	case <-done:
		return task.data, task.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *logStreamExecutor) isTrimmed(glsn types.GLSN) (bool, types.GLSN) {
	committedGLSNBegin := e.committedGLSNBegin.Load()
	return glsn < committedGLSNBegin, committedGLSNBegin
	// notTrimmed := e.committedGLSNBegin.Load() + 1
	// notTrimmed := types.GLSN(atomic.LoadUint64((*uint64)(&e.lastTrimmedGLSN)) + 1)
	//return glsn < notTrimmed, notTrimmed
	/*
		e.mu.RLock()
		defer e.mu.RUnlock()
		notTrimmed := e.lastTrimmedGLSN + 1
		return glsn < notTrimmed, notTrimmed
	*/
}

func (e *logStreamExecutor) isUncommitted(glsn types.GLSN) bool {
	return glsn >= e.committedGLSNEnd.Load()
	// return glsn > types.GLSN(atomic.LoadUint64((*uint64)(&e.lastCommittedGLSN)))
	/*
		e.mu.RLock()
		defer e.mu.RUnlock()
		return glsn > e.lastCommittedGLSN
	*/
}

func (e *logStreamExecutor) read(t *readTask) {
	t.data, t.err = e.storage.Read(t.glsn)
	close(t.done)
}

func (e *logStreamExecutor) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
	// TODO: mutex or atomic
	if ok, _ := e.isTrimmed(glsn); ok {
		return nil, varlog.ErrTrimmed
	}
	out := make(chan SubscribeResult)
	go e.subscribe(ctx, glsn, out)
	return out, nil
}

func (e *logStreamExecutor) subscribe(ctx context.Context, glsn types.GLSN, c chan<- SubscribeResult) {
	defer close(c)
	if ok, nextGLSN := e.isTrimmed(glsn); ok {
		glsn = nextGLSN
	}
	scanner, err := e.storage.Scan(glsn)
	if err != nil {
		c <- SubscribeResult{err: err}
		return
	}
	for {
		select {
		case <-ctx.Done():
			c <- SubscribeResult{err: ctx.Err()}
			return
		default:
			data, err := scanner.Next()
			c <- SubscribeResult{
				data: data,
				err:  err,
			}
			if err != nil {
				return
			}
		}
	}
}

// Append appends a log entry at the end of the log stream. Append comprises of three parts -
// writing log entry into the underlying storage, replicating the log entry to backups, and
// waiting for commit completion event. After receiving commit completion event, Append returns
// given GLSN.
// If the log stream is locked, the append is failed.
// All Appends are processed sequentially by using the appendC.
func (e *logStreamExecutor) Append(ctx context.Context, data []byte) (types.GLSN, error) {
	if e.isSealed() {
		return 0, varlog.ErrSealed
	}

	done := make(chan struct{})
	t := &appendTask{
		data: data,
		done: done,
	}

	if err := e.prepare(t); err != nil {
		return 0, err
	}

	<-done
	e.cwm.del(t.llsn)
	return t.glsn, t.err
}

func (e *logStreamExecutor) prepare(t *appendTask) error {
	e.mtxAppend.Lock()
	defer e.mtxAppend.Unlock()

	if err := e.write(t); err != nil {
		e.seal()
		return err
	}
	e.triggerReplication(t)
	return nil
}

// append issues new LLSN for a LogEntry and write it to the storage. It, then, adds given
// appendTask to the taskmap to receive commit result. It also triggers replication after a
// successful write.
func (e *logStreamExecutor) write(t *appendTask) error {
	// llsn := e.uncommittedLLSNEnd
	llsn := e.uncommittedLLSNEnd.Load()
	if err := e.storage.Write(llsn, t.data); err != nil {
		return err
	}
	t.llsn = llsn
	e.cwm.put(llsn, t)
	e.uncommittedLLSNEnd.Add(1)
	// e.uncommittedLLSNEnd++
	return nil
}

func (e *logStreamExecutor) triggerReplication(t *appendTask) {
	e.replicateC <- struct{}{}
}

func (e *logStreamExecutor) Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error) {
	done := make(chan struct{})
	task := &trimTask{
		glsn:  glsn,
		async: async,
		done:  done,
	}
	e.trimC <- task
	if async {
		return 0, nil
	}
	<-done
	return task.nr, task.err
}

func (e *logStreamExecutor) trim(t *trimTask) {
	defer close(t.done)
	if ok, _ := e.isTrimmed(t.glsn); ok {
		return
	}
	t.nr, t.err = e.storage.Delete(t.glsn)
	if t.err == nil {
		e.committedGLSNBegin.Store(t.glsn + 1)
		//atomic.StoreUint64((*uint64)(&e.lastTrimmedGLSN), (uint64)(t.glsn))
		//e.mu.Lock()
		//e.lastTrimmedGLSN = t.glsn
		//e.mu.Unlock()
	}
}

func (e *logStreamExecutor) GetReport() UncommittedLogStreamStatus {
	// TODO: If this is sealed, report is constant.
	e.mu.RLock()
	defer e.mu.RUnlock()
	return UncommittedLogStreamStatus{
		LogStreamID:          e.logStreamID,
		KnownNextGLSN:        e.knownNextGLSN,
		UncommittedLLSNBegin: e.uncommittedLLSNBegin,
		UncommittedLLSNEnd:   e.uncommittedLLSNEnd.Load(),
	}
}

func (e *logStreamExecutor) Commit(cr CommittedLogStreamStatus) error {
	if e.logStreamID != cr.LogStreamID {
		return varlog.ErrInvalid
	}

	if e.isSealed() {
		return varlog.ErrSealed
	}

	e.mtxCommit.Lock()
	defer e.mtxCommit.Unlock()

	// TODO: mismatched KnownNextGLSN
	// If knownNextGLSN of LogStreamExecutor is zero, the LogStreamExecutor is in initial state.
	e.mu.RLock()
	badKnownNextGLSN := e.knownNextGLSN != 0 && e.knownNextGLSN != cr.PrevNextGLSN
	e.mu.RUnlock()
	if badKnownNextGLSN {
		return varlog.ErrInvalid
	}
	return e.commit(cr)
}

func (e *logStreamExecutor) commit(cr CommittedLogStreamStatus) error {
	var err error
	e.mu.Lock()
	e.knownNextGLSN = cr.NextGLSN
	e.uncommittedLLSNBegin += types.LLSN(cr.CommittedGLSNEnd - cr.CommittedGLSNBegin)
	e.mu.Unlock()

	glsn := cr.CommittedGLSNBegin
	for glsn < cr.CommittedGLSNEnd {
		llsn := e.committedLLSNEnd
		if err := e.storage.Commit(llsn, glsn); err != nil {
			// NOTE: The LogStreamExecutor fails to commit Log entries that are
			// assigned GLSN by MR, for example, because of the storage failure.
			// In other replicated storage nodes, it can be okay.
			// Should we lock, that is, finalize this log stream or need other else
			// mechanisms?
			e.seal()
			break
		}
		appendT, ok := e.cwm.get(llsn)
		if ok {
			appendT.glsn = glsn
			close(appendT.done)
		}
		e.committedLLSNEnd++
		glsn++
		e.committedGLSNEnd.Store(glsn)
		// atomic.StoreUint64((*uint64)(&e.lastCommittedGLSN), uint64(glsn))
		/*
			e.mu.Lock()
			e.lastCommittedGLSN = glsn
			e.mu.Unlock()
		*/

	}

	// NOTE: This is a very subtle case. MR assigns GLSNs to these log entries, but the storage
	// fails to commit it. Actually, these GLSNs are holes. See the above comments.
	llsn := e.committedLLSNEnd
	for glsn < cr.CommittedGLSNEnd {
		appendT, ok := e.cwm.get(llsn)
		if ok {
			appendT.glsn = glsn
			appendT.err = varlog.ErrInternal
			close(appendT.done)
		}
		glsn++
		llsn++
	}

	return err
}
