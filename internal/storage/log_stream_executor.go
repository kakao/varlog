package storage

import (
	"context"
	"fmt"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/syncutil/atomicutil"
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
	Append(ctx context.Context, data []byte, backups ...Replica) (types.GLSN, error)
	Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error)

	Replicate(ctx context.Context, llsn types.LLSN, data []byte) error

	GetReport() UncommittedLogStreamStatus
	Commit(CommittedLogStreamStatus)
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
	data        []byte
	replication bool
	replicas    []Replica
	llsn        types.LLSN
	glsn        types.GLSN
	err         error
	written     bool
	done        chan struct{}

	writeErr     error
	replicateErr error
	commitErr    error
}

type trimTask struct {
	glsn  types.GLSN
	async bool
	nr    uint64
	err   error
	done  chan struct{}
}

type commitTask struct {
	nextGLSN           types.GLSN
	prevNextGLSN       types.GLSN
	committedGLSNBegin types.GLSN
	committedGLSNEnd   types.GLSN
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

const (
	lseAppendCSize = 0
	lseTrimCSize   = 0
	lseCommitCSize = 0
)

// TODO:
// - handle read or subscribe operations competing with trim operations
type logStreamExecutor struct {
	logStreamID types.LogStreamID
	storage     Storage
	replicator  *Replicator

	once sync.Once

	appendC chan *appendTask
	//replicateC chan *appendTask
	commitC chan commitTask
	trimC   chan *trimTask
	cancel  context.CancelFunc

	// lock guard for knownNextGLSN and uncommittedLLSNBegin
	// knownNextGLSN and uncommittedLLSNBegin should be updated simultaneously.
	mu sync.RWMutex

	// knownNextGLSN acts as a version of commit result. In the scope of the same
	// knownNextGLSN, all the reports requested by MR have the same uncommittedLLSNBegin.
	// Because the reports with different values of uncommittedLLSNBegin with the same
	// knownNextGLSN can make some log entries in the log stream have no or more than one GLSN.
	// It is read by GetReport, and written by commit.
	knownNextGLSN types.GLSN // set by Commit

	// uncommittedLLSNBegin is the start of the range that is reported as uncommitted log
	// entries. In the reports marked as the same KnownNextGLSN, uncommittedLLSNBegin of those
	// must be the same value. Because MR calculates commit results based on the KnownNextGLSN
	// and uncommittedLLSNBegin.
	// After all of the commits in a commit result are completed successfully, the value of
	// uncommittedLLSNBegin should be increased by the difference between committedGLSNEnd and
	// committedGLSNBegin in the commit result.
	// It is read by GetReport and written by commit.
	uncommittedLLSNBegin types.LLSN

	// committedLLSNEnd is the next position to be committed to storage. It is used only by
	// commit function.
	// Sometimes, it can be greater than uncommittedLLSNBegin. See the reason why below.
	// It is read and written only by commit.
	committedLLSNEnd types.LLSN // increased only by Commit

	// uncommittedLLSNEnd is the tail of the LogStreamExecutor. It indicates the next position
	// for the log entry to be written. It is increased after successful writing to the storage
	// during the append operation.
	// It is read by GetReport, written by Append
	uncommittedLLSNEnd types.AtomicLLSN

	// atomic access
	// committedGLSNBegin   types.GLSN // R: read & subscribe W: Trim
	// lastCommittedGLSN types.GLSN // R: read W: Commit
	// committedGLSNBegin and committedGLSNEnd are knowledges which are learned from Trim and
	// Commit operations.
	//
	// learnedGLSNBegin and committedGLSNEnd are knowledge that is learned from Trim and
	// Commit operations. Exact values of them are maintained in the storage. By learning
	// these values in the LogStreamExecutor, it can be avoided to unnecessary requests to
	// the storage.
	learnedGLSNBegin types.AtomicGLSN
	learnedGLSNEnd   types.AtomicGLSN

	cwm *commitWaitMap

	sealed atomicutil.AtomicBool
}

func NewLogStreamExecutor(logStreamID types.LogStreamID, storage Storage) LogStreamExecutor {
	lse := &logStreamExecutor{
		logStreamID: logStreamID,
		storage:     storage,
		cwm:         newCommitWaitMap(),
		appendC:     make(chan *appendTask, lseAppendCSize),
		trimC:       make(chan *trimTask, lseTrimCSize),
		commitC:     make(chan commitTask, lseCommitCSize),
	}
	return lse
}

func (lse *logStreamExecutor) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	lse.cancel = cancel
	go lse.dispatchAppendC(ctx)
	go lse.dispatchTrimC(ctx)
	go lse.dispatchCommitC(ctx)
}

func (lse *logStreamExecutor) Close() {
	lse.cancel()
}

func (lse *logStreamExecutor) isSealed() bool {
	return lse.sealed.Load()
}

func (lse *logStreamExecutor) seal() {
	lse.sealed.Store(true)
}

func (lse *logStreamExecutor) dispatchAppendC(ctx context.Context) {
	for {
		select {
		case t := <-lse.appendC:
			lse.prepare(ctx, t)
		case <-ctx.Done():
			return
		}
	}
}

/*
func (lse *logStreamExecutor) dispatchReplicateC(ctx context.Context) {
	for {
		select {
		case t := <-lse.replicateC:
			lse.triggerReplication(ctx, t)
		case <-ctx.Done():
			return
		}
	}
}
*/

func (lse *logStreamExecutor) dispatchTrimC(ctx context.Context) {
	for {
		select {
		case t := <-lse.trimC:
			lse.trim(t)
		case <-ctx.Done():
			return
		}
	}
}

func (lse *logStreamExecutor) dispatchCommitC(ctx context.Context) {
	for {
		select {
		case t := <-lse.commitC:
			lse.commit(t)
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
func (lse *logStreamExecutor) Read(ctx context.Context, glsn types.GLSN) ([]byte, error) {
	if ok, _ := lse.isTrimmed(glsn); ok {
		return nil, varlog.ErrTrimmed
	}
	// TODO: wait until decidable or return an error
	if lse.commitUndecidable(glsn) {
		return nil, varlog.ErrUndecidable
	}
	done := make(chan struct{})
	task := &readTask{
		glsn: glsn,
		done: done,
	}
	go lse.read(task)
	select {
	case <-done:
		return task.data, task.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (lse *logStreamExecutor) isTrimmed(glsn types.GLSN) (bool, types.GLSN) {
	learnedGLSNBegin := lse.learnedGLSNBegin.Load()
	return glsn < learnedGLSNBegin, learnedGLSNBegin
}

func (lse *logStreamExecutor) commitUndecidable(glsn types.GLSN) bool {
	return glsn >= lse.learnedGLSNEnd.Load()
}

func (lse *logStreamExecutor) read(t *readTask) {
	// TODO: wait undecidable read operation
	// for lse.commitUndecidable(t.glsn) {
	//	runtime.Gosched()
	//}
	t.data, t.err = lse.storage.Read(t.glsn)
	close(t.done)
}

func (lse *logStreamExecutor) Subscribe(ctx context.Context, glsn types.GLSN) (<-chan SubscribeResult, error) {
	out := make(chan SubscribeResult)
	go lse.subscribe(ctx, glsn, out)
	return out, nil
}

func (lse *logStreamExecutor) subscribe(ctx context.Context, glsn types.GLSN, c chan<- SubscribeResult) {
	defer close(c)
	scanner, err := lse.storage.Scan(glsn)
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

func (lse *logStreamExecutor) Replicate(ctx context.Context, llsn types.LLSN, data []byte) error {
	if lse.isSealed() {
		return varlog.ErrSealed
	}
	done := make(chan struct{})
	t := appendTask{
		llsn:        llsn,
		data:        data,
		done:        done,
		replication: true,
	}
	lse.appendC <- &t
	<-done
	return t.err
}

// Append appends a log entry at the end of the log stream. Append comprises of three parts -
// writing log entry into the underlying storage, replicating the log entry to backups, and
// waiting for commit completion event. After receiving commit completion event, Append returns
// given GLSN.
// If the log stream is locked, the append is failed.
// All Appends are processed sequentially by using the appendC.
func (lse *logStreamExecutor) Append(ctx context.Context, data []byte, replicas ...Replica) (types.GLSN, error) {
	if lse.isSealed() {
		return 0, varlog.ErrSealed
	}
	done := make(chan struct{})
	t := appendTask{
		data:     data,
		replicas: replicas,
		done:     done,
	}
	lse.appendC <- &t
	<-done
	if t.written {
		lse.cwm.del(t.llsn)
	}
	return t.glsn, t.err
}

func (lse *logStreamExecutor) prepare(ctx context.Context, t *appendTask) {
	if t.replication {
		t.err = lse.replicateTo(t.llsn, t.data)
		if t.err != nil {
			lse.seal()
		}
		close(t.done)
		return
	}
	if err := lse.write(t); err != nil {
		lse.seal()
		t.err = err
		close(t.done)
		return
	}
	if len(t.replicas) > 0 {
		lse.triggerReplication(ctx, t)
	}
}

// append issues new LLSN for a LogEntry and write it to the storage. It, then, adds given
// appendTask to the taskmap to receive commit result. It also triggers replication after a
// successful write.
func (lse *logStreamExecutor) write(t *appendTask) error {
	llsn := lse.uncommittedLLSNEnd.Load()
	if err := lse.storage.Write(llsn, t.data); err != nil {
		return err
	}
	t.written = true
	t.llsn = llsn
	lse.cwm.put(llsn, t)
	lse.uncommittedLLSNEnd.Add(1)
	return nil
}

func (lse *logStreamExecutor) replicateTo(llsn types.LLSN, data []byte) error {
	if llsn != lse.uncommittedLLSNEnd.Load() {
		return fmt.Errorf("LLSN mismatch")
	}
	if err := lse.storage.Write(llsn, data); err != nil {
		return err
	}
	lse.uncommittedLLSNEnd.Add(1)
	// TODO: cwm
	return nil
}

func (lse *logStreamExecutor) triggerReplication(ctx context.Context, t *appendTask) {
	errC := lse.replicator.Replicate(ctx, t.llsn, t.data, t.replicas)
	go func() {
		select {
		case err := <-errC:
			if err == nil {
				return
			}
			// FIXME: does t have a concurrency issue?
			t.err = err
		case <-ctx.Done():
			t.err = ctx.Err()
		}
		lse.seal()
		close(t.done)
	}()
}

func (lse *logStreamExecutor) Trim(ctx context.Context, glsn types.GLSN, async bool) (uint64, error) {
	if ok, _ := lse.isTrimmed(glsn); ok {
		return 0, varlog.ErrTrimmed
	}

	done := make(chan struct{})
	task := &trimTask{
		glsn:  glsn,
		async: async,
		done:  done,
	}
	lse.trimC <- task
	if async {
		return 0, nil
	}
	<-done
	return task.nr, task.err
}

func (lse *logStreamExecutor) trim(t *trimTask) {
	defer close(t.done)
	if ok, _ := lse.isTrimmed(t.glsn); ok {
		t.err = varlog.ErrTrimmed
		return
	}
	t.nr, t.err = lse.storage.Delete(t.glsn)
	if t.err == nil {
		lse.learnedGLSNBegin.Store(t.glsn + 1)
	}
}

func (lse *logStreamExecutor) GetReport() UncommittedLogStreamStatus {
	// TODO: If this is sealed, ...
	lse.mu.RLock()
	defer lse.mu.RUnlock()
	return UncommittedLogStreamStatus{
		LogStreamID:          lse.logStreamID,
		KnownNextGLSN:        lse.knownNextGLSN,
		UncommittedLLSNBegin: lse.uncommittedLLSNBegin,
		UncommittedLLSNEnd:   lse.uncommittedLLSNEnd.Load(),
	}
}

func (lse *logStreamExecutor) Commit(cr CommittedLogStreamStatus) {
	if lse.logStreamID != cr.LogStreamID {
		return
	}
	if lse.isSealed() {
		return
	}
	if !lse.verifyCommit(cr.PrevNextGLSN) {
		return
	}

	lse.commitC <- commitTask{
		nextGLSN:           cr.NextGLSN,
		prevNextGLSN:       cr.PrevNextGLSN,
		committedGLSNBegin: cr.CommittedGLSNBegin,
		committedGLSNEnd:   cr.CommittedGLSNEnd,
	}
}

func (lse *logStreamExecutor) verifyCommit(prevNextGLSN types.GLSN) bool {
	lse.mu.RLock()
	defer lse.mu.RUnlock()
	// If the LSE is newbie knownNextGLSN is zero. In LSE-wise commit, it is unnecessary,
	return lse.knownNextGLSN == 0 || lse.knownNextGLSN == prevNextGLSN
}

func (lse *logStreamExecutor) commit(t commitTask) {
	if lse.isSealed() {
		return
	}
	if !lse.verifyCommit(t.prevNextGLSN) {
		return
	}

	commitOk := true
	glsn := t.committedGLSNBegin
	for glsn < t.committedGLSNEnd {
		llsn := lse.committedLLSNEnd
		if err := lse.storage.Commit(llsn, glsn); err != nil {
			// NOTE: The LogStreamExecutor fails to commit Log entries that are
			// assigned GLSN by MR, for example, because of the storage failure.
			// In other replicated storage nodes, it can be okay.
			// Should we lock, that is, finalize this log stream or need other else
			// mechanisms?
			commitOk = false
			lse.seal()
			break
		}
		lse.committedLLSNEnd++
		lse.learnedGLSNEnd.Store(glsn + 1)
		appendT, ok := lse.cwm.get(llsn)
		if ok {
			appendT.glsn = glsn
			close(appendT.done)
		}
		glsn++
	}

	// NOTE: This is a very subtle case. MR assigns GLSNs to these log entries, but the storage
	// fails to commit it. Actually, these GLSNs are holes. See the above comments.
	llsn := lse.committedLLSNEnd
	for glsn < t.committedGLSNEnd {
		appendT, ok := lse.cwm.get(llsn)
		if ok {
			appendT.glsn = glsn
			appendT.err = varlog.ErrInternal
			close(appendT.done)
		}
		glsn++
		llsn++
	}

	if commitOk {
		lse.mu.Lock()
		lse.knownNextGLSN = t.nextGLSN
		lse.uncommittedLLSNBegin += types.LLSN(t.committedGLSNEnd - t.committedGLSNBegin)
		lse.mu.Unlock()
	}
}
