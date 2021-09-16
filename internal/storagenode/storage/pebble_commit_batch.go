package storage

import (
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
)

var pebbleCommitBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleCommitBatch{}
	},
}

type pebbleCommitBatch struct {
	b        *pebble.Batch
	ps       *pebbleStorage
	cc       CommitContext
	snapshot struct {
		prevWrittenLLSN   types.LLSN
		prevCommittedLLSN types.LLSN
		prevCommittedGLSN types.GLSN
	}
	progress struct {
		prevCommittedLLSN types.LLSN
		prevCommittedGLSN types.GLSN
	}
	/*
		prevWrittenLLSN   types.LLSN // snapshot
		prevCommittedLLSN types.LLSN // fixed value
		prevCommittedGLSN types.GLSN // increment value
	*/
}

var _ CommitBatch = (*pebbleCommitBatch)(nil)

func newPebbleCommitBatch() *pebbleCommitBatch {
	return pebbleCommitBatchPool.Get().(*pebbleCommitBatch)
}

func (pcb *pebbleCommitBatch) release() {
	pcb.b = nil
	pcb.ps = nil
	pcb.cc = InvalidCommitContext
	pcb.snapshot.prevWrittenLLSN = types.InvalidLLSN
	pcb.snapshot.prevCommittedLLSN = types.InvalidLLSN
	pcb.snapshot.prevCommittedGLSN = types.InvalidGLSN
	pcb.progress.prevCommittedLLSN = types.InvalidLLSN
	pcb.progress.prevCommittedGLSN = types.InvalidGLSN
	pebbleCommitBatchPool.Put(pcb)
}

func (pcb *pebbleCommitBatch) Put(llsn types.LLSN, glsn types.GLSN) error {
	if llsn.Invalid() || glsn.Invalid() {
		return errors.New("storage: invalid log position")
	}

	if pcb.progress.prevCommittedLLSN+1 != llsn {
		return errors.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", pcb.progress.prevCommittedLLSN, llsn)
	}

	if pcb.progress.prevCommittedGLSN+1 != glsn {
		return errors.Errorf("storage: incorrect GLSN, prev_glsn=%v curr_glsn=%v", pcb.progress.prevCommittedGLSN, glsn)
	}

	if glsn >= pcb.cc.CommittedGLSNEnd {
		return errors.New("invalid commit")
	}

	if llsn > pcb.snapshot.prevWrittenLLSN {
		return errors.New("storage: unwritten log")
	}

	// TODO: validate glsn against commitContext (glsn interval)

	/*
		if numCommit > 0 && prevCommittedLLSN.Invalid() {
			return errors.New("storage: invalid batch")
		}
		if !prevCommittedLLSN.Invalid() && prevCommittedLLSN+1 != llsn {
			return errors.Errorf("storage: incorrect LLSN, prev_llsn=%v curr_llsn=%v", prevCommittedLLSN, llsn)
		}
		if pcb.prevCommittedGLSN >= glsn {
			return errors.Errorf("storage: incorrect GLSN, prev_glsn=%v curr_glsn=%v", pcb.prevCommittedGLSN, glsn)
		}

		if llsn > pcb.prevWrittenLLSN {
			return errors.New("storage: unwritten log")
		}
	*/

	ck := encodeCommitKey(glsn)
	dk := encodeDataKey(llsn)
	if err := pcb.b.Set(ck, dk, pcb.ps.commitOption); err != nil {
		return errors.WithStack(err)
	}
	pcb.progress.prevCommittedLLSN = llsn
	pcb.progress.prevCommittedGLSN = glsn
	return nil
}

func (pcb *pebbleCommitBatch) Apply() error {
	return pcb.ps.applyCommitBatch(pcb)
}

func (pcb *pebbleCommitBatch) Close() error {
	err := pcb.b.Close()
	pcb.release()
	return err
}

func (pcb *pebbleCommitBatch) numCommits() int {
	return int(pcb.progress.prevCommittedLLSN - pcb.snapshot.prevCommittedLLSN)
}
