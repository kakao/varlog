package executor

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// syncState represents the state of source replica while synchronization between two replicas is
// progressing.
type syncState struct {
	cancel context.CancelFunc
	dst    varlogpb.LogStreamReplica
	first  varlogpb.LogEntry
	last   varlogpb.LogEntry

	mu   sync.Mutex
	curr varlogpb.LogEntry
	err  error
}

func newSyncState(cancel context.CancelFunc, dstReplica varlogpb.LogStreamReplica, first, last varlogpb.LogEntry) *syncState {
	return &syncState{
		cancel: cancel,
		dst:    dstReplica,
		first:  first,
		last:   last,
		curr:   varlogpb.InvalidLogEntry(),
	}
}

func (s *syncState) ToSyncStatus() *snpb.SyncStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := &snpb.SyncStatus{
		State:   snpb.SyncStateInProgress,
		First:   snpb.SyncPosition{LLSN: s.first.LLSN, GLSN: s.first.GLSN},
		Last:    snpb.SyncPosition{LLSN: s.last.LLSN, GLSN: s.last.GLSN},
		Current: snpb.SyncPosition{LLSN: s.curr.LLSN, GLSN: s.curr.GLSN},
	}
	// TODO: Add error detail
	if s.err != nil {
		ret.State = snpb.SyncStateError
	} else if s.curr.LLSN == s.last.LLSN {
		ret.State = snpb.SyncStateComplete
	}
	return ret
}

type syncTracker struct {
	tracker map[types.StorageNodeID]*syncState
	wg      sync.WaitGroup
	syncer  func(context.Context, *syncState) error
}

func newSyncTracker(syncer func(context.Context, *syncState) error) *syncTracker {
	return &syncTracker{
		tracker: make(map[types.StorageNodeID]*syncState),
		syncer:  syncer,
	}
}

func (st *syncTracker) get(snID types.StorageNodeID) (*syncState, bool) {
	s, ok := st.tracker[snID]
	return s, ok
}

// run adds synchronization work to the tracker and starts synchronizing. Note that this method is
// not multi goroutine-safe, thus it must be called within mutex.
func (st *syncTracker) run(ctx context.Context, state *syncState, locker sync.Locker) {
	state.mu.Lock()
	replicaSNID := state.dst.StorageNode.StorageNodeID
	state.mu.Unlock()

	st.tracker[replicaSNID] = state

	st.wg.Add(1)
	go func() {
		defer func() {
			locker.Lock()
			delete(st.tracker, replicaSNID)
			locker.Unlock()
			st.wg.Done()
		}()
		_ = st.syncer(ctx, state)
	}()
}

func (st *syncTracker) close() {
	// To shutdown gracefully, closing syncTracker waits for all progressing synchronizations.
	// TODO: add a timeout to avoid deadlock and shutdown forcefully.
	st.wg.Wait()
}

// syncReplicateState represents the state of destination replica while synchronization between two
// replicas is progressing.
type syncReplicateState struct {
	span snpb.SyncRange
	cc   *varlogpb.CommitContext
	ents []*varlogpb.LogEntry
	eos  bool
}

func newSyncReplicateState(span snpb.SyncRange) *syncReplicateState {
	return &syncReplicateState{span: span}
}

func (srs *syncReplicateState) putSyncReplicatePayload(payload snpb.SyncPayload) error {
	if srs == nil {
		return errors.New("invalid syncReplicateState")
	}

	if cc := payload.GetCommitContext(); cc != nil {
		if err := srs.setCommitContext(cc); err != nil {
			return err
		}
	} else if le := payload.GetLogEntry(); le != nil {
		if err := srs.addLogEntry(le); err != nil {
			return err
		}
	} else {
		return errors.New("invalid syncReplicateState")
	}
	return nil
}

func (srs *syncReplicateState) setCommitContext(cc *varlogpb.CommitContext) error {
	if cc == nil {
		return errors.New("invalid commit context")
	}
	if srs.cc != nil {
		return errors.New("unexpected commit context")
	}
	srs.cc = cc
	srs.ents = make([]*varlogpb.LogEntry, 0, cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)
	return nil
}

func (srs *syncReplicateState) addLogEntry(ent *varlogpb.LogEntry) error {
	if ent == nil {
		return errors.New("invalid log entry")
	}
	if srs.cc == nil {
		return errors.New("unexpected log entry")
	}
	srs.ents = append(srs.ents, ent)
	srs.eos = ent.LLSN == srs.span.LastLLSN
	return nil
}

func (srs *syncReplicateState) resetCommitContext() {
	srs.cc = nil
	srs.ents = nil
}

func (srs *syncReplicateState) endOfSync() bool {
	return srs.eos
}
