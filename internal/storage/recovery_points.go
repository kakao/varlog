package storage

import (
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type RecoveryPoints struct {
	LastCommitContext *CommitContext
	CommittedLogEntry struct {
		First *varlogpb.LogEntryMeta
		Last  *varlogpb.LogEntryMeta
	}
	//UncommittedLLSN struct {
	//	First types.LLSN
	//	Last  types.LLSN
	//}
}

func (s *Storage) ReadRecoveryPoints() (rp RecoveryPoints, err error) {
	rp.CommittedLogEntry.First, rp.CommittedLogEntry.Last, err = s.readLogEntryBoundaries()
	if err != nil {
		return
	}
	var lastNonempty *CommitContext
	rp.LastCommitContext, lastNonempty = s.readLastCommitContext()

	// TODO: Find valid commit context and log entries rather than returning an error.
	if lastNonempty != nil && (rp.CommittedLogEntry.Last == nil || lastNonempty.CommittedGLSNEnd-1 != rp.CommittedLogEntry.Last.GLSN) {
		err = fmt.Errorf("storage: mismatched commit context and log entries")
		return
	}
	if lastNonempty == nil && rp.CommittedLogEntry.First != nil {
		err = fmt.Errorf("storage: mismatched commit context and log entries")
		return
	}
	return rp, nil
}

// readLastCommitContext returns the last commit context and the last non-empty commit context.
// It returns nil if not exists.
func (s *Storage) readLastCommitContext() (last, lastNonempty *CommitContext) {
	it := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitContextKeyPrefix},
		UpperBound: []byte{commitContextKeySentinelPrefix},
	})
	defer func() {
		_ = it.Close()
	}()

	if !it.Last() {
		return nil, nil
	}
	cc := decodeCommitContextKey(it.Key())
	last = &cc
	if !cc.Empty() {
		lastNonempty = &cc
		return last, lastNonempty
	}
	it.Prev()
	for it.Valid() {
		cc := decodeCommitContextKey(it.Key())
		if !cc.Empty() {
			lastNonempty = &cc
			break
		}
		it.Prev()
	}
	return last, lastNonempty
}

func (s *Storage) readLogEntryBoundaries() (first, last *varlogpb.LogEntryMeta, err error) {
	it := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = it.Close()
	}()

	if !it.First() {
		return nil, nil, nil
	}
	firstGLSN := decodeCommitKey(it.Key())
	firstLE, err := s.readGLSN(firstGLSN)
	if err != nil {
		return nil, nil, err
	}
	first = &firstLE.LogEntryMeta

	_ = it.Last()
	lastGLSN := decodeCommitKey(it.Key())
	lastLE, err := s.readGLSN(lastGLSN)
	if err != nil {
		return first, nil, err
	}
	return first, &lastLE.LogEntryMeta, nil
}

//func (s *Storage) readUncommittedLogEntryBoundaries(lastCommitted *varlogpb.LogEntryMeta) (first, last types.LLSN) {
//	dk := make([]byte, dataKeyLength)
//	dk = encodeDataKeyInternal(lastCommitted.LLSN+1, dk)
//	it := s.db.NewIter(&pebble.IterOptions{
//		LowerBound: dk,
//		UpperBound: []byte{dataKeySentinelPrefix},
//	})
//	defer func() {
//		_ = it.Close()
//	}()
//
//	if !it.First() {
//		return types.InvalidLLSN, types.InvalidLLSN
//	}
//	first = decodeDataKey(it.Key())
//	_ = it.Last()
//	last = decodeDataKey(it.Key())
//	return first, last
//}
