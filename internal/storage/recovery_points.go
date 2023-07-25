package storage

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

type RecoveryPoints struct {
	LastCommitContext *CommitContext
	CommittedLogEntry struct {
		First *varlogpb.LogSequenceNumber
		Last  *varlogpb.LogSequenceNumber
	}
	UncommittedLLSN struct {
		Begin types.LLSN
		End   types.LLSN
	}
}

// ReadRecoveryPoints reads data necessary to restore the status of a log
// stream replica - the first and last log entries and commit context.
// It is okay when the commit context is not matched with the last log entry,
// resolved through synchronization between replicas later. The first and last
// log entries can be nil if there is no log entry or they can't be read due to
// inconsistency between data and commit. However, if there is a fatal error,
// it returns an error.
func (s *Storage) ReadRecoveryPoints() (rp RecoveryPoints, err error) {
	rp.LastCommitContext, err = s.readLastCommitContext()
	if err != nil {
		return
	}
	rp.CommittedLogEntry.First, rp.CommittedLogEntry.Last = s.readLogEntryBoundaries()

	uncommittedBegin := types.MinLLSN
	if cc := rp.LastCommitContext; cc != nil {
		uncommittedBegin = cc.CommittedLLSNBegin + types.LLSN(cc.CommittedGLSNEnd-cc.CommittedGLSNBegin)
	}
	rp.UncommittedLLSN.Begin, rp.UncommittedLLSN.End, err = s.readUncommittedLogEntryBoundaries(uncommittedBegin)
	if err != nil {
		return
	}

	return rp, nil
}

// readLastCommitContext returns the last commit context.
// It returns nil if not exists.
func (s *Storage) readLastCommitContext() (*CommitContext, error) {
	cc, err := s.ReadCommitContext()
	if err != nil {
		if errors.Is(err, ErrNoCommitContext) {
			return nil, nil
		}
		return nil, err
	}
	return &cc, nil
}

func (s *Storage) readLogEntryBoundaries() (first, last *varlogpb.LogSequenceNumber) {
	dit := s.dataDB.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dataKeyPrefix},
		UpperBound: []byte{dataKeySentinelPrefix},
	})
	cit := s.commitDB.NewIter(&pebble.IterOptions{
		LowerBound: []byte{commitKeyPrefix},
		UpperBound: []byte{commitKeySentinelPrefix},
	})
	defer func() {
		_ = dit.Close()
		_ = cit.Close()
	}()

	first = s.getFirstLogSequenceNumber(cit, dit)
	if first == nil {
		return nil, nil
	}

	last = s.getLastLogSequenceNumber(cit, dit, first)
	if last == nil {
		s.logger.Warn("the last must exist but could not be found.", zap.Stringer("first", first))
		return nil, nil
	}
	return first, last
}

func (s *Storage) getFirstLogSequenceNumber(cit, dit *pebble.Iterator) *varlogpb.LogSequenceNumber {
	if !cit.First() || !dit.First() {
		// No committed log entry is found.
		return nil
	}

	cLLSN := decodeDataKey(cit.Value())
	dLLSN := decodeDataKey(dit.Key())
	for cLLSN != dLLSN {
		if dLLSN < cLLSN {
			key := make([]byte, dataKeyLength)
			key = encodeDataKeyInternal(cLLSN, key)
			if !dit.SeekGE(key) {
				// No committed log entry is found.
				return nil
			}
			dLLSN = decodeDataKey(dit.Key())
		} else { // dLLSN > cLLSN
			glsn := decodeCommitKey(cit.Key())
			glsn += types.GLSN(dLLSN - cLLSN)
			key := make([]byte, commitKeyLength)
			key = encodeCommitKeyInternal(glsn, key)
			if !cit.SeekGE(key) {
				// No committed log entry is found.
				return nil
			}
			cLLSN = decodeDataKey(cit.Value())
		}
	}

	firstGLSN := decodeCommitKey(cit.Key())
	return &varlogpb.LogSequenceNumber{
		LLSN: cLLSN,
		GLSN: firstGLSN,
	}
}

func (s *Storage) getLastLogSequenceNumber(cit, dit *pebble.Iterator, first *varlogpb.LogSequenceNumber) *varlogpb.LogSequenceNumber {
	// The last entry must exist since the first exists.
	_ = cit.Last()
	_ = dit.Last()

	cLLSN := decodeDataKey(cit.Value())
	dLLSN := decodeDataKey(dit.Key())

	// If at least one LLSN of data or commit equals the LLSN of the first log
	// entry, it should be the last since there is only one log entry.
	if cLLSN == first.LLSN || dLLSN == first.LLSN {
		return &varlogpb.LogSequenceNumber{
			LLSN: first.LLSN,
			GLSN: first.GLSN,
		}
	}

	for cLLSN != dLLSN {
		if dLLSN < cLLSN {
			glsn := decodeCommitKey(cit.Key())
			glsn = glsn - types.GLSN(cLLSN-dLLSN) + 1
			key := make([]byte, commitKeyLength)
			key = encodeCommitKeyInternal(glsn, key)
			if !cit.SeekLT(key) {
				return nil
			}
			cLLSN = decodeDataKey(cit.Value())
		} else { // dLLSN > cLLSN
			key := make([]byte, dataKeyLength)
			key = encodeDataKeyInternal(cLLSN+1, key)
			if !dit.SeekLT(key) {
				return nil
			}
			dLLSN = decodeDataKey(dit.Key())
		}
	}

	lastGLSN := decodeCommitKey(cit.Key())
	return &varlogpb.LogSequenceNumber{
		LLSN: cLLSN,
		GLSN: lastGLSN,
	}
}

func (s *Storage) readUncommittedLogEntryBoundaries(uncommittedBegin types.LLSN) (begin, end types.LLSN, err error) {
	dk := make([]byte, dataKeyLength)
	dk = encodeDataKeyInternal(uncommittedBegin, dk)
	it := s.dataDB.NewIter(&pebble.IterOptions{
		LowerBound: dk,
		UpperBound: []byte{dataKeySentinelPrefix},
	})
	defer func() {
		_ = it.Close()
	}()

	if !it.First() {
		return types.InvalidLLSN, types.InvalidLLSN, nil
	}

	begin = decodeDataKey(it.Key())
	if begin != uncommittedBegin {
		err = fmt.Errorf("unexpected uncommitted begin, expected %v but got %v", uncommittedBegin, begin)
		return types.InvalidLLSN, types.InvalidLLSN, err
	}
	_ = it.Last()
	end = decodeDataKey(it.Key()) + 1

	return begin, end, nil
}
