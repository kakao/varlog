package storage

import (
	"errors"

	"github.com/cockroachdb/pebble"

	"github.com/kakao/varlog/proto/varlogpb"
)

type RecoveryPoints struct {
	LastCommitContext *CommitContext
	CommittedLogEntry struct {
		First *varlogpb.LogEntryMeta
		Last  *varlogpb.LogEntryMeta
	}
}

// ReadRecoveryPoints reads data necessary to restore the status of a log
// stream replica - the first and last log entries and commit context.
// Incompatible between the boundary of log entries and commit context is okay;
// thus, it returns nil as err.
// However, if there is a fatal error, such as missing data in a log entry, it
// returns an error.
func (s *Storage) ReadRecoveryPoints() (rp RecoveryPoints, err error) {
	rp.LastCommitContext, err = s.readLastCommitContext()
	if err != nil {
		return
	}
	rp.CommittedLogEntry.First, rp.CommittedLogEntry.Last, err = s.readLogEntryBoundaries()
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

func (s *Storage) readLogEntryBoundaries() (first, last *varlogpb.LogEntryMeta, err error) {
	it := s.commitDB.NewIter(&pebble.IterOptions{
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
