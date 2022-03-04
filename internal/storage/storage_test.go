package storage

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestStorage_InvalidConfig(t *testing.T) {
	defer goleak.VerifyNone(t)

	_, err := New(WithLogger(zap.NewNop()))
	assert.Error(t, err)

	_, err = New(WithPath(t.TempDir()), WithLogger(nil))
	assert.Error(t, err)

	_, err = New(WithPath(t.TempDir()), WithoutWAL())
	assert.Error(t, err)
}

func TestStorageEncodeDecode(t *testing.T) {
	dk := make([]byte, dataKeyLength)
	dk = encodeDataKeyInternal(types.MaxLLSN, dk)
	assert.Equal(t, types.MaxLLSN, decodeDataKey(dk))

	ck := make([]byte, commitKeyLength)
	ck = encodeCommitKeyInternal(types.MaxGLSN, ck)
	assert.Equal(t, types.MaxGLSN, decodeCommitKey(ck))

	cck := make([]byte, commitContextKeyLength)
	cck = encodeCommitContextKeyInternal(CommitContext{
		Version:            types.MaxVersion,
		HighWatermark:      types.MaxGLSN - 1,
		CommittedGLSNBegin: types.MinGLSN,
		CommittedGLSNEnd:   types.MaxGLSN,
		CommittedLLSNBegin: types.MinLLSN,
	}, cck)
	assert.Equal(t, CommitContext{
		Version:            types.MaxVersion,
		HighWatermark:      types.MaxGLSN - 1,
		CommittedGLSNBegin: types.MinGLSN,
		CommittedGLSNEnd:   types.MaxGLSN,
		CommittedLLSNBegin: types.MinLLSN,
	}, decodeCommitContextKey(cck))

	assert.Panics(t, func() {
		_ = decodeCommitKey(dk)
	})
	assert.Panics(t, func() {
		_ = decodeCommitContextKey(dk)
	})
	assert.Panics(t, func() {
		_ = decodeDataKey(ck)
	})
	assert.Panics(t, func() {
		_ = decodeCommitContextKey(ck)
	})
	assert.Panics(t, func() {
		_ = decodeDataKey(cck)
	})
	assert.Panics(t, func() {
		_ = decodeCommitKey(cck)
	})
}

func TestStorageCommitContext(t *testing.T) {
	cc1 := CommitContext{
		Version:            1,
		HighWatermark:      1,
		CommittedGLSNBegin: 1,
		CommittedGLSNEnd:   2,
		CommittedLLSNBegin: 1,
	}
	assert.False(t, cc1.Empty())
	assert.True(t, cc1.Equal(cc1))

	cc2 := CommitContext{
		Version:            1,
		HighWatermark:      1,
		CommittedGLSNBegin: 1,
		CommittedGLSNEnd:   1,
		CommittedLLSNBegin: 1,
	}
	assert.True(t, cc2.Empty())
	assert.True(t, cc2.Equal(cc2))

	assert.False(t, cc1.Equal(cc2))
}

func testStorage(t *testing.T, f func(testing.TB, *Storage)) {
	defer goleak.VerifyNone(t)
	stg := TestNewStorage(t)
	defer func() {
		err := stg.Close()
		assert.NoError(t, err)
	}()
	f(t, stg)
}

func TestStorage(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		assert.NotEmpty(t, stg.Path())
	})
}

func TestStorage_WriteBatch(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Set(1, []byte("1")))
		assert.NoError(t, wb.Set(2, []byte("2")))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		// It could not read log entries since there are no commits.
		_, err := stg.Read(AtLLSN(1))
		assert.Error(t, err)
		_, err = stg.Read(AtLLSN(2))
		assert.Error(t, err)

		scanner := stg.NewScanner(WithLLSN(1, 3))
		assert.True(t, scanner.Valid())
		le, err := scanner.Value()
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 1,
			},
			Data: []byte("1"),
		}, le)
		assert.True(t, scanner.Next())
		le, err = scanner.Value()
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 2,
			},
			Data: []byte("2"),
		}, le)
		assert.False(t, scanner.Next())
		_, err = scanner.Value()
		assert.ErrorIs(t, err, io.EOF)
		assert.False(t, scanner.Next())
		_, err = scanner.Value()
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, scanner.Close())
	})
}

func TestStorage_EmptyWriteBatch(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		scanner := stg.NewScanner(WithLLSN(types.InvalidLLSN, types.MaxLLSN))
		assert.False(t, scanner.Valid())
		_, err := scanner.Value()
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, scanner.Close())
	})
}

func TestStorage_InconsistentWriteCommit(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      12,
			CommittedGLSNBegin: 11,
			CommittedGLSNEnd:   13,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(1, 11))
		assert.NoError(t, cb.Set(2, 12))
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		scanner := stg.NewScanner(WithGLSN(11, 13))
		assert.True(t, scanner.Valid())
		_, err = scanner.Value()
		assert.Error(t, err)
		assert.NotErrorIs(t, err, io.EOF)
		assert.NoError(t, scanner.Close())
	})
}

func TestStorage_WriteCommit(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Set(1, []byte("1")))
		assert.NoError(t, wb.Set(2, []byte("2")))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      12,
			CommittedGLSNBegin: 11,
			CommittedGLSNEnd:   13,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(1, 11))
		assert.NoError(t, cb.Set(2, 12))
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		// read by LLSN
		le, err := stg.Read(AtLLSN(1))
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 1,
				GLSN: 11,
			},
			Data: []byte("1"),
		}, le)
		le, err = stg.Read(AtLLSN(2))
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 2,
				GLSN: 12,
			},
			Data: []byte("2"),
		}, le)

		// read by GLSN
		le, err = stg.Read(AtGLSN(11))
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 1,
				GLSN: 11,
			},
			Data: []byte("1"),
		}, le)
		le, err = stg.Read(AtGLSN(12))
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 2,
				GLSN: 12,
			},
			Data: []byte("2"),
		}, le)

		// scan
		scanner := stg.NewScanner(WithGLSN(11, 13))
		assert.True(t, scanner.Valid())
		le, err = scanner.Value()
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 1,
				GLSN: 11,
			},
			Data: []byte("1"),
		}, le)
		assert.True(t, scanner.Next())
		assert.True(t, scanner.Valid())
		le, err = scanner.Value()
		assert.NoError(t, err)
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: 2,
				GLSN: 12,
			},
			Data: []byte("2"),
		}, le)

		assert.False(t, scanner.Next())
		assert.False(t, scanner.Valid())
		_, err = scanner.Value()
		assert.ErrorIs(t, err, io.EOF)
		assert.False(t, scanner.Next())
		assert.False(t, scanner.Valid())
		_, err = scanner.Value()
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, scanner.Close())
	})
}

func TestStorageRead(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		// no logs
		_, err := stg.Read(AtGLSN(0))
		assert.Error(t, err)
		_, err = stg.Read(AtGLSN(1))
		assert.Error(t, err)

		_, err = stg.Read(AtLLSN(0))
		assert.Error(t, err)
		_, err = stg.Read(AtLLSN(1))
		assert.Error(t, err)

		// log with nil data
		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      11,
			CommittedGLSNBegin: 11,
			CommittedGLSNEnd:   12,
			CommittedLLSNBegin: 11,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(11, 11))
		assert.NoError(t, cb.Apply())

		// no data corresponded to the commit
		_, err = stg.Read(AtGLSN(11))
		assert.Error(t, err)

		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Set(11, nil))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		le, err := stg.Read(AtGLSN(11))
		assert.NoError(t, err)
		// FIXME(jun): LogEntryMeta has unnecessary fields?
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 11, LLSN: 11,
			},
			Data: nil,
		}, le)

		// log
		cb, err = stg.NewCommitBatch(CommitContext{
			Version:            2,
			HighWatermark:      12,
			CommittedGLSNBegin: 12,
			CommittedGLSNEnd:   13,
			CommittedLLSNBegin: 12,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(12, 12))
		assert.NoError(t, cb.Apply())

		wb = stg.NewWriteBatch()
		assert.NoError(t, wb.Set(12, []byte("foo")))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		le, err = stg.Read(AtGLSN(12))
		assert.NoError(t, err)
		// FIXME(jun): LogEntryMeta has unnecessary fields?
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 12, LLSN: 12,
			},
			Data: []byte("foo"),
		}, le)
	})
}

func TestStorageReadLastCommitContext(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		// no cc
		lastCC, lastNonemptyCC := stg.readLastCommitContext()
		assert.Nil(t, lastCC)
		assert.Nil(t, lastNonemptyCC)

		// empty cc
		expectedLastCC := CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   1,
			CommittedLLSNBegin: 1,
		}
		cb, err := stg.NewCommitBatch(expectedLastCC)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		lastCC, lastNonemptyCC = stg.readLastCommitContext()
		// lastCC
		assert.NotNil(t, lastCC)
		assert.True(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)
		// lastNonemptyCC
		assert.Nil(t, lastNonemptyCC)

		// empty cc
		expectedLastCC = CommitContext{
			Version:            2,
			HighWatermark:      2,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   1,
			CommittedLLSNBegin: 1,
		}
		cb, err = stg.NewCommitBatch(expectedLastCC)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		lastCC, lastNonemptyCC = stg.readLastCommitContext()
		// lastCC
		assert.NotNil(t, lastCC)
		assert.True(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)
		// lastNonemptyCC
		assert.Nil(t, lastNonemptyCC)

		// nonempty cc
		expectedLastCC = CommitContext{
			Version:            3,
			HighWatermark:      3,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		}
		expectedLastNonempyCC := expectedLastCC
		cb, err = stg.NewCommitBatch(expectedLastCC)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		lastCC, lastNonemptyCC = stg.readLastCommitContext()
		// lastCC
		assert.NotNil(t, lastCC)
		assert.False(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)
		// lastNonemptyCC
		assert.NotNil(t, lastNonemptyCC)
		assert.False(t, lastNonemptyCC.Empty())
		assert.Equal(t, expectedLastNonempyCC, *lastNonemptyCC)

		// empty cc
		expectedLastCC = CommitContext{
			Version:            4,
			HighWatermark:      4,
			CommittedGLSNBegin: 2,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 2,
		}
		cb, err = stg.NewCommitBatch(expectedLastCC)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		lastCC, lastNonemptyCC = stg.readLastCommitContext()
		// lastCC
		assert.NotNil(t, lastCC)
		assert.True(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)
		// lastNonemptyCC
		assert.NotNil(t, lastNonemptyCC)
		assert.False(t, lastNonemptyCC.Empty())
		assert.Equal(t, expectedLastNonempyCC, *lastNonemptyCC)
	})
}

func TestStorageReadLogEntryBoundaries(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		// no logs
		first, last, err := stg.readLogEntryBoundaries()
		assert.NoError(t, err)
		assert.Nil(t, first)
		assert.Nil(t, last)

		// single log
		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(1, 1))
		assert.NoError(t, cb.Apply())

		// no data corresponded to the commit
		_, _, err = stg.readLogEntryBoundaries()
		assert.Error(t, err)

		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Set(1, nil))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		first, last, err = stg.readLogEntryBoundaries()
		assert.NoError(t, err)
		// FIXME(jun): LogEntryMeta has unnecessary fields?
		assert.Equal(t, &varlogpb.LogEntryMeta{GLSN: 1, LLSN: 1}, first)
		assert.Equal(t, &varlogpb.LogEntryMeta{GLSN: 1, LLSN: 1}, last)

		// two logs
		cb, err = stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 2,
			CommittedGLSNEnd:   3,
			CommittedLLSNBegin: 2,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(2, 2))
		assert.NoError(t, cb.Apply())

		// no data corresponded to the commit
		_, _, err = stg.readLogEntryBoundaries()
		assert.Error(t, err)

		wb = stg.NewWriteBatch()
		assert.NoError(t, wb.Set(2, nil))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		first, last, err = stg.readLogEntryBoundaries()
		assert.NoError(t, err)
		// FIXME(jun): LogEntryMeta has unnecessary fields?
		assert.Equal(t, &varlogpb.LogEntryMeta{GLSN: 1, LLSN: 1}, first)
		assert.Equal(t, &varlogpb.LogEntryMeta{GLSN: 2, LLSN: 2}, last)
	})
}

func TestStorageReadRecoveryPoints(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		rp, err := stg.ReadRecoveryPoints()
		assert.NoError(t, err)
		assert.Nil(t, rp.LastCommitContext)
		assert.Nil(t, rp.CommittedLogEntry.First)
		assert.Nil(t, rp.CommittedLogEntry.Last)

		// empty cc
		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   1,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		rp, err = stg.ReadRecoveryPoints()
		assert.NoError(t, err)
		assert.Equal(t, &CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   1,
			CommittedLLSNBegin: 1,
		}, rp.LastCommitContext)
		assert.Nil(t, rp.CommittedLogEntry.First)
		assert.Nil(t, rp.CommittedLogEntry.Last)

		// nonempty cc
		cb, err = stg.NewCommitBatch(CommitContext{
			Version:            2,
			HighWatermark:      2,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(1, 1))
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		// no data corresponded to the commit
		_, err = stg.ReadRecoveryPoints()
		assert.Error(t, err)

		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Set(1, nil))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		rp, err = stg.ReadRecoveryPoints()
		assert.NoError(t, err)
		assert.Equal(t, &CommitContext{
			Version:            2,
			HighWatermark:      2,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		}, rp.LastCommitContext)
		assert.Equal(t, &varlogpb.LogEntryMeta{GLSN: 1, LLSN: 1}, rp.CommittedLogEntry.First)
		assert.Equal(t, &varlogpb.LogEntryMeta{GLSN: 1, LLSN: 1}, rp.CommittedLogEntry.Last)
	})
}

func TestStorageReadRecoveryPoints_InconsistentWriteCommit(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		ck := make([]byte, commitKeyLength)
		dk := make([]byte, dataKeyLength)

		// committed log: llsn = 1, glsn = 1
		err := stg.db.Set(encodeCommitKeyInternal(1, ck), encodeDataKeyInternal(1, dk), pebble.Sync)
		assert.NoError(t, err)

		err = stg.db.Set(encodeDataKeyInternal(1, dk), nil, pebble.Sync)
		assert.NoError(t, err)

		// A committed log exists, but no commit context for that.
		_, err = stg.ReadRecoveryPoints()
		assert.Error(t, err)

		// nonempty cc: llsn = 1, glsn = 2
		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 2,
			CommittedGLSNEnd:   3,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		// Committed log and commit context exist, but they are inconsistent.
		_, err = stg.ReadRecoveryPoints()
		assert.Error(t, err)
	})
}

func TestStorage_CommitContextOf(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		cc1 := CommitContext{
			Version:            1,
			HighWatermark:      10,
			CommittedGLSNBegin: 8,
			CommittedGLSNEnd:   11,
			CommittedLLSNBegin: 1,
		}
		cc2 := CommitContext{
			Version:            2,
			HighWatermark:      22,
			CommittedGLSNBegin: 21,
			CommittedGLSNEnd:   23,
			CommittedLLSNBegin: 4,
		}
		cb, err := stg.NewCommitBatch(cc1)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		cb, err = stg.NewCommitBatch(cc2)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		for glsn := types.GLSN(0); glsn < cc1.CommittedGLSNBegin; glsn++ {
			_, err := stg.CommitContextOf(glsn)
			assert.ErrorIs(t, err, ErrNoCommitContext)
		}
		for glsn := cc1.CommittedGLSNBegin; glsn < cc1.CommittedGLSNEnd; glsn++ {
			cc, err := stg.CommitContextOf(glsn)
			assert.NoError(t, err)
			assert.Equal(t, cc1, cc)

			cc, err = stg.NextCommitContextOf(cc)
			assert.NoError(t, err)
			assert.Equal(t, cc2, cc)
		}
		for glsn := cc1.CommittedGLSNEnd; glsn < cc2.CommittedGLSNBegin; glsn++ {
			_, err := stg.CommitContextOf(glsn)
			assert.ErrorIs(t, err, ErrNoCommitContext)
		}
		for glsn := cc2.CommittedGLSNBegin; glsn < cc2.CommittedGLSNEnd; glsn++ {
			cc, err := stg.CommitContextOf(glsn)
			assert.NoError(t, err)
			assert.Equal(t, cc2, cc)

			_, err = stg.NextCommitContextOf(cc)
			assert.ErrorIs(t, err, ErrNoCommitContext)
		}

	})
}

func TestStorage_Trim(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		err := stg.Trim(1)
		assert.ErrorIs(t, err, ErrNoLogEntry)

		// CC  : +-1-+
		// LLSN: 1 2 3
		// GLSN: 1 2 3
		wb := stg.NewWriteBatch()
		assert.NoError(t, wb.Set(1, nil))
		assert.NoError(t, wb.Set(2, nil))
		assert.NoError(t, wb.Set(3, nil))
		assert.NoError(t, wb.Apply())
		assert.NoError(t, wb.Close())

		cb, err := stg.NewCommitBatch(CommitContext{
			Version:            1,
			HighWatermark:      5,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   4,
			CommittedLLSNBegin: 1,
		})
		assert.NoError(t, err)
		assert.NoError(t, cb.Set(1, 1))
		assert.NoError(t, cb.Set(2, 2))
		assert.NoError(t, cb.Set(3, 3))
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		// CC  : +-1-+
		// LLSN: _ 2 3
		// GLSN: _ 2 3
		err = stg.Trim(1)
		assert.NoError(t, err)
		it := stg.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{dataKeyPrefix},
			UpperBound: []byte{dataKeySentinelPrefix},
		})
		assert.True(t, it.First())
		assert.Equal(t, types.LLSN(2), decodeDataKey(it.Key()))
		assert.True(t, it.Next())
		assert.Equal(t, types.LLSN(3), decodeDataKey(it.Key()))
		assert.NoError(t, it.Close())
		it = stg.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{commitKeyPrefix},
			UpperBound: []byte{commitKeySentinelPrefix},
		})
		assert.True(t, it.First())
		assert.Equal(t, types.GLSN(2), decodeCommitKey(it.Key()))
		assert.True(t, it.Next())
		assert.Equal(t, types.GLSN(3), decodeCommitKey(it.Key()))
		assert.NoError(t, it.Close())
		it = stg.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{commitContextKeyPrefix},
			UpperBound: []byte{commitContextKeySentinelPrefix},
		})
		assert.True(t, it.First())
		assert.Equal(t, CommitContext{
			Version:            1,
			HighWatermark:      5,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   4,
			CommittedLLSNBegin: 1,
		}, decodeCommitContextKey(it.Key()))
		assert.NoError(t, it.Close())

		// CC  : +-_-+
		// LLSN: _ _ _
		// GLSN: _ _ _
		err = stg.Trim(3)
		assert.NoError(t, err)
		it = stg.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{dataKeyPrefix},
			UpperBound: []byte{dataKeySentinelPrefix},
		})
		assert.False(t, it.First())
		assert.NoError(t, it.Close())
		it = stg.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{commitKeyPrefix},
			UpperBound: []byte{commitKeySentinelPrefix},
		})
		assert.False(t, it.First())
		assert.NoError(t, it.Close())
		it = stg.db.NewIter(&pebble.IterOptions{
			LowerBound: []byte{commitContextKeyPrefix},
			UpperBound: []byte{commitContextKeySentinelPrefix},
		})
		assert.False(t, it.First())
		assert.NoError(t, it.Close())

		for glsn := types.MinGLSN; glsn < types.GLSN(5); glsn++ {
			err = stg.Trim(glsn)
			assert.ErrorIs(t, err, ErrNoLogEntry)
		}
	})
}
