package storage

import (
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStorage_New(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(t *testing.T)
	}{
		{
			name: "NoPath",
			testf: func(t *testing.T) {
				cache := NewCache(1 << 20)
				defer cache.Unref()

				_, err := New(
					WithLogger(zap.NewNop()),
					WithCache(cache),
				)
				require.Error(t, err)
			},
		},
		{
			name: "NoLogger",
			testf: func(t *testing.T) {
				path := t.TempDir()
				cache := NewCache(1 << 20)
				defer cache.Unref()

				_, err := New(
					WithPath(path),
					WithLogger(nil),
					WithCache(cache),
				)
				require.Error(t, err)
			},
		},
		{
			name: "NoWALWithSync",
			testf: func(t *testing.T) {
				path := t.TempDir()
				cache := NewCache(1 << 20)
				defer cache.Unref()

				_, err := New(
					WithPath(path),
					WithCache(cache),
					WithDataDBOptions(
						WithWAL(false),
						WithSync(true),
					),
				)
				require.Error(t, err)
			},
		},
		{
			name: "NoCache",
			testf: func(t *testing.T) {
				path := t.TempDir()

				s, err := New(
					WithPath(path),
					WithLogger(zap.NewNop()),
				)
				require.NoError(t, err)
				require.NoError(t, s.Close())
			},
		},
		{
			name: "UseCache",
			testf: func(t *testing.T) {
				path := t.TempDir()
				cache := NewCache(1 << 20)
				defer cache.Unref()

				s, err := New(
					WithPath(path),
					WithLogger(zap.NewNop()),
					WithCache(cache),
					SeparateDatabase(),
				)
				require.NoError(t, err)
				require.NoError(t, s.Close())
			},
		},
		{
			name: "SeparateAndNotSeparate",
			testf: func(t *testing.T) {
				path := t.TempDir()

				s, err := New(
					WithPath(path),
					SeparateDatabase(),
				)
				require.NoError(t, err)
				require.NoError(t, s.Close())

				_, err = New(
					WithPath(path),
				)
				require.Error(t, err)
			},
		},
		{
			name: "NotSeparateAndSeparate",
			testf: func(t *testing.T) {
				path := t.TempDir()

				s, err := New(
					WithPath(path),
				)
				require.NoError(t, err)
				require.NoError(t, s.Close())

				_, err = New(
					WithPath(path),
					SeparateDatabase(),
				)
				require.Error(t, err)
			},
		},
		{
			name: "NewAndCloseSeparateDB",
			testf: func(t *testing.T) {
				path := t.TempDir()
				opts := []Option{
					WithPath(path),
					SeparateDatabase(),
					WithVerboseLogging(),
					WithMetricsLogInterval(time.Second),
				}

				s, err := New(opts...)
				require.NoError(t, err)
				require.NoError(t, s.Close())

				s, err = New(opts...)
				require.NoError(t, err)
				require.NoError(t, s.Close())
			},
		},
		{
			name: "NewAndCloseNotSeparateDB",
			testf: func(t *testing.T) {
				path := t.TempDir()
				opts := []Option{
					WithPath(path),
					WithVerboseLogging(),
					WithMetricsLogInterval(time.Second),
				}

				s, err := New(opts...)
				require.NoError(t, err)
				require.NoError(t, s.Close())

				s, err = New(opts...)
				require.NoError(t, err)
				require.NoError(t, s.Close())
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.testf(t)
		})
	}
}

func TestStorageEncodeDecode(t *testing.T) {
	dk := make([]byte, dataKeyLength)
	dk = encodeDataKeyInternal(types.MaxLLSN, dk)
	assert.Equal(t, types.MaxLLSN, decodeDataKey(dk))

	ck := make([]byte, commitKeyLength)
	ck = encodeCommitKeyInternal(types.MaxGLSN, ck)
	assert.Equal(t, types.MaxGLSN, decodeCommitKey(ck))

	assert.Panics(t, func() {
		_ = decodeCommitKey(dk)
	})
	assert.Panics(t, func() {
		_ = decodeDataKey(ck)
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

func TestStorage_CommitContext(t *testing.T) {
	testStorage(t, func(t testing.TB, stg *Storage) {
		expected := CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		}
		cb, err := stg.NewCommitBatch(expected)
		require.NoError(t, err)
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		actual, err := stg.ReadCommitContext()
		require.NoError(t, err)
		require.Equal(t, expected, actual)

		expected = CommitContext{
			Version:            2,
			HighWatermark:      3,
			CommittedGLSNBegin: 2,
			CommittedGLSNEnd:   4,
			CommittedLLSNBegin: 2,
		}
		cb, err = stg.NewCommitBatch(expected)
		require.NoError(t, err)
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		actual, err = stg.ReadCommitContext()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
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

		scanner, err := stg.NewScanner(WithLLSN(1, 3))
		require.NoError(t, err)
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

		scanner, err := stg.NewScanner(WithLLSN(types.InvalidLLSN, types.MaxLLSN))
		require.NoError(t, err)
		assert.False(t, scanner.Valid())
		_, err = scanner.Value()
		assert.ErrorIs(t, err, io.EOF)
		assert.NoError(t, scanner.Close())
	})
}

func TestStorage_AppendBatch(t *testing.T) {
	cc := CommitContext{
		Version:            1,
		HighWatermark:      2,
		CommittedGLSNBegin: 3,
		CommittedGLSNEnd:   4,
		CommittedLLSNBegin: 5,
	}

	tcs := []struct {
		name  string
		testf func(t testing.TB, stg *Storage)
	}{
		{
			name: "LogEntry",
			testf: func(t testing.TB, stg *Storage) {
				batch := stg.NewAppendBatch()
				require.NoError(t, batch.SetLogEntry(1, 1, []byte("one")))
				require.NoError(t, batch.Apply())
				require.NoError(t, batch.Close())

				entry, err := stg.Read(AtGLSN(1))
				require.NoError(t, err)
				require.Equal(t, types.LLSN(1), entry.LLSN)
				require.Equal(t, types.GLSN(1), entry.GLSN)
				require.Equal(t, []byte("one"), entry.Data)
			},
		},
		{
			name: "CommitContext",
			testf: func(t testing.TB, stg *Storage) {
				batch := stg.NewAppendBatch()
				require.NoError(t, batch.SetCommitContext(cc))
				require.NoError(t, batch.Apply())
				require.NoError(t, batch.Close())

				actual, err := stg.ReadCommitContext()
				require.NoError(t, err)
				require.Equal(t, cc, actual)
			},
		},
		{
			name: "Combined",
			testf: func(t testing.TB, stg *Storage) {
				batch := stg.NewAppendBatch()
				require.NoError(t, batch.SetLogEntry(1, 1, []byte("one")))
				require.NoError(t, batch.SetCommitContext(cc))
				require.NoError(t, batch.Apply())
				require.NoError(t, batch.Close())

				entry, err := stg.Read(AtGLSN(1))
				require.NoError(t, err)
				require.Equal(t, types.LLSN(1), entry.LLSN)
				require.Equal(t, types.GLSN(1), entry.GLSN)
				require.Equal(t, []byte("one"), entry.Data)

				actual, err := stg.ReadCommitContext()
				require.NoError(t, err)
				require.Equal(t, cc, actual)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			testStorage(t, func(t testing.TB, stg *Storage) {
				tc.testf(t, stg)
			})
		})
	}
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

		scanner, err := stg.NewScanner(WithGLSN(11, 13))
		require.NoError(t, err)
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
		scanner, err := stg.NewScanner(WithGLSN(11, 13))
		require.NoError(t, err)
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
		lastCC, err := stg.readLastCommitContext()
		assert.NoError(t, err)
		assert.Nil(t, lastCC)

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

		lastCC, err = stg.readLastCommitContext()
		assert.NoError(t, err)
		// lastCC
		assert.NotNil(t, lastCC)
		assert.True(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)

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

		lastCC, err = stg.readLastCommitContext()
		assert.NoError(t, err)
		// lastCC
		assert.NotNil(t, lastCC)
		assert.True(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)

		// nonempty cc
		expectedLastCC = CommitContext{
			Version:            3,
			HighWatermark:      3,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		}
		cb, err = stg.NewCommitBatch(expectedLastCC)
		assert.NoError(t, err)
		assert.NoError(t, cb.Apply())
		assert.NoError(t, cb.Close())

		lastCC, err = stg.readLastCommitContext()
		assert.NoError(t, err)
		// lastCC
		assert.NotNil(t, lastCC)
		assert.False(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)

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

		lastCC, err = stg.readLastCommitContext()
		assert.NoError(t, err)
		// lastCC
		assert.NotNil(t, lastCC)
		assert.True(t, lastCC.Empty())
		assert.Equal(t, expectedLastCC, *lastCC)
	})
}

func TestStorageReadLogEntryBoundaries(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(t *testing.T, stg *Storage)
	}{
		{
			name: "NoLogEntry",
			testf: func(t *testing.T, stg *Storage) {
				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Nil(t, first)
				require.Nil(t, last)
			},
		},
		{
			name: "NoLogEntryData",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Apply())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Nil(t, first)
				require.Nil(t, last)
			},
		},
		{
			name: "SingleLogEntry",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				want := &varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}
				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, want, first)
				require.Equal(t, want, last)
			},
		},
		{
			name: "TwoLogEntries",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   3,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 1, LLSN: 1}, first)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 2, LLSN: 2}, last)
			},
		},
		{
			// data:   _ 2 3
			// commit: 1 2 3
			name: "NoDataAtFront",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   4,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Set(3, 3))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Set(3, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 2, LLSN: 2}, first)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 3, LLSN: 3}, last)
			},
		},
		{
			// data:   1 2 3
			// commit: _ 2 3
			name: "NoCommitAtFront",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   4,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Set(3, 3))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Set(3, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 2, LLSN: 2}, first)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 3, LLSN: 3}, last)
			},
		},
		{
			// data:   1 2 _
			// commit: 1 2 3
			name: "NoDataAtRear",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   4,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Set(3, 3))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 1, LLSN: 1}, first)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 2, LLSN: 2}, last)
			},
		},
		{
			// data:   1 2 3
			// commit: 1 2 _
			name: "NoCommitAtRear",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   4,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Set(3, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 1, LLSN: 1}, first)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 2, LLSN: 2}, last)
			},
		},
		{
			// data:   _ 2 3
			// commit: 1 2 _
			name: "SingleLogEntryWithAnomaly",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   4,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Set(3, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				want := &varlogpb.LogSequenceNumber{LLSN: 2, GLSN: 2}
				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, want, first)
				require.Equal(t, want, last)
			},
		},
		{
			// data:   1 2 3 _
			// commit: _ 2 3 4
			name: "TwoLogEntriesWithAnomaly",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   5,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Set(3, 3))
				require.NoError(t, cb.Set(4, 4))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Set(3, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 2, LLSN: 2}, first)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 3, LLSN: 3}, last)
			},
		},
		{
			// data:   1 2 _
			// commit: _ _ 3
			name: "NoCommittedLogEntry",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   11,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(3, 3))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Set(2, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Nil(t, first)
				require.Nil(t, last)
			},
		},
		{
			// data:   _ _ 3
			// commit: 1 2 _
			name: "NoLCommittedogEntry",
			testf: func(t *testing.T, stg *Storage) {
				cb, err := stg.NewCommitBatch(CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   11,
					CommittedLLSNBegin: 1,
				})
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Set(2, 2))
				require.NoError(t, cb.Apply())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(3, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				first, last, err := stg.readLogEntryBoundaries()
				require.NoError(t, err)
				require.Nil(t, first)
				require.Nil(t, last)
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			stg := TestNewStorage(t)
			defer func() {
				err := stg.Close()
				require.NoError(t, err)
			}()
			tc.testf(t, stg)
		})
	}
}

func TestStorageReadRecoveryPoints(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(t *testing.T, stg *Storage)
	}{
		{
			name: "NoLogEntry",
			testf: func(t *testing.T, stg *Storage) {
				rp, err := stg.ReadRecoveryPoints()
				require.NoError(t, err)
				require.Nil(t, rp.LastCommitContext)
				require.Nil(t, rp.CommittedLogEntry.First)
				require.Nil(t, rp.CommittedLogEntry.Last)
			},
		},
		{
			name: "EmptyCommitContext",
			testf: func(t *testing.T, stg *Storage) {
				cc := CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   1,
					CommittedLLSNBegin: 1,
				}

				cb, err := stg.NewCommitBatch(cc)
				require.NoError(t, err)
				require.NoError(t, cb.Apply())
				require.NoError(t, cb.Close())

				rp, err := stg.ReadRecoveryPoints()
				require.NoError(t, err)
				require.Equal(t, &cc, rp.LastCommitContext)
				require.Zero(t, rp.CommittedLogEntry)
			},
		},
		{
			name: "NoData",
			testf: func(t *testing.T, stg *Storage) {
				cc := CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				}

				cb, err := stg.NewCommitBatch(cc)
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Apply())
				require.NoError(t, cb.Close())

				rp, err := stg.ReadRecoveryPoints()
				require.NoError(t, err)
				require.Equal(t, &cc, rp.LastCommitContext)
				require.Zero(t, rp.CommittedLogEntry)
			},
		},
		{
			name: "SingleLogEntry",
			testf: func(t *testing.T, stg *Storage) {
				cc := CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				}

				cb, err := stg.NewCommitBatch(cc)
				require.NoError(t, err)
				require.NoError(t, cb.Set(1, 1))
				require.NoError(t, cb.Apply())
				require.NoError(t, cb.Close())

				wb := stg.NewWriteBatch()
				require.NoError(t, wb.Set(1, nil))
				require.NoError(t, wb.Apply())
				require.NoError(t, wb.Close())

				rp, err := stg.ReadRecoveryPoints()
				require.NoError(t, err)
				require.Equal(t, &cc, rp.LastCommitContext)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 1, LLSN: 1}, rp.CommittedLogEntry.First)
				require.Equal(t, &varlogpb.LogSequenceNumber{GLSN: 1, LLSN: 1}, rp.CommittedLogEntry.Last)
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			stg := TestNewStorage(t)
			defer func() {
				err := stg.Close()
				require.NoError(t, err)
			}()
			tc.testf(t, stg)
		})
	}
}

func TestStorage_TrimWhenNoLogEntry(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(t testing.TB, stg *Storage)
	}{
		{
			name: "MinGLSN",
			testf: func(t testing.TB, stg *Storage) {
				err := stg.Trim(types.MinGLSN)
				assert.ErrorIs(t, err, ErrNoLogEntry)
			},
		},
		{
			name: "MaxGLSN",
			testf: func(t testing.TB, stg *Storage) {
				err := stg.Trim(types.MaxGLSN)
				assert.ErrorIs(t, err, ErrNoLogEntry)
			},
		},
		{
			name: "InvalidGLSN",
			testf: func(t testing.TB, stg *Storage) {
				err := stg.Trim(types.InvalidGLSN)
				assert.ErrorIs(t, err, ErrNoLogEntry)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			testStorage(t, func(t testing.TB, stg *Storage) {
				tc.testf(t, stg)
			})
		})
	}
}

func TestStorage_Trim(t *testing.T) {
	expectedCC := CommitContext{
		Version:            1,
		HighWatermark:      3,
		CommittedGLSNBegin: 1,
		CommittedGLSNEnd:   4,
		CommittedLLSNBegin: 1,
	}

	tcs := []struct {
		name  string
		testf func(t testing.TB, stg *Storage)
	}{
		{
			name: "MinGLSN",
			testf: func(t testing.TB, stg *Storage) {
				err := stg.Trim(types.MinGLSN)
				assert.NoError(t, err)

				it, err := stg.dataDB.NewIter(&pebble.IterOptions{
					LowerBound: []byte{dataKeyPrefix},
					UpperBound: []byte{dataKeySentinelPrefix},
				})
				require.NoError(t, err)
				assert.True(t, it.First())
				assert.Equal(t, types.LLSN(2), decodeDataKey(it.Key()))
				assert.True(t, it.Next())
				assert.Equal(t, types.LLSN(3), decodeDataKey(it.Key()))
				assert.NoError(t, it.Close())

				it, err = stg.commitDB.NewIter(&pebble.IterOptions{
					LowerBound: []byte{commitKeyPrefix},
					UpperBound: []byte{commitKeySentinelPrefix},
				})
				require.NoError(t, err)
				assert.True(t, it.First())
				assert.Equal(t, types.GLSN(2), decodeCommitKey(it.Key()))
				assert.True(t, it.Next())
				assert.Equal(t, types.GLSN(3), decodeCommitKey(it.Key()))
				assert.NoError(t, it.Close())

				cc, err := stg.ReadCommitContext()
				assert.NoError(t, err)
				assert.Equal(t, expectedCC, cc)
			},
		},
		{
			name: "LastCommittedGLSN",
			testf: func(t testing.TB, stg *Storage) {
				err := stg.Trim(3)
				assert.NoError(t, err)
				assert.NoError(t, err)

				it, err := stg.dataDB.NewIter(&pebble.IterOptions{
					LowerBound: []byte{dataKeyPrefix},
					UpperBound: []byte{dataKeySentinelPrefix},
				})
				require.NoError(t, err)
				assert.False(t, it.First())
				assert.NoError(t, it.Close())

				it, err = stg.commitDB.NewIter(&pebble.IterOptions{
					LowerBound: []byte{commitKeyPrefix},
					UpperBound: []byte{commitKeySentinelPrefix},
				})
				require.NoError(t, err)
				assert.False(t, it.First())
				assert.NoError(t, it.Close())

				cc, err := stg.ReadCommitContext()
				assert.NoError(t, err)
				assert.Equal(t, expectedCC, cc)
			},
		},
		{
			name: "MaxGLSN",
			testf: func(t testing.TB, stg *Storage) {
				err := stg.Trim(types.MaxGLSN)
				assert.NoError(t, err)

				it, err := stg.dataDB.NewIter(&pebble.IterOptions{
					LowerBound: []byte{dataKeyPrefix},
					UpperBound: []byte{dataKeySentinelPrefix},
				})
				require.NoError(t, err)
				assert.False(t, it.First())
				assert.NoError(t, it.Close())

				it, err = stg.commitDB.NewIter(&pebble.IterOptions{
					LowerBound: []byte{commitKeyPrefix},
					UpperBound: []byte{commitKeySentinelPrefix},
				})
				require.NoError(t, err)
				assert.False(t, it.First())
				assert.NoError(t, it.Close())

				cc, err := stg.ReadCommitContext()
				assert.NoError(t, err)
				assert.Equal(t, expectedCC, cc)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			testStorage(t, func(t testing.TB, stg *Storage) {
				// CC  : +---+
				// LLSN: 1 2 3
				// GLSN: 1 2 3
				wb := stg.NewWriteBatch()
				assert.NoError(t, wb.Set(1, nil))
				assert.NoError(t, wb.Set(2, nil))
				assert.NoError(t, wb.Set(3, nil))
				assert.NoError(t, wb.Apply())
				assert.NoError(t, wb.Close())

				cb, err := stg.NewCommitBatch(expectedCC)
				assert.NoError(t, err)
				assert.NoError(t, cb.Set(1, 1))
				assert.NoError(t, cb.Set(2, 2))
				assert.NoError(t, cb.Set(3, 3))
				assert.NoError(t, cb.Apply())
				assert.NoError(t, cb.Close())

				tc.testf(t, stg)
			})
		})
	}
}
