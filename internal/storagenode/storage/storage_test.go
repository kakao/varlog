package storage

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
)

type testStorage struct {
	name   string
	getter func() Storage
}

func newTestStorage(t *testing.T) (tss []testStorage) {
	tss = append(tss, testStorage{name: "pebble"})
	for i := range tss {
		tss[i].getter = func() Storage {
			strg, err := NewStorage(
				WithName(tss[i].name),
				WithPath(t.TempDir()), WithLogger(zap.L()),
			)
			require.NoError(t, err)
			return strg
		}
	}
	return tss
}

func testEachStorage(t *testing.T, testFunc func(t *testing.T, strg Storage)) {
	tss := newTestStorage(t)
	for i := range tss {
		ts := tss[i]
		t.Run(ts.name, func(t *testing.T) {
			strg := ts.getter()
			testFunc(t, strg)
		})
	}
}

func TestStorageWrite(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		numWriteBatch    = 10
		maxWriteBatchLen = 100
		storagePrevLLSN  = 0
	)

	tcs := []struct {
		initLLSN int
		delta    int
		data     []byte
		ok       bool
	}{
		{
			initLLSN: storagePrevLLSN + 1,
			delta:    1,
			data:     []byte("foo"),
			ok:       true,
		},
		{
			// data is nil
			initLLSN: storagePrevLLSN + 1,
			delta:    1,
			data:     nil,
			ok:       true,
		},
		{
			// unmatch with prevLLSN
			initLLSN: storagePrevLLSN + 2,
			delta:    1,
			data:     []byte("foo"),
		},
	}

	rand.Seed(time.Now().Unix())

	tss := newTestStorage(t)

	for i := range tcs {
		tc := tcs[i]
		for j := range tss {
			ts := tss[j]
			t.Run(ts.name, func(t *testing.T) {
				strg := ts.getter()
				defer func() {
					require.NoError(t, strg.Close())
				}()

				require.Positive(t, len(strg.Path()))

				llsn := tc.initLLSN
				for i := 0; i < numWriteBatch; i++ {
					batchLen := rand.Intn(maxWriteBatchLen) + 1
					wb := strg.NewWriteBatch()
					for j := 0; j < batchLen; j++ {
						err := wb.Put(types.LLSN(llsn), tc.data)
						llsn += tc.delta
						if tc.ok {
							require.NoError(t, err)
						} else {
							require.Error(t, err)
						}
					}
					require.NoError(t, wb.Apply())
					require.NoError(t, wb.Close())
				}
			})
		}
	}
}

func TestStorageBadWrite(t *testing.T) {
	tss := newTestStorage(t)
	for i := range tss {
		ts := tss[i]
		t.Run(ts.name, func(t *testing.T) {
			strg := ts.getter()
			defer func() {
				require.NoError(t, strg.Close())
			}()

			var wb WriteBatch

			wb = strg.NewWriteBatch()
			require.NoError(t, wb.Put(1, nil))
			require.NoError(t, wb.Apply())
			require.NoError(t, wb.Close())

			wb = strg.NewWriteBatch()
			require.NoError(t, wb.Put(2, nil))
			require.NoError(t, wb.Apply())
			require.NoError(t, wb.Close())

			wb = strg.NewWriteBatch()
			require.Error(t, wb.Put(2, nil))
			require.NoError(t, wb.Close())

			wb = strg.NewWriteBatch()
			require.Error(t, wb.Put(4, nil))
			require.NoError(t, wb.Close())

			wb = strg.NewWriteBatch()
			require.Error(t, wb.Put(1, nil))
			require.NoError(t, wb.Close())

			wb = strg.NewWriteBatch()
			require.NoError(t, wb.Put(3, nil))
			require.NoError(t, wb.Apply())
			require.NoError(t, wb.Close())
		})
	}
}

func TestStorageInterleavedWrite(t *testing.T) {
	tss := newTestStorage(t)
	for i := range tss {
		ts := tss[i]
		t.Run(ts.name, func(t *testing.T) {
			strg := ts.getter()
			defer func() {
				require.NoError(t, strg.Close())
			}()

			wb1 := strg.NewWriteBatch()
			require.NoError(t, wb1.Put(1, nil))
			require.NoError(t, wb1.Put(2, nil))

			wb2 := strg.NewWriteBatch()
			require.NoError(t, wb2.Put(1, nil))
			require.NoError(t, wb2.Put(2, nil))
			require.NoError(t, wb2.Apply())
			require.NoError(t, wb2.Close())

			require.NoError(t, wb1.Put(3, nil))
			require.NoError(t, wb1.Put(4, nil))
			require.Error(t, wb1.Apply())
			require.NoError(t, wb1.Close())
		})
	}
}

func TestStorageWriteCommitReadScanDelete(t *testing.T) {
	tss := newTestStorage(t)
	for i := range tss {
		ts := tss[i]
		t.Run(ts.name, func(t *testing.T) {
			strg := ts.getter()
			defer func() {
				require.NoError(t, strg.Close())
			}()

			var (
				err error
				le  types.LogEntry
				wb  WriteBatch
				cb  CommitBatch
				cc  CommitContext
				sc  Scanner
				sr  ScanResult
			)

			require.Error(t, strg.DeleteCommitted(0))
			require.Error(t, strg.DeleteUncommitted(0))

			// Write
			// LLSN: 1, 2, 3, 4
			wb = strg.NewWriteBatch()
			require.NoError(t, wb.Put(1, nil))
			require.NoError(t, wb.Put(2, []byte("foo")))
			require.NoError(t, wb.Put(3, []byte("bar")))
			require.NoError(t, wb.Put(4, []byte{}))
			require.NoError(t, wb.Apply())
			require.NoError(t, wb.Close())

			_, err = strg.Read(0)
			require.Error(t, err)
			_, err = strg.Read(1)
			require.Error(t, err)

			// Commit
			// Invalid commit context
			cc = CommitContext{
				HighWatermark:      1,
				PrevHighWatermark:  0,
				CommittedGLSNBegin: 2,
				CommittedGLSNEnd:   1,
			}
			_, err = strg.NewCommitBatch(cc)
			require.Error(t, err)

			// Invalid commit: good CC, but no entries
			cb, err = strg.NewCommitBatch(CommitContext{
				HighWatermark:      3,
				PrevHighWatermark:  0,
				CommittedGLSNBegin: 2,
				CommittedGLSNEnd:   4,
			})
			require.NoError(t, err)
			require.Error(t, cb.Apply())
			require.NoError(t, cb.Close())

			// (LLSN,GLSN): (1,2), (2,3)
			cc = CommitContext{
				HighWatermark:      3,
				PrevHighWatermark:  0,
				CommittedGLSNBegin: 2,
				CommittedGLSNEnd:   4,
			}
			cb, err = strg.NewCommitBatch(cc)
			require.NoError(t, err)
			require.Error(t, cb.Put(0, 1))   // invalid LLSN
			require.Error(t, cb.Put(1, 0))   // invalid GLSN
			require.Error(t, cb.Put(2, 1))   // not sequential LLSN
			require.Error(t, cb.Put(1, 1))   // not in commit context
			require.Error(t, cb.Put(1, 4))   // not in commit context
			require.NoError(t, cb.Put(1, 2)) // ok
			require.Error(t, cb.Put(2, 4))   // invalid glsn
			require.NoError(t, cb.Put(2, 3)) // ok
			require.Error(t, cb.Put(3, 4))   // beyond commit context
			require.NoError(t, cb.Apply())
			require.NoError(t, cb.Close())

			_, err = strg.Read(1) // no entry
			require.Error(t, err)

			le, err = strg.Read(2) // ok
			require.NoError(t, err)
			require.Equal(t, types.LLSN(1), le.LLSN)
			require.Nil(t, le.Data)

			le, err = strg.Read(3) // ok
			require.NoError(t, err)
			require.Equal(t, types.LLSN(2), le.LLSN)
			require.Equal(t, []byte("foo"), le.Data)

			// Commit
			// (LLSN,GLSN): (3,6), (4,7)
			cc = CommitContext{
				HighWatermark:      8,
				PrevHighWatermark:  3,
				CommittedGLSNBegin: 6,
				CommittedGLSNEnd:   8,
			}
			cb, err = strg.NewCommitBatch(cc)
			require.NoError(t, err)
			require.Error(t, cb.Put(1, 1))   // already committed LLSN
			require.Error(t, cb.Put(2, 3))   // already committed LLSN
			require.Error(t, cb.Put(3, 3))   // already committed GLSN
			require.Error(t, cb.Put(3, 5))   // not in commit context
			require.Error(t, cb.Put(3, 8))   // not in commit context
			require.NoError(t, cb.Put(3, 6)) // ok
			require.NoError(t, cb.Put(4, 7)) // ok
			require.NoError(t, cb.Apply())
			require.NoError(t, cb.Close())

			// Commit (invalid commit context) overlapped with previous committed range
			cc = CommitContext{
				HighWatermark:      9,
				PrevHighWatermark:  8,
				CommittedGLSNBegin: 7,
				CommittedGLSNEnd:   8,
			}
			_, err = strg.NewCommitBatch(cc)
			require.Error(t, err)

			// Commit (not written log)
			cc = CommitContext{
				HighWatermark:      9,
				PrevHighWatermark:  8,
				CommittedGLSNBegin: 9,
				CommittedGLSNEnd:   10,
			}
			cb, err = strg.NewCommitBatch(cc)
			require.NoError(t, err) // invalid commit context (unwritten logs), but cant know
			require.Error(t, cb.Put(5, 9))
			require.NoError(t, cb.Close())

			// Read
			le, err = strg.Read(6)
			require.NoError(t, err)
			require.Equal(t, types.LLSN(3), le.LLSN)
			require.Equal(t, []byte("bar"), le.Data)

			le, err = strg.Read(7)
			require.NoError(t, err)
			require.Equal(t, types.LLSN(4), le.LLSN)
			require.Nil(t, le.Data)

			// Scan
			sc = strg.Scan(0, 8)
			sc.Close()

			sc = strg.Scan(2, 8)
			sr = sc.Next()
			require.True(t, sr.Valid())
			require.Equal(t, types.LogEntry{
				GLSN: 2,
				LLSN: 1,
				Data: nil,
			}, sr.LogEntry)

			sr = sc.Next()
			require.True(t, sr.Valid())
			require.Equal(t, types.LogEntry{
				GLSN: 3,
				LLSN: 2,
				Data: []byte("foo"),
			}, sr.LogEntry)

			sr = sc.Next()
			require.True(t, sr.Valid())
			require.Equal(t, types.LogEntry{
				GLSN: 6,
				LLSN: 3,
				Data: []byte("bar"),
			}, sr.LogEntry)

			sr = sc.Next()
			require.True(t, sr.Valid())
			require.Equal(t, types.LogEntry{
				GLSN: 7,
				LLSN: 4,
				Data: nil,
			}, sr.LogEntry)

			sr = sc.Next()
			require.False(t, sr.Valid())

			sc.Close()

			// Write
			// LLSN: 5, 6
			wb = strg.NewWriteBatch()
			require.NoError(t, wb.Put(5, []byte("hello")))
			require.NoError(t, wb.Put(6, []byte("world")))
			require.NoError(t, wb.Apply())
			require.NoError(t, wb.Close())

			// DeleteCommitted (Prefix Trim)
			// invalid range
			require.Error(t, strg.DeleteCommitted(0))
			require.Error(t, strg.DeleteCommitted(9))

			// DeleteCommittedGLSN: [1, 4)
			require.NoError(t, strg.DeleteCommitted(4))
			_, err = strg.Read(2)
			require.Error(t, err)
			_, err = strg.Read(3)
			require.Error(t, err)

			sc = strg.Scan(0, 5)
			require.False(t, sc.Next().Valid())
			require.NoError(t, sc.Close())

			sc = strg.Scan(0, 7)
			sr = sc.Next()
			require.True(t, sr.Valid())
			require.Equal(t, types.LogEntry{
				GLSN: 6,
				LLSN: 3,
				Data: []byte("bar"),
			}, sr.LogEntry)
			sr = sc.Next()
			require.False(t, sr.Valid())
			require.NoError(t, sc.Close())

			// delete again
			require.NoError(t, strg.DeleteCommitted(5))
			require.NoError(t, strg.DeleteCommitted(4))
			require.NoError(t, strg.DeleteCommitted(2))

			// DeleteUncommitted
			require.Error(t, strg.DeleteUncommitted(0))   // invalid LLSN
			require.Error(t, strg.DeleteUncommitted(1))   // already committed
			require.Error(t, strg.DeleteUncommitted(4))   // already committed
			require.NoError(t, strg.DeleteUncommitted(5)) // ok
			require.NoError(t, strg.DeleteUncommitted(7)) // ok, already deleted
		})
	}
}

func TestStorageInterleavedCommit(t *testing.T) {
	tss := newTestStorage(t)
	for i := range tss {
		ts := tss[i]
		t.Run(ts.name, func(t *testing.T) {
			strg := ts.getter()
			defer func() {
				require.NoError(t, strg.Close())
			}()

			var (
				err error
				wb  WriteBatch
				cb1 CommitBatch
				cb2 CommitBatch
			)

			// LLSN: 1, 2, 3, 4
			wb = strg.NewWriteBatch()
			require.NoError(t, wb.Put(1, nil))
			require.NoError(t, wb.Put(2, nil))
			require.NoError(t, wb.Put(3, nil))
			require.NoError(t, wb.Put(4, nil))
			require.NoError(t, wb.Apply())
			require.NoError(t, wb.Close())

			cc1 := CommitContext{
				PrevHighWatermark:  0,
				HighWatermark:      4,
				CommittedGLSNBegin: 1,
				CommittedGLSNEnd:   5,
			}
			cb1, err = strg.NewCommitBatch(cc1)
			require.NoError(t, err)
			require.NoError(t, cb1.Put(1, 1))
			require.NoError(t, cb1.Put(2, 2))

			cc2 := CommitContext{
				PrevHighWatermark:  0,
				HighWatermark:      5,
				CommittedGLSNBegin: 3,
				CommittedGLSNEnd:   6,
			}
			cb2, err = strg.NewCommitBatch(cc2)
			require.NoError(t, err)
			require.NoError(t, cb2.Put(1, 3))
			require.NoError(t, cb2.Put(2, 4))
			require.NoError(t, cb2.Put(3, 5))
			require.NoError(t, cb2.Apply())
			require.NoError(t, cb2.Close())

			require.NoError(t, cb1.Put(3, 3))
			require.NoError(t, cb1.Put(4, 4))
			require.Error(t, cb1.Apply())
			require.NoError(t, cb1.Close())
		})
	}
}

func TestStorageReadRecoveryInfoEmptyStorage(t *testing.T) {
	testEachStorage(t, func(t *testing.T, strg Storage) {
		ri, err := strg.ReadRecoveryInfo()
		require.NoError(t, err)
		require.False(t, ri.LastCommitContext.Found)
		require.False(t, ri.LastNonEmptyCommitContext.Found)
		require.False(t, ri.LogEntryBoundary.Found)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.First)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.Last)

		require.NoError(t, strg.Close())
	})
}

func TestStorageReadRecoveryInfoOnlyEmptyCommitContext(t *testing.T) {
	testEachStorage(t, func(t *testing.T, strg Storage) {
		// empty cc, hwm=1
		cb, err := strg.NewCommitBatch(CommitContext{
			HighWatermark:      1,
			PrevHighWatermark:  0,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   1,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		// recovery info: only empty cc
		ri, err := strg.ReadRecoveryInfo()
		require.NoError(t, err)
		require.True(t, ri.LastCommitContext.Found)
		require.False(t, ri.LastNonEmptyCommitContext.Found)
		require.False(t, ri.LogEntryBoundary.Found)
		require.Equal(t, types.GLSN(1), ri.LastCommitContext.CC.HighWatermark)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.First)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.Last)

		// empty cc, hwm=2
		cb, err = strg.NewCommitBatch(CommitContext{
			HighWatermark:      2,
			PrevHighWatermark:  1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   1,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		// recovery info: only empty cc
		ri, err = strg.ReadRecoveryInfo()
		require.NoError(t, err)
		require.True(t, ri.LastCommitContext.Found)
		require.False(t, ri.LastNonEmptyCommitContext.Found)
		require.False(t, ri.LogEntryBoundary.Found)
		require.Equal(t, types.GLSN(2), ri.LastCommitContext.CC.HighWatermark)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.First)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.Last)

		require.NoError(t, strg.Close())
	})
}

func TestStorageReadRecoveryInfoNonEmptyCommitContext(t *testing.T) {
	testEachStorage(t, func(t *testing.T, strg Storage) {
		// cc, hwm=5, le=(1,3), (2,4)
		wb := strg.NewWriteBatch()
		require.NoError(t, wb.Put(1, nil))
		require.NoError(t, wb.Put(2, nil))
		require.NoError(t, wb.Apply())
		require.NoError(t, wb.Close())
		cb, err := strg.NewCommitBatch(CommitContext{
			HighWatermark:      5,
			PrevHighWatermark:  0,
			CommittedGLSNBegin: 3,
			CommittedGLSNEnd:   5,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Put(1, 3))
		require.NoError(t, cb.Put(2, 4))
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		// recovery info: last cc = non-empty and le
		ri, err := strg.ReadRecoveryInfo()
		require.NoError(t, err)
		require.True(t, ri.LastCommitContext.Found)
		require.True(t, ri.LastNonEmptyCommitContext.Found)
		require.True(t, ri.LogEntryBoundary.Found)
		require.Equal(t, types.GLSN(5), ri.LastCommitContext.CC.HighWatermark) // global hwm
		require.Equal(t, types.GLSN(4), ri.LogEntryBoundary.Last.GLSN)         // local hwm
		require.Equal(t, types.GLSN(3), ri.LogEntryBoundary.First.GLSN)        // local lwm
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.First)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.Last)

		require.NoError(t, strg.Close())
	})
}

func TestStorageReadRecoveryInfoMixed(t *testing.T) {
	testEachStorage(t, func(t *testing.T, strg Storage) {
		wb := strg.NewWriteBatch()
		require.NoError(t, wb.Put(1, nil))
		require.NoError(t, wb.Put(2, nil))
		require.NoError(t, wb.Apply())
		require.NoError(t, wb.Close())
		cb, err := strg.NewCommitBatch(CommitContext{
			HighWatermark:      5,
			PrevHighWatermark:  0,
			CommittedGLSNBegin: 3,
			CommittedGLSNEnd:   5,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Put(1, 3))
		require.NoError(t, cb.Put(2, 4))
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		// empty cc, hwm=6
		cb, err = strg.NewCommitBatch(CommitContext{
			HighWatermark:      6,
			PrevHighWatermark:  5,
			CommittedGLSNBegin: 5,
			CommittedGLSNEnd:   5,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		// empty cc, hwm=7
		cb, err = strg.NewCommitBatch(CommitContext{
			HighWatermark:      7,
			PrevHighWatermark:  6,
			CommittedGLSNBegin: 6, // or 5? TODO: clarify it
			CommittedGLSNEnd:   6, // or 5? TODO: clarify it
		})
		require.NoError(t, err)
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		// recovery info: last cc = empty and le
		ri, err := strg.ReadRecoveryInfo()
		require.NoError(t, err)
		require.True(t, ri.LastCommitContext.Found)
		require.True(t, ri.LastNonEmptyCommitContext.Found)
		require.True(t, ri.LogEntryBoundary.Found)
		require.Equal(t, types.GLSN(7), ri.LastCommitContext.CC.HighWatermark) // global hwm
		require.Equal(t, types.GLSN(4), ri.LogEntryBoundary.Last.GLSN)         // local hwm
		require.Equal(t, types.GLSN(3), ri.LogEntryBoundary.First.GLSN)        // local lwm
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.First)
		require.Equal(t, types.InvalidLLSN, ri.UncommittedLogEntryBoundary.Last)

		require.NoError(t, strg.Close())
	})
}

func TestStorageRecoveryInfoUncommitted(t *testing.T) {
	testEachStorage(t, func(t *testing.T, strg Storage) {
		wb := strg.NewWriteBatch()
		require.NoError(t, wb.Put(1, nil))
		require.NoError(t, wb.Put(2, nil))
		require.NoError(t, wb.Put(3, nil))
		require.NoError(t, wb.Put(4, nil))
		require.NoError(t, wb.Apply())
		require.NoError(t, wb.Close())

		cb, err := strg.NewCommitBatch(CommitContext{
			HighWatermark:      5,
			PrevHighWatermark:  0,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   3,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Put(1, 1))
		require.NoError(t, cb.Put(2, 2))
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		ri, err := strg.ReadRecoveryInfo()
		require.NoError(t, err)
		require.True(t, ri.LastCommitContext.Found)
		require.True(t, ri.LastNonEmptyCommitContext.Found)
		require.True(t, ri.LogEntryBoundary.Found)
		require.Equal(t, types.GLSN(5), ri.LastCommitContext.CC.HighWatermark) // global hwm
		require.Equal(t, types.GLSN(2), ri.LogEntryBoundary.Last.GLSN)         // local hwm
		require.Equal(t, types.GLSN(1), ri.LogEntryBoundary.First.GLSN)        // local lwm
		require.Equal(t, types.LLSN(3), ri.UncommittedLogEntryBoundary.First)
		require.Equal(t, types.LLSN(4), ri.UncommittedLogEntryBoundary.Last)

		require.NoError(t, strg.Close())
	})
}

func TestStorageReadCommitContext(t *testing.T) {
	testEachStorage(t, func(t *testing.T, strg Storage) {
		_, err := strg.ReadCommitContext(1)
		require.ErrorIs(t, ErrNotFoundCommitContext, err)

		// LLSN | GLSN  | HWM | PrevHWM
		//    1 |    5  |   6 |       0
		//    2 |    6  |   6 |       0
		//    3 |    9  |  10 |       6
		//    4 |    10 |  10 |       6
		wb := strg.NewWriteBatch()
		require.NoError(t, wb.Put(1, nil))
		require.NoError(t, wb.Put(2, nil))
		require.NoError(t, wb.Put(3, nil))
		require.NoError(t, wb.Put(4, nil))
		require.NoError(t, wb.Apply())
		require.NoError(t, wb.Close())

		cb, err := strg.NewCommitBatch(CommitContext{
			HighWatermark:      6,
			PrevHighWatermark:  0,
			CommittedGLSNBegin: 5,
			CommittedGLSNEnd:   7,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Put(1, 5))
		require.NoError(t, cb.Put(2, 6))
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		cb, err = strg.NewCommitBatch(CommitContext{
			HighWatermark:      10,
			PrevHighWatermark:  6,
			CommittedGLSNBegin: 9,
			CommittedGLSNEnd:   11,
		})
		require.NoError(t, err)
		require.NoError(t, cb.Put(3, 9))
		require.NoError(t, cb.Put(4, 10))
		require.NoError(t, cb.Apply())
		require.NoError(t, cb.Close())

		_, err = strg.ReadCommitContext(0)
		require.NoError(t, err)

		_, err = strg.ReadCommitContext(4)
		require.ErrorIs(t, ErrInconsistentCommitContext, err)

		_, err = strg.ReadCommitContext(5)
		require.ErrorIs(t, ErrInconsistentCommitContext, err)

		_, err = strg.ReadCommitContext(6)
		require.NoError(t, err)

		_, err = strg.ReadCommitContext(7)
		require.ErrorIs(t, ErrInconsistentCommitContext, err)

		_, err = strg.ReadCommitContext(10)
		require.ErrorIs(t, ErrNotFoundCommitContext, err)

		_, err = strg.ReadCommitContext(11)
		require.ErrorIs(t, ErrNotFoundCommitContext, err)

		require.NoError(t, strg.Close())
	})
}
