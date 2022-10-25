package storage

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestNewStorage(tb testing.TB, opts ...Option) *Storage {
	defaultOpts := []Option{
		WithPath(tb.TempDir()),
		WithoutSync(), // FIXME: Use only in mac since sync is too slow in mac os.
	}
	s, err := New(append(defaultOpts, opts...)...)
	assert.NoError(tb, err)
	return s
}

// TestGetUnderlyingDB returns a pebble that is an internal database in the
// storage.
func TestGetUnderlyingDB(tb testing.TB, stg *Storage) *pebble.DB {
	require.NotNil(tb, stg)
	require.NotNil(tb, stg.db)
	return stg.db
}

// TestAppendLogEntryWithoutCommitContext stores log entries without commit
// context.
func TestAppendLogEntryWithoutCommitContext(tb testing.TB, stg *Storage, llsn types.LLSN, glsn types.GLSN, data []byte) {
	batch := stg.NewAppendBatch()
	require.NoError(tb, batch.SetLogEntry(llsn, glsn, data))
	require.NoError(tb, batch.Apply())
	require.NoError(tb, batch.Close())
}

// TestSetCommitContext stores only commit context.
func TestSetCommitContext(tb testing.TB, stg *Storage, cc CommitContext) {
	batch := stg.NewAppendBatch()
	require.NoError(tb, batch.SetCommitContext(cc))
	require.NoError(tb, batch.Apply())
	require.NoError(tb, batch.Close())
}

func TestDeleteCommitContext(tb testing.TB, stg *Storage) {
	err := stg.db.Delete(commitContextKey, pebble.Sync)
	require.NoError(tb, err)
}

func TestDeleteLogEntry(tb testing.TB, stg *Storage, lsn varlogpb.LogSequenceNumber) {
	batch := stg.db.NewBatch()
	defer func() {
		err := batch.Close()
		require.NoError(tb, err)
	}()

	dk := make([]byte, dataKeyLength)
	err := batch.Delete(encodeDataKeyInternal(lsn.LLSN, dk), nil)
	require.NoError(tb, err)

	ck := make([]byte, commitKeyLength)
	err = batch.Delete(encodeCommitKeyInternal(lsn.GLSN, ck), nil)
	require.NoError(tb, err)

	err = batch.Commit(pebble.Sync)
	require.NoError(tb, err)
}
