package storage

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestNewStorage(tb testing.TB, opts ...Option) *Storage {
	cache := NewCache(1 << 20)
	defer cache.Unref()
	defaultOpts := []Option{
		WithPath(tb.TempDir()),
		WithoutSync(), // FIXME: Use only in mac since sync is too slow in mac os.
		WithCache(cache),
	}
	s, err := New(append(defaultOpts, opts...)...)
	assert.NoError(tb, err)
	return s
}

// TestGetUnderlyingDB returns a pebble that is an internal database in the
// storage.
func TestGetUnderlyingDB(tb testing.TB, stg *Storage) (dataDB, commitDB *pebble.DB) {
	require.NotNil(tb, stg)
	require.NotNil(tb, stg.dataDB)
	require.NotNil(tb, stg.commitDB)
	return stg.dataDB, stg.commitDB
}

// TestWriteLogEntry stores data located by the llsn. The data is not committed
// because it does not store commits.
func TestWriteLogEntry(tb testing.TB, stg *Storage, llsn types.LLSN, data []byte) {
	batch := stg.NewWriteBatch()
	require.NoError(tb, batch.Set(llsn, data))
	require.NoError(tb, batch.Apply())
	require.NoError(tb, batch.Close())
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
	err := stg.commitDB.Delete(commitContextKey, pebble.Sync)
	require.NoError(tb, err)
}

func TestDeleteLogEntry(tb testing.TB, stg *Storage, lsn varlogpb.LogSequenceNumber) {
	dataBatch := stg.dataDB.NewBatch()
	commitBatch := stg.commitDB.NewBatch()
	defer func() {
		err := errors.Join(dataBatch.Close(), commitBatch.Close())
		require.NoError(tb, err)
	}()

	dk := make([]byte, dataKeyLength)
	err := dataBatch.Delete(encodeDataKeyInternal(lsn.LLSN, dk), nil)
	require.NoError(tb, err)

	ck := make([]byte, commitKeyLength)
	err = commitBatch.Delete(encodeCommitKeyInternal(lsn.GLSN, ck), nil)
	require.NoError(tb, err)

	err = errors.Join(dataBatch.Commit(pebble.Sync), commitBatch.Commit(pebble.Sync))
	require.NoError(tb, err)
}
