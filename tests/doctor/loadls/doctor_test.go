package loadls

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/types"
)

func TestLoadLogStream(t *testing.T) {
	lspath := os.Getenv("lspath") // cid_1_snid_2_tpid_3_lsid_4
	if len(lspath) == 0 {
		t.Skip("lspath is not set")
	}
	logger := zaptest.NewLogger(t)
	t.Cleanup(func() {
		_ = logger.Sync()
	})
	stg, err := storage.New(
		storage.WithLogger(logger),
		storage.SeparateDatabase(),
		storage.WithPath(lspath),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := stg.Close()
		require.NoError(t, err)
	})

	cc, err := stg.ReadCommitContext()
	require.NoError(t, err)
	t.Logf("commit context: %+v", cc)

	rp, err := stg.ReadRecoveryPoints()
	require.NoError(t, err)
	t.Logf("recovery points: %+v", rp)
	t.Logf("last commit context: %+v", *rp.LastCommitContext)

	dataDB, commitDB := storage.TestGetUnderlyingDB(t, stg)

	dataIter, err := dataDB.NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(0x40)},
		UpperBound: []byte{byte(0x41)},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = dataIter.Close()
	})

	ok := dataIter.First()
	require.True(t, ok)
	dk := dataIter.Key()
	firstLLSN := types.LLSN(binary.BigEndian.Uint64(dk[1:]))
	t.Logf("[data db] first LLSN: %d", firstLLSN)

	clear(dk)
	ok = dataIter.Last()
	require.True(t, ok)
	dk = dataIter.Key()
	lastLLSN := types.LLSN(binary.BigEndian.Uint64(dk[1:]))
	t.Logf("[data db] last LLSN: %d", lastLLSN)

	commitIter, err := commitDB.NewIter(&pebble.IterOptions{
		LowerBound: []byte{byte(0x80)},
		UpperBound: []byte{byte(0x81)},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = commitIter.Close()
	})

	ok = commitIter.First()
	require.True(t, ok)
	ck := commitIter.Key()
	firstGLSN := types.GLSN(binary.BigEndian.Uint64(ck[1:]))
	dk = commitIter.Value()
	firstLLSN = types.LLSN(binary.BigEndian.Uint64(dk[1:]))
	t.Logf("[commit db] first LLSN: %d, GLSN: %d", firstLLSN, firstGLSN)

	clear(ck)
	ok = commitIter.Last()
	require.True(t, ok)
	ck = commitIter.Key()
	lastGLSN := types.GLSN(binary.BigEndian.Uint64(ck[1:]))
	dk = commitIter.Value()
	lastLLSN = types.LLSN(binary.BigEndian.Uint64(dk[1:]))
	t.Logf("[commit db] last LLSN: %d, GLSN: %d", lastLLSN, lastGLSN)

	// iter.Last()
	// iter.Key()
}
