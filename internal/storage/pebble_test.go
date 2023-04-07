package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func init() {
	rand.Seed(time.Now().UnixNano())
}

func newPebbleStorage(opts ...Option) (*pebble.DB, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	pebbleOpts := &pebble.Options{
		DisableWAL:                  !cfg.wal,
		L0CompactionThreshold:       cfg.l0CompactionThreshold,
		L0StopWritesThreshold:       cfg.l0StopWritesThreshold,
		LBaseMaxBytes:               cfg.lbaseMaxBytes,
		MaxOpenFiles:                cfg.maxOpenFiles,
		MemTableSize:                cfg.memTableSize,
		MemTableStopWritesThreshold: cfg.memTableStopWritesThreshold,
		MaxConcurrentCompactions:    func() int { return cfg.maxConcurrentCompaction },
		Levels:                      make([]pebble.LevelOptions, 7),
		ErrorIfExists:               false,
	}

	for i := 0; i < len(pebbleOpts.Levels); i++ {
		l := &pebbleOpts.Levels[i]
		if i == 0 {
			l.TargetFileSize = 2 << 20
		}
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = pebbleOpts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	pebbleOpts.Levels[6].FilterPolicy = nil
	pebbleOpts.FlushSplitBytes = pebbleOpts.Levels[0].TargetFileSize
	pebbleOpts.EnsureDefaults()

	if cfg.verbose {
		pebbleOpts.EventListener = pebble.MakeLoggingEventListener(newLogAdaptor(cfg.logger))
		pebbleOpts.EventListener.TableDeleted = nil
		pebbleOpts.EventListener.TableIngested = nil
		pebbleOpts.EventListener.WALCreated = nil
		pebbleOpts.EventListener.WALDeleted = nil
	}

	if cfg.readOnly {
		pebbleOpts.ReadOnly = true
	}

	return pebble.Open(cfg.path, pebbleOpts)
}

func generateRandomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)

	return b
}

func generateRandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestPebbleRandomWrite(t *testing.T) {
	db, err := newPebbleStorage(
		WithoutSync(),
		WithoutWAL(),
		WithMaxConcurrentCompaction(5),
		WithPath("/data1/pebble-test"),
	)
	require.NoError(t, err)
	defer db.Close()

	writeOpts := &pebble.WriteOptions{Sync: false}

	cnt := 1000000
	payload := generateRandomBytes(4096)

	st := time.Now()
	for i := 0; i < cnt; i++ {
		key := generateRandomString(16)

		err = db.Set([]byte(key), payload, writeOpts)
		require.NoError(t, err)
	}

	dur := time.Since(st)
	fmt.Printf("dur: %v, %f ms\n", dur, float64(dur.Nanoseconds()/int64(cnt))/1000000.0)
}

func TestPebbleSequentialWrite(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db, err := newPebbleStorage(
		//WithMemTableSize(64<<20),
		//WithMemTableStopWritesThreshold(4),
		WithoutSync(),
		WithoutWAL(),
		WithVerboseLogging(),
		WithLogger(logger),
		WithMaxConcurrentCompaction(5),
		WithPath("/data1/sequential-test"),
	)
	require.NoError(t, err)
	defer db.Close()

	writeOpts := &pebble.WriteOptions{Sync: false}

	cnt := 500000000
	payload := generateRandomBytes(4096)

	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Printf("%s\n", db.Metrics().String())
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	st := time.Now()
	for i := 0; i < cnt; i++ {
		key := fmt.Sprintf("TEST-%20d", st.UnixNano()+int64(i))

		err = db.Set([]byte(key), payload, writeOpts)
		require.NoError(t, err)

		if i%100000 == 0 {
			dur := time.Since(st)
			fmt.Printf("total write %d, dur: %v, %f ms\n", i, dur, float64(dur.Nanoseconds()/int64(100000))/1000000.0)
			st = time.Now()
		}
	}

	cancel()
	g.Wait()
}

func TestPebbleVarlogWrite(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db, err := newPebbleStorage(
		WithMemTableSize(64<<20),
		WithMemTableStopWritesThreshold(4),
		WithoutSync(),
		//WithVerboseLogging(),
		WithLogger(logger),
		WithMaxConcurrentCompaction(5),
		WithPath("/data1/varlog-test"),
	)
	require.NoError(t, err)
	defer db.Close()

	writeOpts := &pebble.WriteOptions{Sync: false}

	cnt := 100000000
	commitCnt := 50
	payload := generateRandomBytes(4096)
	commitPayload := generateRandomBytes(16)

	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Printf("%s\n", db.Metrics().String())
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	st1 := time.Now()
	st2 := time.Now()
	baseSeq := st1.UnixNano()
	for i := 0; i < cnt/commitCnt; i++ {
		// data
		for j := 0; j < commitCnt; j++ {
			key := fmt.Sprintf("Z-%20d", baseSeq+int64(i*commitCnt)+int64(j))

			err = db.Set([]byte(key), payload, writeOpts)
			require.NoError(t, err)
		}

		// commit
		for j := 0; j < commitCnt; j++ {
			key := fmt.Sprintf("C-%20d", baseSeq+int64(i*commitCnt)+int64(j))

			err = db.Set([]byte(key), commitPayload, writeOpts)
			require.NoError(t, err)
		}

		// commit context
		err = db.Set([]byte("AA"), commitPayload, writeOpts)
		require.NoError(t, err)

		if i%2000 == 0 {
			dur := time.Since(st2)
			fmt.Printf("total write %d, dur: %v, %f ms\n", (i+1)*commitCnt, dur, float64(dur.Nanoseconds()/int64(100000))/(float64(commitCnt)*2000.0))
			st2 = time.Now()
		}
	}

	dur := time.Since(st1)
	fmt.Printf("dur: %v, %f ms\n", dur, float64(dur.Nanoseconds()/int64(cnt))/1000000.0)

	cancel()
	g.Wait()
}

func TestPebbleVarlogWriteSeperate(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	dbData, err := newPebbleStorage(
		WithMemTableSize(64<<20),
		WithMemTableStopWritesThreshold(4),
		WithoutSync(),
		//WithVerboseLogging(),
		WithLogger(logger),
		WithMaxConcurrentCompaction(3),
		WithPath("/data1/varlog-test-data"),
	)
	require.NoError(t, err)
	defer dbData.Close()

	dbCommit, err := newPebbleStorage(
		WithMemTableSize(4<<20),
		WithMemTableStopWritesThreshold(4),
		WithoutSync(),
		//WithVerboseLogging(),
		WithLogger(logger),
		WithMaxConcurrentCompaction(3),
		WithPath("/data1/varlog-test-commit"),
	)
	require.NoError(t, err)
	defer dbCommit.Close()

	writeOpts := &pebble.WriteOptions{Sync: false}

	cnt := 100000000
	commitCnt := 50
	payload := generateRandomBytes(4096)
	commitPayload := generateRandomBytes(16)

	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				fmt.Printf("### pebble metric ###\n")
				fmt.Printf("## data DB ##\n")
				fmt.Printf("%s\n", dbData.Metrics().String())
				fmt.Printf("## commit DB ##\n")
				fmt.Printf("%s\n", dbCommit.Metrics().String())
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	st1 := time.Now()
	st2 := time.Now()
	baseSeq := st1.UnixNano()
	for i := 0; i < cnt/commitCnt; i++ {
		// data
		for j := 0; j < commitCnt; j++ {
			key := fmt.Sprintf("Z-%20d", baseSeq+int64(i*commitCnt)+int64(j))

			err = dbData.Set([]byte(key), payload, writeOpts)
			require.NoError(t, err)
		}

		// commit
		for j := 0; j < commitCnt; j++ {
			key := fmt.Sprintf("C-%20d", baseSeq+int64(i*commitCnt)+int64(j))

			err = dbCommit.Set([]byte(key), commitPayload, writeOpts)
			require.NoError(t, err)
		}

		// commit context
		err = dbCommit.Set([]byte("ZZ"), commitPayload, writeOpts)
		require.NoError(t, err)

		if i%2000 == 0 {
			dur := time.Since(st2)
			fmt.Printf("total write %d, dur: %v, %f ms\n", (i+1)*commitCnt, dur, float64(dur.Nanoseconds()/int64(100000))/(float64(commitCnt)*2000.0))
			st2 = time.Now()
		}
	}

	dur := time.Since(st1)
	fmt.Printf("dur: %v, %f ms\n", dur, float64(dur.Nanoseconds()/int64(cnt))/1000000.0)

	cancel()
	g.Wait()
}

func TestPebbleManualCompaction(t *testing.T) {
	db, err := newPebbleStorage(
		WithoutSync(),
		WithoutWAL(),
		WithVerboseLogging(),
		WithMaxConcurrentCompaction(5),
		WithPath("/data1/sequential-test"),
	)
	require.NoError(t, err)
	defer db.Close()

	start := fmt.Sprintf("TEST-%20d", 0)
	end := fmt.Sprintf("TEST-%20d", time.Now().UnixNano())
	err = db.Compact([]byte(start), []byte(end), true)
	require.NoError(t, err)
}
