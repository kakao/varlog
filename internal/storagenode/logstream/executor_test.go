package logstream

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/batchlet"
	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestExecutor_InvalidConfig(t *testing.T) {
	stg := storage.TestNewStorage(t)
	defer func() {
		assert.NoError(t, stg.Close())
	}()

	_, err := NewExecutor(WithSequenceQueueCapacity(minQueueCapacity-1), WithStorage(stg))
	assert.Error(t, err)

	_, err = NewExecutor(WithWriteQueueCapacity(minQueueCapacity-1), WithStorage(stg))
	assert.Error(t, err)

	_, err = NewExecutor(WithCommitQueueCapacity(minQueueCapacity-1), WithStorage(stg))
	assert.Error(t, err)

	_, err = NewExecutor(WithReplicateClientQueueCapacity(minQueueCapacity-1), WithStorage(stg))
	assert.Error(t, err)

	_, err = NewExecutor(WithStorage(nil))
	assert.Error(t, err)

	_, err = NewExecutor(WithLogger(nil), WithStorage(stg))
	assert.Error(t, err)
}

func TestExecutor_Closed(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(2)
		lsid = types.LogStreamID(3)
	)

	lse := testNewExecutor(t,
		[]varlogpb.LogStreamReplica{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     tpid,
					LogStreamID: lsid,
				},
			},
		},
		WithStorageNodeID(snid),
		WithTopicID(tpid),
		WithLogStreamID(lsid),
		WithSequenceQueueCapacity(0),
		WithWriteQueueCapacity(0),
		WithCommitQueueCapacity(0),
	)
	assert.NoError(t, lse.Close())
	assert.Equal(t, executorStateClosed, lse.esm.load())

	_, err := lse.Append(context.Background(), TestNewBatchData(t, 1, 0))
	assert.ErrorIs(t, err, verrors.ErrClosed)

	err = lse.Replicate(context.Background(), []types.LLSN{1}, TestNewBatchData(t, 1, 0))
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, _, err = lse.Seal(context.Background(), types.MinGLSN)
	assert.ErrorIs(t, err, verrors.ErrClosed)

	err = lse.Unseal(context.Background(), []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid + 1,
				Address:       "addr",
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		},
	})
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.Report(context.Background())
	assert.ErrorIs(t, err, verrors.ErrClosed)

	err = lse.Commit(context.Background(), snpb.LogStreamCommitResult{})
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.Metadata()
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.LogStreamMetadata()
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.SubscribeWithGLSN(types.MinGLSN, types.MinGLSN)
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.SubscribeWithLLSN(types.MinLLSN, types.MinLLSN)
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{FirstLLSN: 1, LastLLSN: 1})
	assert.ErrorIs(t, err, verrors.ErrClosed)

	err = lse.SyncReplicate(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		},
	})
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.Sync(context.Background(), varlogpb.LogStreamReplica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid + 1,
			Address:       "addr",
		},
		TopicLogStream: varlogpb.TopicLogStream{
			TopicID:     tpid,
			LogStreamID: lsid,
		},
	})
	assert.ErrorIs(t, err, verrors.ErrClosed)

	err = lse.Trim(context.Background(), 1)
	assert.ErrorIs(t, err, verrors.ErrClosed)
}

func TestExecutor_Sealing(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(2)
		lsid = types.LogStreamID(3)
	)

	lse := testNewExecutor(t,
		[]varlogpb.LogStreamReplica{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     tpid,
					LogStreamID: lsid,
				},
			},
		},
		WithStorageNodeID(snid),
		WithTopicID(tpid),
		WithLogStreamID(lsid),
		WithSequenceQueueCapacity(0),
		WithWriteQueueCapacity(0),
		WithCommitQueueCapacity(0),
	)
	defer func() {
		err := lse.Close()
		assert.NoError(t, err)
	}()

	status, localHWM, err := lse.Seal(context.Background(), types.MaxGLSN)
	assert.NoError(t, err)
	assert.Equal(t, types.InvalidGLSN, localHWM)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, status)
	assert.Equal(t, executorStateSealing, lse.esm.load())

	_, err = lse.Append(context.Background(), TestNewBatchData(t, 1, 0))
	assert.ErrorIs(t, err, verrors.ErrSealed)

	err = lse.Replicate(context.Background(), []types.LLSN{1}, TestNewBatchData(t, 1, 0))
	assert.ErrorIs(t, err, verrors.ErrSealed)
}

func TestExecutor_Sealed(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(2)
		lsid = types.LogStreamID(3)
	)

	lse := testNewExecutor(t,
		[]varlogpb.LogStreamReplica{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     tpid,
					LogStreamID: lsid,
				},
			},
		},
		WithStorageNodeID(snid),
		WithTopicID(tpid),
		WithLogStreamID(lsid),
		WithSequenceQueueCapacity(0),
		WithWriteQueueCapacity(0),
		WithCommitQueueCapacity(0),
	)
	defer func() {
		err := lse.Close()
		assert.NoError(t, err)
	}()

	status, localHWM, err := lse.Seal(context.Background(), types.InvalidGLSN)
	assert.NoError(t, err)
	assert.Equal(t, types.InvalidGLSN, localHWM)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)
	assert.Equal(t, executorStateSealed, lse.esm.load())

	_, err = lse.Append(context.Background(), TestNewBatchData(t, 1, 0))
	assert.ErrorIs(t, err, verrors.ErrSealed)

	err = lse.Replicate(context.Background(), []types.LLSN{1}, TestNewBatchData(t, 1, 0))
	assert.ErrorIs(t, err, verrors.ErrSealed)
}

func testUnsealInitialExecutor(t *testing.T, lse *Executor, replicas []varlogpb.LogStreamReplica, lastGLSN types.GLSN) {
	lsmd, err := lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, lsmd.Status)

	_, err = lse.Append(context.Background(), [][]byte{[]byte("hello")})
	assert.Error(t, err)

	status, localHWM, err := lse.Seal(context.Background(), lastGLSN)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)
	assert.Equal(t, lastGLSN, localHWM)

	err = lse.Unseal(context.Background(), replicas)
	assert.NoError(t, err)

	lsmd, err = lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lsmd.Status)
}

func testNewExecutor(t *testing.T, replicas []varlogpb.LogStreamReplica, executorOpts ...ExecutorOption) *Executor {
	stg := storage.TestNewStorage(t)

	lse, err := NewExecutor(append(executorOpts, WithStorage(stg))...)
	assert.NoError(t, err)

	testUnsealInitialExecutor(t, lse, replicas, types.InvalidGLSN)
	return lse
}

func testRespawnExecutor(t *testing.T, old *Executor, path string, lastGLSN types.GLSN) *Executor {
	stg := storage.TestNewStorage(t, storage.WithPath(path))
	lse, err := NewExecutor(
		WithStorageNodeID(old.snid),
		WithTopicID(old.tpid),
		WithLogStreamID(old.lsid),
		WithSequenceQueueCapacity(0),
		WithWriteQueueCapacity(0),
		WithCommitQueueCapacity(0),
		WithStorage(stg),
	)
	assert.NoError(t, err)

	testUnsealInitialExecutor(t, lse, old.primaryBackups, lastGLSN)
	return lse
}

func testNewPrimaryExecutor(t *testing.T, opts ...ExecutorOption) *Executor {
	const (
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(2)
		lsid = types.LogStreamID(3)
	)
	replicas := []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		},
	}
	executorOpts := []ExecutorOption{
		WithStorageNodeID(snid),
		WithTopicID(tpid),
		WithLogStreamID(lsid),
		WithSequenceQueueCapacity(0),
		WithWriteQueueCapacity(0),
		WithCommitQueueCapacity(0),
	}
	executorOpts = append(executorOpts, opts...)
	lse := testNewExecutor(t,
		replicas,
		executorOpts...,
	)
	return lse
}

func testNewBackupExecutor(t *testing.T, opts ...ExecutorOption) *Executor {
	const (
		snid          = types.StorageNodeID(2)
		tpid          = types.TopicID(2)
		lsid          = types.LogStreamID(3)
		queueCapacity = 1024
	)
	replicas := []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid - 1,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		},
	}
	executorOpts := []ExecutorOption{
		WithStorageNodeID(snid),
		WithTopicID(tpid),
		WithLogStreamID(lsid),
		WithSequenceQueueCapacity(queueCapacity),
		WithWriteQueueCapacity(queueCapacity),
		WithCommitQueueCapacity(queueCapacity),
	}
	executorOpts = append(executorOpts, opts...)
	lse := testNewExecutor(t,
		replicas,
		executorOpts...,
	)
	return lse
}

func TestExecutor_ShouldBeSealedAtFirst(t *testing.T) {
	testCases := []struct {
		name      string
		generator func(t *testing.T) *Executor
	}{
		{
			name: "primary",
			generator: func(t *testing.T) *Executor {
				return testNewPrimaryExecutor(t)
			},
		},
		{
			name: "backup",
			generator: func(t *testing.T) *Executor {
				return testNewBackupExecutor(t)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lse := tc.generator(t)
			err := lse.Close()
			assert.NoError(t, err)
		})
	}
}

func TestExecutor_Append(t *testing.T) {
	testCases := []struct {
		name      string
		generator func(t *testing.T) *Executor
		isErr     bool
	}{
		{
			name: "primary",
			generator: func(t *testing.T) *Executor {
				return testNewPrimaryExecutor(t)
			},
			isErr: false,
		},
		{
			name: "backup",
			generator: func(t *testing.T) *Executor {
				return testNewBackupExecutor(t)
			},
			isErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lse := tc.generator(t)
			defer func() {
				err := lse.Close()
				assert.NoError(t, err)
			}()

			// backup
			if tc.isErr {
				_, err := lse.Append(context.Background(), [][]byte{nil})
				assert.Error(t, err)
				return
			}

			// primary
			batchLens := make([]int, 0, len(batchlet.LengthClasses)*3+1)
			batchLens = append(batchLens, 1)
			for _, batchletLen := range batchlet.LengthClasses {
				batchLens = append(batchLens, batchletLen-1)
				batchLens = append(batchLens, batchletLen)
				batchLens = append(batchLens, batchletLen+1)
			}

			var wg sync.WaitGroup
			for i := 0; i < len(batchLens); i++ {
				batchLen := batchLens[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					batch := TestNewBatchData(t, batchLen, 0)
					_, err := lse.Append(context.Background(), batch)
					assert.NoError(t, err)
				}()
			}

			var (
				lastLLSN    = types.InvalidLLSN
				lastGLSN    = types.InvalidGLSN
				lastVersion = types.InvalidVersion
			)

			for i := 0; i < len(batchLens); i++ {
				batchLen := uint64(batchLens[i])
				assert.Eventually(t, func() bool {
					_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
						TopicID:             lse.tpid,
						LogStreamID:         lse.lsid,
						CommittedLLSNOffset: lastLLSN + 1,
						CommittedGLSNOffset: lastGLSN + 1,
						CommittedGLSNLength: batchLen,
						Version:             lastVersion + 1,
						HighWatermark:       lastGLSN + types.GLSN(batchLen),
					})

					rpt, err := lse.Report(context.Background())
					assert.NoError(t, err)
					if rpt.Version != lastVersion+1 {
						return false
					}

					lastVersion++
					lastLLSN += types.LLSN(batchLen)
					lastGLSN += types.GLSN(batchLen)
					return true
				}, time.Second, 10*time.Millisecond)
			}
			wg.Wait()

			// FIXME: Use lse.Report to check local high watermark and low watermark
			version, globalHWM, uncommittedLLSNBegin := lse.lsc.reportCommitBase()
			assert.Equal(t, lastVersion, version)
			assert.Equal(t, lastGLSN, globalHWM)
			assert.Equal(t, lastLLSN+1, uncommittedLLSNBegin)

			// FIXME: Fields TopicID and LogStreamID in varlogpb.LogEntryMeta should be filled.
			assert.Equal(t, varlogpb.LogEntryMeta{
				LLSN: types.MinLLSN,
				GLSN: types.MinGLSN,
			}, lse.lsc.localLowWatermark())
			assert.Equal(t, varlogpb.LogEntryMeta{
				LLSN: lastLLSN,
				GLSN: lastGLSN,
			}, lse.lsc.localHighWatermark())

			// simple subscribe
			sr, err := lse.SubscribeWithGLSN(types.MinGLSN, lastGLSN+1)
			assert.NoError(t, err)
			expectedLLSN, expectedGLSN := types.MinLLSN, types.MinGLSN
			for le := range sr.Result() {
				assert.Equal(t, expectedLLSN, le.LLSN)
				assert.Equal(t, expectedGLSN, le.GLSN)
				expectedLLSN++
				expectedGLSN++
			}
			sr.Stop()
			assert.NoError(t, sr.Err())

			// simple subscribe
			sr, err = lse.SubscribeWithLLSN(types.MinLLSN, lastLLSN+1)
			assert.NoError(t, err)
			expectedLLSN = types.MinLLSN
			for le := range sr.Result() {
				assert.Equal(t, expectedLLSN, le.LLSN)
				expectedLLSN++
			}
			sr.Stop()
			assert.NoError(t, sr.Err())
		})
	}
}

func TestExecutor_Replicate(t *testing.T) {
	testCases := []struct {
		name      string
		generator func(t *testing.T) *Executor
		isErr     bool
	}{
		{
			name: "primary",
			generator: func(t *testing.T) *Executor {
				return testNewPrimaryExecutor(t)
			},
			isErr: true,
		},
		{
			name: "backup",
			generator: func(t *testing.T) *Executor {
				return testNewBackupExecutor(t)
			},
			isErr: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lse := tc.generator(t)
			defer func() {
				err := lse.Close()
				assert.NoError(t, err)
			}()

			// primary
			if tc.isErr {
				err := lse.Replicate(context.Background(), []types.LLSN{1}, [][]byte{nil})
				assert.Error(t, err)
				return
			}

			// backup
			var llsn types.LLSN
			for _, batchLen := range batchlet.LengthClasses {
				dataList := TestNewBatchData(t, batchLen, 0)
				llsnList := make([]types.LLSN, batchLen)
				for i := 0; i < batchLen; i++ {
					llsn++
					llsnList[i] = llsn
				}
				err := lse.Replicate(context.Background(), llsnList, dataList)
				assert.NoError(t, err)
			}

			// Commit
			var (
				lastLLSN    = types.InvalidLLSN
				lastGLSN    = types.InvalidGLSN
				lastVersion = types.InvalidVersion
			)
			for _, batchLen := range batchlet.LengthClasses {
				assert.Eventually(t, func() bool {
					_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
						TopicID:             lse.tpid,
						LogStreamID:         lse.lsid,
						CommittedLLSNOffset: lastLLSN + 1,
						CommittedGLSNOffset: lastGLSN + 1,
						CommittedGLSNLength: uint64(batchLen),
						Version:             lastVersion + 1,
						HighWatermark:       lastGLSN + types.GLSN(batchLen),
					})

					rpt, err := lse.Report(context.Background())
					assert.NoError(t, err)
					if rpt.Version != lastVersion+1 {
						return false
					}

					lastVersion++
					lastLLSN += types.LLSN(batchLen)
					lastGLSN += types.GLSN(batchLen)
					return true
				}, time.Second, 10*time.Millisecond)
			}

			version, globalHWM, uncommittedLLSNBegin := lse.lsc.reportCommitBase()
			assert.Equal(t, lastVersion, version)
			assert.Equal(t, lastGLSN, globalHWM)
			assert.Equal(t, lastLLSN+1, uncommittedLLSNBegin)

			// FIXME: Fields TopicID and LogStreamID in varlogpb.LogEntryMeta should be filled.
			assert.Equal(t, varlogpb.LogEntryMeta{
				LLSN: types.MinLLSN,
				GLSN: types.MinGLSN,
			}, lse.lsc.localLowWatermark())
			assert.Equal(t, varlogpb.LogEntryMeta{
				LLSN: lastLLSN,
				GLSN: lastGLSN,
			}, lse.lsc.localHighWatermark())

			// simple subscribe
			sr, err := lse.SubscribeWithGLSN(types.MinGLSN, lastGLSN+1)
			assert.NoError(t, err)
			expectedLLSN, expectedGLSN := types.MinLLSN, types.MinGLSN
			for le := range sr.Result() {
				assert.Equal(t, expectedLLSN, le.LLSN)
				assert.Equal(t, expectedGLSN, le.GLSN)
				expectedLLSN++
				expectedGLSN++
			}
			sr.Stop()
			assert.NoError(t, sr.Err())

			// simple subscribe
			sr, err = lse.SubscribeWithLLSN(types.MinLLSN, lastLLSN+1)
			assert.NoError(t, err)
			expectedLLSN = types.MinLLSN
			for le := range sr.Result() {
				assert.Equal(t, expectedLLSN, le.LLSN)
				expectedLLSN++
			}
			sr.Stop()
			assert.NoError(t, sr.Err())
		})
	}
}

func TestExecutor_AppendSeal(t *testing.T) {
	const (
		numClients = 20
		numLogs    = 10
	)
	lse := testNewPrimaryExecutor(t)
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	var wg sync.WaitGroup

	// Append
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := lse.Append(context.Background(), [][]byte{[]byte("hello")})
				if err != nil {
					break
				}
			}
		}()
	}

	// Commit
	var (
		lastLLSN    = types.InvalidLLSN
		lastGLSN    = types.InvalidGLSN
		lastVersion = types.InvalidVersion
	)
	assert.Eventually(t, func() bool {
		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.UncommittedLLSNLength < numLogs {
			return false
		}

		_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			TopicID:             lse.tpid,
			LogStreamID:         lse.lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: uint64(numLogs),
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + types.GLSN(numLogs),
		})

		rpt, err = lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.Version != lastVersion+1 {
			return false
		}

		lastVersion++
		lastLLSN += types.LLSN(numLogs)
		lastGLSN += types.GLSN(numLogs)
		return true
	}, time.Second, 10*time.Millisecond)

	// Seal
	assert.Eventually(t, func() bool {
		status, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return status == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
	}, time.Second, 10*time.Millisecond)

	wg.Wait()

	// Unseal
	err := lse.Unseal(context.Background(), lse.primaryBackups)
	assert.NoError(t, err)

	rpt, err := lse.Report(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		UncommittedLLSNOffset: lastLLSN + 1,
		UncommittedLLSNLength: 0,
		Version:               lastVersion,
		HighWatermark:         lastGLSN,
	}, rpt)

	// Append
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, err := lse.Append(context.Background(), [][]byte{[]byte("hello")})
				if err != nil {
					break
				}
			}
		}()
	}

	// Commit
	assert.Eventually(t, func() bool {
		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.UncommittedLLSNLength < numLogs {
			return false
		}

		_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			TopicID:             lse.tpid,
			LogStreamID:         lse.lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: uint64(numLogs),
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + types.GLSN(numLogs),
		})
		rpt, err = lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.Version != lastVersion+1 {
			return false
		}

		lastVersion++
		lastLLSN += types.LLSN(numLogs)
		lastGLSN += types.GLSN(numLogs)
		return true
	}, time.Second, 10*time.Millisecond)

	// Seal
	assert.Eventually(t, func() bool {
		status, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return status == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
	}, time.Second, 10*time.Millisecond)

	wg.Wait()

	rpt, err = lse.Report(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		UncommittedLLSNOffset: lastLLSN + 1,
		UncommittedLLSNLength: 0,
		Version:               lastVersion,
		HighWatermark:         lastGLSN,
	}, rpt)
}

func TestExecutor_ReplicateSeal(t *testing.T) {
	const numLogs = 10

	lse := testNewBackupExecutor(t)
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	var (
		wg          sync.WaitGroup
		lastLLSN    = types.InvalidLLSN
		lastGLSN    = types.InvalidGLSN
		lastVersion = types.InvalidVersion
	)

	// Replicate
	wg.Add(1)
	go func() {
		defer wg.Done()
		for llsn := lastLLSN + 1; llsn < types.MaxLLSN; llsn++ {
			err := lse.Replicate(context.Background(), []types.LLSN{llsn}, [][]byte{nil})
			if err != nil {
				break
			}
		}
	}()

	// Commit
	assert.Eventually(t, func() bool {
		_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			TopicID:             lse.tpid,
			LogStreamID:         lse.lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: uint64(numLogs),
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + types.GLSN(numLogs),
		})

		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.Version != lastVersion+1 {
			return false
		}

		lastVersion++
		lastLLSN += types.LLSN(numLogs)
		lastGLSN += types.GLSN(numLogs)
		return true
	}, time.Second, 10*time.Millisecond)

	// Seal
	assert.Eventually(t, func() bool {
		status, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return status == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
	}, time.Second, 10*time.Millisecond)

	// wait for stopping a goroutine that calls Replicate RPC.
	wg.Wait()

	// Unseal
	err := lse.Unseal(context.Background(), lse.primaryBackups)
	assert.NoError(t, err)

	rpt, err := lse.Report(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		UncommittedLLSNOffset: lastLLSN + 1,
		UncommittedLLSNLength: 0,
		Version:               lastVersion,
		HighWatermark:         lastGLSN,
	}, rpt)

	// Replicate
	wg.Add(1)
	go func() {
		defer wg.Done()
		for llsn := lastLLSN + 1; llsn < types.MaxLLSN; llsn++ {
			err := lse.Replicate(context.Background(), []types.LLSN{llsn}, [][]byte{nil})
			if err != nil {
				break
			}
		}
	}()

	// Commit
	assert.Eventually(t, func() bool {
		_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			TopicID:             lse.tpid,
			LogStreamID:         lse.lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: uint64(numLogs),
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + types.GLSN(numLogs),
		})

		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.Version != lastVersion+1 {
			return false
		}

		lastVersion++
		lastLLSN += types.LLSN(numLogs)
		lastGLSN += types.GLSN(numLogs)
		return true
	}, time.Second, 10*time.Millisecond)

	// Seal
	assert.Eventually(t, func() bool {
		status, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return status == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
	}, time.Second, 10*time.Millisecond)

	wg.Wait()

	rpt, err = lse.Report(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		UncommittedLLSNOffset: lastLLSN + 1,
		UncommittedLLSNLength: 0,
		Version:               lastVersion,
		HighWatermark:         lastGLSN,
	}, rpt)
}

func TestExecutor_SubscribeWithInvalidRange(t *testing.T) {
	lse := testNewPrimaryExecutor(t)
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	_, err := lse.SubscribeWithGLSN(1, 1)
	assert.Error(t, err)

	_, err = lse.SubscribeWithLLSN(1, 1)
	assert.Error(t, err)

	// FIXME: Use TrimDeprecated rather than modifying localLowWatermark manually.
	lse.globalLowWatermark.glsn = 3
	lse.lsc.setLocalLowWatermark(varlogpb.LogEntryMeta{LLSN: 3, GLSN: 3})

	_, err = lse.SubscribeWithGLSN(1, 4)
	assert.ErrorIs(t, err, verrors.ErrTrimmed)

	_, err = lse.SubscribeWithLLSN(1, 4)
	assert.ErrorIs(t, err, verrors.ErrTrimmed)
}

func TestExecutor_Subscribe(t *testing.T) {
	const numLogs = 10

	lse := testNewPrimaryExecutor(t)
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	expectedGLSN := types.MinGLSN
	go func() {
		defer wg.Done()
		for i := 0; i < numLogs; i++ {
			data := []byte(strconv.Itoa(int(expectedGLSN)))
			res, err := lse.Append(context.Background(), [][]byte{data})
			assert.NoError(t, err)
			assert.Equal(t, []snpb.AppendResult{{
				Meta: varlogpb.LogEntryMeta{
					TopicID:     lse.tpid,
					LogStreamID: lse.lsid,
					GLSN:        expectedGLSN,
					LLSN:        types.LLSN(expectedGLSN),
				},
			}}, res)
			expectedGLSN++
		}
	}()

	var (
		lastLLSN    = types.InvalidLLSN
		lastGLSN    = types.InvalidGLSN
		lastVersion = types.InvalidVersion
	)
	for i := 0; i < numLogs; i++ {
		assert.Eventually(t, func() bool {
			_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
				TopicID:             lse.tpid,
				LogStreamID:         lse.lsid,
				CommittedLLSNOffset: lastLLSN + 1,
				CommittedGLSNOffset: lastGLSN + 1,
				CommittedGLSNLength: 1,
				Version:             lastVersion + 1,
				HighWatermark:       lastGLSN + types.GLSN(1),
			})

			rpt, err := lse.Report(context.Background())
			assert.NoError(t, err)
			if rpt.Version != lastVersion+1 {
				return false
			}

			lastVersion++
			lastLLSN++
			lastGLSN++
			return true
		}, time.Second, 10*time.Millisecond)
	}
	wg.Wait()

	// SubscribeWithGLSN [1, 11): Scan 1, 2, ..., 10
	// NoError
	sr, err := lse.SubscribeWithGLSN(types.MinGLSN, types.GLSN(numLogs+1))
	assert.NoError(t, err)
	for i := 0; i < numLogs; i++ {
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				GLSN:        types.GLSN(i + 1),
				LLSN:        types.LLSN(i + 1),
			},
			Data: []byte(strconv.Itoa(i + 1)),
		}, <-sr.Result())
	}
	_, ok := <-sr.Result()
	assert.False(t, ok)
	sr.Stop()
	assert.NoError(t, sr.Err())

	// SubscribeWithLLSN [1, 11): Scan 1, 2, ..., 10
	// NoError
	sr, err = lse.SubscribeWithLLSN(types.MinLLSN, types.LLSN(numLogs+1))
	assert.NoError(t, err)
	for i := 0; i < numLogs; i++ {
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				LLSN:        types.LLSN(i + 1),
			},
			Data: []byte(strconv.Itoa(i + 1)),
		}, <-sr.Result())
	}
	_, ok = <-sr.Result()
	assert.False(t, ok)
	sr.Stop()
	assert.NoError(t, sr.Err())

	// SubscribeWithGLSN [1, max): Scan 1, 2, ..., 10, wait...
	// Stop -> Error (cancelled)
	sr, err = lse.SubscribeWithGLSN(types.MinGLSN, types.MaxGLSN)
	assert.NoError(t, err)
	for i := 0; i < numLogs; i++ {
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				GLSN:        types.GLSN(i + 1),
				LLSN:        types.LLSN(i + 1),
			},
			Data: []byte(strconv.Itoa(i + 1)),
		}, <-sr.Result())
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok := <-sr.Result()
		assert.False(t, ok)
	}()
	sr.Stop()
	wg.Wait()
	assert.Error(t, sr.Err())

	// SubscribeWithLLSN [1, max): Scan 1, 2, ..., 10, wait...
	// Stop -> Error (cancelled)
	sr, err = lse.SubscribeWithLLSN(types.MinLLSN, types.MaxLLSN)
	assert.NoError(t, err)
	for i := 0; i < numLogs; i++ {
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				LLSN:        types.LLSN(i + 1),
			},
			Data: []byte(strconv.Itoa(i + 1)),
		}, <-sr.Result())
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok := <-sr.Result()
		assert.False(t, ok)
	}()
	sr.Stop()
	wg.Wait()
	assert.Error(t, sr.Err())

	// Append a log at 11, but not yet committed
	var appendWg sync.WaitGroup
	appendWg.Add(1)
	go func() {
		defer appendWg.Done()
		data := []byte(strconv.Itoa(int(expectedGLSN)))
		res, err := lse.Append(context.Background(), [][]byte{data})
		assert.NoError(t, err)
		assert.Equal(t, []snpb.AppendResult{{
			Meta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				GLSN:        expectedGLSN,
				LLSN:        types.LLSN(expectedGLSN),
			},
		}}, res)
		expectedGLSN++
	}()
	assert.Eventually(t, func() bool {
		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		return rpt.UncommittedLLSNLength > 0
	}, time.Second, 10*time.Millisecond)

	// SubscribeWithGLSN [1, 12): Scan 1, 2, ..., 10, wait...
	// Stop -> Error (cancelled)
	sr, err = lse.SubscribeWithGLSN(types.MinGLSN, types.GLSN(numLogs+2))
	assert.NoError(t, err)
	for i := 0; i < numLogs; i++ {
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				GLSN:        types.GLSN(i + 1),
				LLSN:        types.LLSN(i + 1),
			},
			Data: []byte(strconv.Itoa(i + 1)),
		}, <-sr.Result())
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok := <-sr.Result()
		assert.False(t, ok)
	}()
	sr.Stop()
	wg.Wait()
	assert.Error(t, sr.Err())

	// SubscribeWithLLSN [1, 12): Scan 1, 2, ..., 10, wait...
	// Stop -> Error (cancelled)
	sr, err = lse.SubscribeWithLLSN(types.MinLLSN, types.LLSN(numLogs+2))
	assert.NoError(t, err)
	for i := 0; i < numLogs; i++ {
		assert.Equal(t, varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     lse.tpid,
				LogStreamID: lse.lsid,
				LLSN:        types.LLSN(i + 1),
			},
			Data: []byte(strconv.Itoa(i + 1)),
		}, <-sr.Result())
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok := <-sr.Result()
		assert.False(t, ok)
	}()
	sr.Stop()
	wg.Wait()
	assert.Error(t, sr.Err())

	// SubscribeWithGLSN [1, 12): Scan 1, 2, ..., 11
	// NoError
	wg.Add(1)
	go func() {
		defer wg.Done()
		sr, err := lse.SubscribeWithGLSN(types.MinGLSN, types.GLSN(numLogs+2))
		assert.NoError(t, err)
		for i := 0; i < numLogs+1; i++ {
			assert.Equal(t, varlogpb.LogEntry{
				LogEntryMeta: varlogpb.LogEntryMeta{
					TopicID:     lse.tpid,
					LogStreamID: lse.lsid,
					GLSN:        types.GLSN(i + 1),
					LLSN:        types.LLSN(i + 1),
				},
				Data: []byte(strconv.Itoa(i + 1)),
			}, <-sr.Result())
		}
		_, ok := <-sr.Result()
		assert.False(t, ok)
		sr.Stop()
		assert.NoError(t, sr.Err())
	}()

	// SubscribeWithLLSN [1, 12): Scan 1, 2, ..., 11
	// NoError
	wg.Add(1)
	go func() {
		defer wg.Done()
		sr, err := lse.SubscribeWithLLSN(types.MinLLSN, types.LLSN(numLogs+2))
		assert.NoError(t, err)
		for i := 0; i < numLogs+1; i++ {
			assert.Equal(t, varlogpb.LogEntry{
				LogEntryMeta: varlogpb.LogEntryMeta{
					TopicID:     lse.tpid,
					LogStreamID: lse.lsid,
					LLSN:        types.LLSN(i + 1),
				},
				Data: []byte(strconv.Itoa(i + 1)),
			}, <-sr.Result())
		}
		_, ok := <-sr.Result()
		assert.False(t, ok)
		sr.Stop()
		assert.NoError(t, sr.Err())
	}()

	// Commit the log at 11.
	assert.Eventually(t, func() bool {
		_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			TopicID:             lse.tpid,
			LogStreamID:         lse.lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: 1,
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + types.GLSN(1),
		})

		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.Version != lastVersion+1 {
			return false
		}

		lastVersion++
		lastLLSN++
		lastGLSN++
		return true
	}, time.Second, 10*time.Millisecond)

	wg.Wait()
	appendWg.Wait()
}

func TestExecutor_Recover(t *testing.T) {
	const (
		numClients       = 20
		numCommitLogs    = 10
		minCommitVersion = 50
	)

	lse := testNewPrimaryExecutor(t)
	path := lse.stg.Path()

	var wg sync.WaitGroup
	// Append
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer func() {
				// t.Logf("stopping clients %d", i)
				wg.Done()
			}()
			for {
				_, err := lse.Append(context.Background(), [][]byte{[]byte("hello")})
				if err == nil {
					continue
				}
				if assert.ErrorIs(t, err, verrors.ErrClosed) {
					return
				}
			}
		}(i)
	}

	// commit
	var (
		lastLLSN    = types.InvalidLLSN
		lastGLSN    = types.InvalidGLSN
		lastVersion = types.InvalidVersion
	)
	wg.Add(1)
	go func() {
		defer func() {
			// t.Logf("stopping mr simulator")
			wg.Done()
		}()

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			}
			assert.Eventually(t, func() bool {
				_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
					TopicID:             lse.tpid,
					LogStreamID:         lse.lsid,
					CommittedLLSNOffset: lastLLSN + 1,
					CommittedGLSNOffset: lastGLSN + 1,
					CommittedGLSNLength: uint64(numCommitLogs),
					Version:             lastVersion + 1,
					HighWatermark:       lastGLSN + types.GLSN(numCommitLogs),
				})

				rpt, err := lse.Report(context.Background())
				assert.NoError(t, err)
				if rpt.Version != lastVersion+1 {
					return false
				}
				lastVersion++
				lastLLSN += types.LLSN(numCommitLogs)
				lastGLSN += types.GLSN(numCommitLogs)
				return true
			}, time.Second, 10*time.Millisecond)

			if lastVersion >= minCommitVersion {
				assert.NoError(t, lse.Close())
				return
			}
		}
	}()

	wg.Wait()

	lse = testRespawnExecutor(t, lse, path, lastGLSN)
	defer func() {
		assert.NoError(t, lse.Close())
	}()
	rpt, err := lse.Report(context.Background())
	assert.NoError(t, err)
	assert.Positive(t, rpt.Version)
	assert.Equal(t, lastGLSN, rpt.HighWatermark)
	assert.Equal(t, lastLLSN+1, rpt.UncommittedLLSNOffset)

	assert.Equal(t, varlogpb.LogEntryMeta{
		GLSN: types.MinGLSN,
		LLSN: types.MinLLSN,
	}, lse.lsc.localLowWatermark())
	assert.Equal(t, varlogpb.LogEntryMeta{
		GLSN: lastGLSN,
		LLSN: lastLLSN,
	}, lse.lsc.localHighWatermark())
}

func TestExecutorSyncInit_InvalidState(t *testing.T) {
	lse := testNewPrimaryExecutor(t)

	_, err := lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{
		FirstLLSN: 1,
		LastLLSN:  10,
	})
	assert.Error(t, err)

	assert.NoError(t, lse.Close())

	_, err = lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{
		FirstLLSN: 1,
		LastLLSN:  10,
	})
	assert.Error(t, err)
}

func TestExecutorSyncInit_InvalidRange(t *testing.T) {
	lse := testNewPrimaryExecutor(t)
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	status, localHWM, err := lse.Seal(context.Background(), 10)
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, status)
	assert.Equal(t, types.InvalidGLSN, localHWM)

	_, err = lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{
		FirstLLSN: 0,
		LastLLSN:  1,
	})
	assert.Error(t, err)

	_, err = lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{
		FirstLLSN: 2,
		LastLLSN:  1,
	})
	assert.Error(t, err)
}

func TestExecutorSyncInit(t *testing.T) {
	const (
		numLogs         = 10
		syncInitTimeout = time.Second
	)

	lse := testNewPrimaryExecutor(t, WithSyncInitTimeout(syncInitTimeout))
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	srcReplica := varlogpb.LogStreamReplica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: lse.snid + 1,
			Address:       "fake-addr",
		},
		TopicLogStream: varlogpb.TopicLogStream{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < numLogs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.Background(), [][]byte{nil})
			assert.NoError(t, err)
		}()
	}

	// Commit
	var (
		lastLLSN    = types.InvalidLLSN
		lastGLSN    = types.InvalidGLSN
		lastVersion = types.InvalidVersion
	)
	assert.Eventually(t, func() bool {
		_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			TopicID:             lse.tpid,
			LogStreamID:         lse.lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: uint64(numLogs),
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + types.GLSN(numLogs),
		})

		rpt, err := lse.Report(context.Background())
		assert.NoError(t, err)
		if rpt.Version != lastVersion+1 {
			return false
		}

		lastVersion++
		lastLLSN += types.LLSN(numLogs)
		lastGLSN += types.GLSN(numLogs)
		return true
	}, time.Second, 10*time.Millisecond)

	wg.Wait()

	// sealing
	status, localHWM, err := lse.Seal(context.Background(), lastGLSN+1)
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, status)
	assert.Equal(t, lastGLSN, localHWM)

	assert.Panics(t, func() {
		_, _ = lse.SyncInit(context.Background(), srcReplica, snpb.SyncRange{
			FirstLLSN: 1,
			LastLLSN:  lastLLSN - 1,
		})
	})

	// no need to sync
	syncRange, err := lse.SyncInit(context.Background(), srcReplica, snpb.SyncRange{
		FirstLLSN: 1,
		LastLLSN:  lastLLSN,
	})
	assert.NoError(t, err)
	assert.True(t, syncRange.FirstLLSN.Invalid())
	assert.True(t, syncRange.LastLLSN.Invalid())

	// need to sync
	syncRange, err = lse.SyncInit(context.Background(), srcReplica, snpb.SyncRange{
		FirstLLSN: 1,
		LastLLSN:  20,
	})
	assert.NoError(t, err)
	assert.Equal(t, snpb.SyncRange{
		FirstLLSN: lastLLSN + 1,
		LastLLSN:  20,
	}, syncRange)

	newSrcReplica := srcReplica
	newSrcReplica.StorageNodeID++
	assert.Eventually(t, func() bool {
		_, err := lse.SyncInit(context.Background(), newSrcReplica, snpb.SyncRange{
			FirstLLSN: 1,
			LastLLSN:  20,
		})
		return err == nil && lse.srb.srcReplica.Equal(newSrcReplica)
	}, syncInitTimeout*3, syncInitTimeout)
}

func TestExecutorSyncReplicate(t *testing.T) {
	lse := testNewPrimaryExecutor(t)
	defer func() {
		assert.NoError(t, lse.Close())
	}()

	srcReplica := varlogpb.LogStreamReplica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: lse.snid + 1,
			Address:       "fake-addr",
		},
		TopicLogStream: varlogpb.TopicLogStream{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
		},
	}

	assert.Error(t, lse.SyncReplicate(context.Background(), srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		},
	}))

	status, localHWM, err := lse.Seal(context.Background(), 2)
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, status)
	assert.Equal(t, types.InvalidGLSN, localHWM)

	srcRange := snpb.SyncRange{
		FirstLLSN: 1,
		LastLLSN:  2,
	}

	// SyncInit: sealing -> learning
	syncRange, err := lse.SyncInit(context.Background(), srcReplica, srcRange)
	assert.NoError(t, err)
	assert.Equal(t, syncRange, srcRange)
	assert.Equal(t, executorStateLearning, lse.esm.load())

	// SyncReplicate Error: learning -> sealing
	err = lse.SyncReplicate(context.Background(), srcReplica, snpb.SyncPayload{})
	assert.Error(t, err)
	assert.Equal(t, executorStateSealing, lse.esm.load())

	// SyncInit: sealing -> learning
	syncRange, err = lse.SyncInit(context.Background(), srcReplica, srcRange)
	assert.NoError(t, err)
	assert.Equal(t, syncRange, srcRange)
	assert.Equal(t, executorStateLearning, lse.esm.load())

	err = lse.SyncReplicate(context.Background(), srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   2,
			CommittedLLSNBegin: 1,
		},
	})
	assert.NoError(t, err)
	err = lse.SyncReplicate(context.Background(), srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     srcReplica.TopicID,
				LogStreamID: srcReplica.LogStreamID,
				GLSN:        1,
				LLSN:        1,
			},
		},
	})
	assert.NoError(t, err)

	err = lse.SyncReplicate(context.Background(), srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            2,
			HighWatermark:      2,
			CommittedGLSNBegin: 2,
			CommittedGLSNEnd:   3,
			CommittedLLSNBegin: 2,
		},
	})
	assert.NoError(t, err)
	err = lse.SyncReplicate(context.Background(), srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				TopicID:     srcReplica.TopicID,
				LogStreamID: srcReplica.LogStreamID,
				GLSN:        2,
				LLSN:        2,
			},
		},
	})
	assert.NoError(t, err)

	// end of sync: learning -> sealing
	assert.Equal(t, executorStateSealing, lse.esm.load())
}

func TestExecutor_SyncInvalidState(t *testing.T) {
	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	for _, st := range []executorState{executorStateSealing, executorStateLearning, executorStateAppendable} {
		lse.esm.store(st)
		_, err := lse.Sync(context.Background(), varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 2,
				Address:       "addr",
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     2,
				LogStreamID: 3,
			},
		})
		assert.Error(t, err)
	}
}

func TestExecutor_Trim(t *testing.T) {
	const (
		numLogs   = 10
		commitLen = 5
	)

	lse := testNewPrimaryExecutor(t)
	path := lse.stg.Path()

	// trim: not appended logs
	err := lse.Trim(context.Background(), types.MinGLSN)
	assert.Error(t, err)

	// CC:   +-- 1 --+ +--  2 --+
	// LLSN: 1 2 3 4 5 6 7 8 9 10
	// GLSN: 1 2 3 4 5 6 7 8 9 10
	var wg sync.WaitGroup
	for i := 0; i < numLogs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.Background(), [][]byte{[]byte("hello")})
			assert.NoError(t, err)
		}()
	}

	var (
		lastLLSN    = types.InvalidLLSN
		lastGLSN    = types.InvalidGLSN
		lastVersion = types.InvalidVersion
	)
	for i := 0; i < numLogs; i += commitLen {
		assert.Eventually(t, func() bool {
			_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
				TopicID:             lse.tpid,
				LogStreamID:         lse.lsid,
				CommittedLLSNOffset: lastLLSN + 1,
				CommittedGLSNOffset: lastGLSN + 1,
				CommittedGLSNLength: commitLen,
				Version:             lastVersion + 1,
				HighWatermark:       lastGLSN + types.GLSN(commitLen),
			})

			rpt, err := lse.Report(context.Background())
			assert.NoError(t, err)
			if rpt.Version != lastVersion+1 {
				return false
			}

			lastVersion++
			lastLLSN += commitLen
			lastGLSN += commitLen
			return true
		}, time.Second, 10*time.Millisecond)
	}
	wg.Wait()

	// CC:   +-- 1 --+ +--  2 --+
	// LLSN: _ _ _ _ 5 6 7 8 9 10
	// GLSN: _ _ _ _ 5 6 7 8 9 10
	err = lse.Trim(context.Background(), 4)
	assert.NoError(t, err)
	// already trimmed
	_, err = lse.SubscribeWithGLSN(4, types.MaxGLSN)
	assert.ErrorIs(t, err, verrors.ErrTrimmed)
	_, err = lse.SubscribeWithLLSN(4, types.MaxLLSN)
	assert.ErrorIs(t, err, verrors.ErrTrimmed)
	sr, err := lse.SubscribeWithGLSN(5, types.MaxGLSN)
	assert.NoError(t, err)
	for i := 5; i <= 10; i++ {
		le := <-sr.Result()
		assert.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
			GLSN:        types.GLSN(i),
			LLSN:        types.LLSN(i),
		}, le.LogEntryMeta)
	}
	sr.Stop()
	// FIXME(jun): revisit context of subscriber
	// assert.NoError(t, sr.Err())
	sr, err = lse.SubscribeWithLLSN(5, types.MaxLLSN)
	assert.NoError(t, err)
	for i := 5; i <= 10; i++ {
		le := <-sr.Result()
		assert.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
			LLSN:        types.LLSN(i),
		}, le.LogEntryMeta)
	}
	sr.Stop()
	// FIXME(jun): revisit context of subscriber
	// assert.NoError(t, sr.Err())

	// restart after trim
	assert.NoError(t, lse.Close())
	lse = testRespawnExecutor(t, lse, path, lastGLSN)
	rpt, err := lse.Report(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, snpb.LogStreamUncommitReport{
		LogStreamID:           lse.lsid,
		UncommittedLLSNOffset: 11,
		UncommittedLLSNLength: 0,
		Version:               lastVersion,
		HighWatermark:         lastGLSN,
	}, rpt)
	sr, err = lse.SubscribeWithGLSN(5, types.MaxGLSN)
	assert.NoError(t, err)
	for i := 5; i <= 10; i++ {
		le := <-sr.Result()
		assert.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
			GLSN:        types.GLSN(i),
			LLSN:        types.LLSN(i),
		}, le.LogEntryMeta)
	}
	sr.Stop()
	// FIXME(jun): revisit context of subscriber
	// assert.NoError(t, sr.Err())

	// CC:   +-- 1 --+ +--  2 --+
	// LLSN: _ _ _ _ _ 6 7 8 9 10
	// GLSN: _ _ _ _ _ 6 7 8 9 10
	err = lse.Trim(context.Background(), 5)
	assert.NoError(t, err)
	sr, err = lse.SubscribeWithGLSN(6, types.MaxGLSN)
	assert.NoError(t, err)
	for i := 6; i <= 10; i++ {
		le := <-sr.Result()
		assert.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
			GLSN:        types.GLSN(i),
			LLSN:        types.LLSN(i),
		}, le.LogEntryMeta)
	}
	sr.Stop()
	// FIXME(jun): revisit context of subscriber
	// assert.NoError(t, sr.Err())
	sr, err = lse.SubscribeWithLLSN(6, types.MaxLLSN)
	assert.NoError(t, err)
	for i := 6; i <= 10; i++ {
		le := <-sr.Result()
		assert.Equal(t, varlogpb.LogEntryMeta{
			TopicID:     lse.tpid,
			LogStreamID: lse.lsid,
			LLSN:        types.LLSN(i),
		}, le.LogEntryMeta)
	}
	sr.Stop()
	// FIXME(jun): revisit context of subscriber
	// assert.NoError(t, sr.Err())

	err = lse.Trim(context.Background(), 10)
	assert.Error(t, err)

	assert.NoError(t, lse.Close())
}
