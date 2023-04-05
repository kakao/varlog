package logstream

import (
	"context"
	"flag"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/internal/batchlet"
	"github.com/kakao/varlog/internal/storage"
	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

var update = flag.Bool("update", false, "update files")

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

	lse := testNewAppendableExecutor(t,
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
	assert.ErrorIs(t, err, snerrors.ErrClosed)

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

	_, err = lse.SubscribeWithGLSN(types.MinGLSN, types.MinGLSN)
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.SubscribeWithLLSN(types.MinLLSN, types.MinLLSN)
	assert.ErrorIs(t, err, verrors.ErrClosed)

	_, err = lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{FirstLLSN: 1, LastLLSN: 1}, 1)
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

	tcs := []struct {
		name  string
		testf func(t *testing.T, lse *Executor)
	}{
		{
			name: "CouldNotAppend",
			testf: func(t *testing.T, lse *Executor) {
				st, localHWM, err := lse.Seal(context.Background(), types.MaxGLSN)
				assert.NoError(t, err)
				assert.Equal(t, types.InvalidGLSN, localHWM)
				assert.Equal(t, varlogpb.LogStreamStatusSealing, st)
				assert.Equal(t, executorStateSealing, lse.esm.load())

				_, err = lse.Append(context.Background(), TestNewBatchData(t, 1, 0))
				assert.ErrorIs(t, err, verrors.ErrSealed)
			},
		},
		{
			name: "CouldNotReplicate",
			testf: func(t *testing.T, lse *Executor) {
				st, localHWM, err := lse.Seal(context.Background(), types.MaxGLSN)
				assert.NoError(t, err)
				assert.Equal(t, types.InvalidGLSN, localHWM)
				assert.Equal(t, varlogpb.LogStreamStatusSealing, st)
				assert.Equal(t, executorStateSealing, lse.esm.load())

				err = lse.Replicate(context.Background(), []types.LLSN{1}, TestNewBatchData(t, 1, 0))
				assert.ErrorIs(t, err, verrors.ErrSealed)
			},
		},
		{
			name: "PanicNewLogStreamWithInvalidReportCommitBase",
			testf: func(t *testing.T, lse *Executor) {
				ver, hwm, offset, _ := lse.lsc.reportCommitBase()
				lse.lsc.storeReportCommitBase(ver, hwm, offset, true)

				require.Panics(t, func() {
					_, _, _ = lse.Seal(context.Background(), types.InvalidGLSN)
				})
			},
		},
		{
			name: "TheSameLastCommittedGLSNWithInvalidReportCommitBase",
			testf: func(t *testing.T, lse *Executor) {
				const lastLSN = 1
				lse.lsc.storeReportCommitBase(types.Version(1), types.GLSN(lastLSN), varlogpb.LogSequenceNumber{
					LLSN: types.LLSN(lastLSN + 1),
					GLSN: types.GLSN(lastLSN + 1),
				}, true)
				lse.lsc.setLocalLowWatermark(varlogpb.LogSequenceNumber{LLSN: lastLSN, GLSN: lastLSN})
				st, localHWM, err := lse.Seal(context.Background(), types.GLSN(lastLSN))
				assert.NoError(t, err)
				assert.Equal(t, types.GLSN(lastLSN), localHWM)
				assert.Equal(t, varlogpb.LogStreamStatusSealing, st)
				assert.Equal(t, executorStateSealing, lse.esm.load())
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			lse := testNewAppendableExecutor(t,
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

			tc.testf(t, lse)
		})
	}
}

func TestExecutor_Sealed(t *testing.T) {
	const (
		snid = types.StorageNodeID(1)
		tpid = types.TopicID(2)
		lsid = types.LogStreamID(3)
	)

	lse := testNewAppendableExecutor(t,
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

	st, localHWM, err := lse.Seal(context.Background(), types.InvalidGLSN)
	assert.NoError(t, err)
	assert.Equal(t, types.InvalidGLSN, localHWM)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, st)
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

	st, localHWM, err := lse.Seal(context.Background(), lastGLSN)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, st)
	assert.Equal(t, lastGLSN, localHWM)
	assert.NoError(t, err)

	err = lse.Unseal(context.Background(), replicas)
	assert.NoError(t, err)

	lsmd, err = lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogStreamStatusRunning, lsmd.Status)
}

// testNewExecutor returns a log stream executor created by given executorOpts.
// Result executor's state is SEALING.
func testNewExecutor(t *testing.T, executorOpts ...ExecutorOption) *Executor {
	lse, err := NewExecutor(executorOpts...)
	require.NoError(t, err)
	return lse
}

func testNewAppendableExecutor(t *testing.T, replicas []varlogpb.LogStreamReplica, executorOpts ...ExecutorOption) *Executor {
	stg := storage.TestNewStorage(t)

	opts := make([]ExecutorOption, len(executorOpts))
	copy(opts, executorOpts)
	opts = append(opts, WithStorage(stg))
	lse := testNewExecutor(t, opts...)

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
	lse := testNewAppendableExecutor(t,
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
	lse := testNewAppendableExecutor(t,
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
			version, globalHWM, uncommittedBegin, _ := lse.lsc.reportCommitBase()
			assert.Equal(t, lastVersion, version)
			assert.Equal(t, lastGLSN, globalHWM)
			assert.Equal(t, lastLLSN+1, uncommittedBegin.LLSN)

			// FIXME: Fields TopicID and LogStreamID in varlogpb.LogEntryMeta should be filled.
			localLWM, localHWM, _ := lse.lsc.localWatermarks()
			assert.Equal(t, varlogpb.LogSequenceNumber{
				LLSN: types.MinLLSN,
				GLSN: types.MinGLSN,
			}, localLWM)
			assert.Equal(t, varlogpb.LogSequenceNumber{
				LLSN: lastLLSN,
				GLSN: lastGLSN,
			}, localHWM)

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

			version, globalHWM, uncommittedBegin, _ := lse.lsc.reportCommitBase()
			assert.Equal(t, lastVersion, version)
			assert.Equal(t, lastGLSN, globalHWM)
			assert.Equal(t, lastLLSN+1, uncommittedBegin.LLSN)

			// FIXME: Fields TopicID and LogStreamID in varlogpb.LogEntryMeta should be filled.
			localLWM, localHWM, _ := lse.lsc.localWatermarks()
			assert.Equal(t, varlogpb.LogSequenceNumber{
				LLSN: types.MinLLSN,
				GLSN: types.MinGLSN,
			}, localLWM)
			assert.Equal(t, varlogpb.LogSequenceNumber{
				LLSN: lastLLSN,
				GLSN: lastGLSN,
			}, localHWM)

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
		st, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return st == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
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
		st, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return st == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
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
		st, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return st == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
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
		st, localHWM, err := lse.Seal(context.Background(), lastGLSN)
		assert.NoError(t, err)
		return st == varlogpb.LogStreamStatusSealed && localHWM == lastGLSN
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
	lse.lsc.setLocalLowWatermark(varlogpb.LogSequenceNumber{LLSN: 3, GLSN: 3})

	_, err = lse.SubscribeWithGLSN(1, 4)
	assert.ErrorIs(t, err, verrors.ErrTrimmed)

	_, err = lse.SubscribeWithLLSN(1, 4)
	assert.ErrorIs(t, err, verrors.ErrTrimmed)
}

func TestExecutor_SubscribeWithGLSN(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)

		localLWM  = 11
		localHWM  = 20
		numLogs   = localHWM - localLWM + 1
		globalHWM = localHWM + 10
	)

	msg := []byte("foo")

	tcs := []struct {
		name  string
		testf func(t *testing.T, lse *Executor)
	}{
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  : [1    11)
			// Result: None
			name: "SubscribeNothing_TooLowerRange_LowerThanLocalLWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(1, localLWM /*11*/)
				require.NoError(t, err)
				defer sr.Stop()

				_, ok := <-sr.Result()
				require.False(t, ok)
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :                                     [21           31)
			// Result: None
			name: "SubscribeNothing_TooHigherRange_HigherThanLocalHWM_LowerThanGlobalHWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localHWM+1 /*21*/, globalHWM+1 /*31*/)
				require.NoError(t, err)
				defer sr.Stop()

				_, ok := <-sr.Result()
				require.False(t, ok)
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :      [11                            21)
			// Result: 11, 12, .... 20
			name: "SubscribeAll_FromLocalLWM_ToLocalHWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localLWM /*11*/, localHWM+1 /*21*/)
				require.NoError(t, err)
				defer sr.Stop()

				for i := 0; i < numLogs; i++ {
					llsn := types.LLSN(i + 1)
					glsn := types.GLSN(i + localLWM)
					assert.Equal(t, varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     lse.tpid,
							LogStreamID: lse.lsid,
							LLSN:        llsn,
							GLSN:        glsn,
						},
						Data: msg,
					}, <-sr.Result())
				}
				_, ok := <-sr.Result()
				require.False(t, ok)
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :      [11                             22)
			// Result: 11, 12, .... 20
			name: "SubscribeAll_FromLocalLWM_ToHigherThanLocalHWM_LowerThanGlobalHWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localLWM /*11*/, localHWM+2 /*22*/)
				require.NoError(t, err)
				defer sr.Stop()

				for i := 0; i < numLogs; i++ {
					llsn := types.LLSN(i + 1)
					glsn := types.GLSN(i + localLWM)
					assert.Equal(t, varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     lse.tpid,
							LogStreamID: lse.lsid,
							LLSN:        llsn,
							GLSN:        glsn,
						},
						Data: msg,
					}, <-sr.Result())
				}
				_, ok := <-sr.Result()
				require.False(t, ok)
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :      [11                                          31)
			// Result: 11, 12, .... 20
			name: "SubscribeAll_FromLocalLWM_ToGlobalHWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localLWM /*11*/, globalHWM+1 /*31*/)
				require.NoError(t, err)
				defer sr.Stop()

				for i := 0; i < numLogs; i++ {
					llsn := types.LLSN(i + 1)
					glsn := types.GLSN(i + localLWM)
					assert.Equal(t, varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     lse.tpid,
							LogStreamID: lse.lsid,
							LLSN:        llsn,
							GLSN:        glsn,
						},
						Data: msg,
					}, <-sr.Result())
				}
				_, ok := <-sr.Result()
				require.False(t, ok)
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :      [11                                            32)
			// Result: 11, 12, .... 20, wait...
			name: "SubscribeAll_FromLocalLWM_ToHigherThanGlobalHWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localLWM /*11*/, globalHWM+2 /*32*/)
				require.NoError(t, err)
				defer sr.Stop()

				for i := 0; i < numLogs; i++ {
					llsn := types.LLSN(i + 1)
					glsn := types.GLSN(i + localLWM)
					assert.Equal(t, varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     lse.tpid,
							LogStreamID: lse.lsid,
							LLSN:        llsn,
							GLSN:        glsn,
						},
						Data: msg,
					}, <-sr.Result())
				}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, ok := <-sr.Result()
					assert.False(t, ok)
				}()
				sr.Stop()
				wg.Wait()
				assert.Error(t, sr.Err())
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :      [11                                            max)
			// Result: 11, 12, .... 20, wait...
			name: "SubscribeAll_FromLocalLWM_ToMax",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localLWM /*11*/, types.MaxGLSN)
				require.NoError(t, err)
				defer sr.Stop()

				for i := 0; i < numLogs; i++ {
					llsn := types.LLSN(i + 1)
					glsn := types.GLSN(i + localLWM)
					assert.Equal(t, varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     lse.tpid,
							LogStreamID: lse.lsid,
							LLSN:        llsn,
							GLSN:        glsn,
						},
						Data: msg,
					}, <-sr.Result())
				}
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, ok := <-sr.Result()
					assert.False(t, ok)
				}()
				sr.Stop()
				wg.Wait()
				assert.Error(t, sr.Err())
			},
		},
		{
			// LLSN  :        1  2  3  4  5  6  7  8  9 10
			// GLSN  :       11 12 13 14 15 16 17 18 19 20
			// GLWM  : 0 (no trim)
			// GHWM  :                                                30
			//
			// SCAN  :                                    [21              32)
			// Result: wait...
			name: "SubscribeAll_FromHigherThanLocalLWM_ToHigherThanGlobalHWM",
			testf: func(t *testing.T, lse *Executor) {
				sr, err := lse.SubscribeWithGLSN(localHWM+1 /*21*/, globalHWM+2 /*32*/)
				require.NoError(t, err)
				defer sr.Stop()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, ok := <-sr.Result()
					assert.False(t, ok)
				}()
				time.Sleep(100 * time.Millisecond)
				sr.Stop()
				wg.Wait()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			lse := testNewExecutor(t,
				WithClusterID(cid),
				WithStorageNodeID(snid),
				WithTopicID(tpid),
				WithLogStreamID(lsid),
				WithStorage(storage.TestNewStorage(t)),
			)
			defer func() {
				err := lse.Close()
				require.NoError(t, err)
			}()

			lss, lastCommittedGLSN, err := lse.Seal(context.Background(), types.InvalidGLSN)
			require.NoError(t, err)
			require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
			require.Zero(t, lastCommittedGLSN)

			err = lse.Unseal(context.Background(), []varlogpb.LogStreamReplica{{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     tpid,
					LogStreamID: lsid,
				},
			}})
			require.NoError(t, err)

			var wg sync.WaitGroup
			for i := 0; i < numLogs; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{msg})
					assert.NoError(t, err)
				}()
			}
			assert.Eventually(t, func() bool {
				_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid,
					CommittedLLSNOffset: 1,
					CommittedGLSNOffset: localLWM,
					CommittedGLSNLength: numLogs,
					Version:             1,
					HighWatermark:       globalHWM,
				})

				rpt, err := lse.Report(context.Background())
				assert.NoError(t, err)

				return rpt.Version == types.Version(1)
			}, time.Second, 10*time.Millisecond)
			wg.Wait()

			lsrmd, err := lse.Metadata()
			require.NoError(t, err)
			require.Equal(t, types.GLSN(globalHWM), lsrmd.GlobalHighWatermark)
			require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: localLWM}, lsrmd.LocalLowWatermark)
			require.Equal(t, varlogpb.LogSequenceNumber{LLSN: numLogs, GLSN: localHWM}, lsrmd.LocalHighWatermark)

			tc.testf(t, lse)
		})
	}
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
	// Stop -> Error (canceled)
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
	// Stop -> Error (canceled)
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
	// Stop -> Error (canceled)
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
	// Stop -> Error (canceled)
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
			wg.Done()
		}()

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			<-ticker.C
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

	localLWM, localHWM, _ := lse.lsc.localWatermarks()
	assert.Equal(t, varlogpb.LogSequenceNumber{
		GLSN: types.MinGLSN,
		LLSN: types.MinLLSN,
	}, localLWM)
	assert.Equal(t, varlogpb.LogSequenceNumber{
		GLSN: lastGLSN,
		LLSN: lastLLSN,
	}, localHWM)
}

func TestExecutor_SealAfterRestart(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)
	replicas := []varlogpb.LogStreamReplica{{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
		},
		TopicLogStream: varlogpb.TopicLogStream{
			TopicID:     tpid,
			LogStreamID: lsid,
		},
	}}

	newExecutor := func(t *testing.T, path string) *Executor {
		var stg *storage.Storage
		if path == "" {
			stg = storage.TestNewStorage(t)
		} else {
			stg = storage.TestNewStorage(t, storage.WithPath(path))
		}
		lse := testNewExecutor(t,
			WithClusterID(cid),
			WithStorageNodeID(snid),
			WithTopicID(tpid),
			WithLogStreamID(lsid),
			WithStorage(stg),
		)
		return lse
	}

	tcs := []struct {
		name  string
		pref  func(t *testing.T, lse *Executor)
		postf func(t *testing.T, lse *Executor)
	}{
		{
			// An empty log stream replica with no log and commit context
			// should become the state "sealed" by invoking Seal RPC with
			// InvalidGLSN as the last committed GLSN.
			//
			// logs    : _ (empty)
			// cc      : _ (empty)
			// seal    : 0
			// expected: sealed
			name: "LocalHighWatermarkZero_CommitContextNone_LastCommitZero",
			pref: func(t *testing.T, lse *Executor) {
				lss, lhwm, err := lse.Seal(context.Background(), types.InvalidGLSN)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.Zero(t, lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lsrmd.Status)
				require.Equal(t, types.Version(0), lsrmd.Version)
				require.Equal(t, types.GLSN(0), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// An empty log stream replica with no log and commit context
			// should become the state "sealing" by invoking Seal RPC with
			// GLSN(1) as the last committed GLSN.
			//
			// logs    : _ (empty)
			// cc      : _ (empty)
			// seal    : 1
			// expected: sealing
			name: "LocalHighWatermarkZero_CommitContextNone_LastCommitOne",
			pref: func(t *testing.T, lse *Executor) {
				lss, lhwm, err := lse.Seal(context.Background(), 1)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lss)
				require.Zero(t, lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, types.Version(0), lsrmd.Version)
				require.Equal(t, types.GLSN(0), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// A log stream replica with no log entries but commit context
			// should become state "sealing" by invoking Seal RPC with
			// InvalidGLSN as the last committed GLSN.
			//
			// logs    : _ (empty)
			// cc      : 1
			// seal    : 0
			// expected: sealing
			// actual  : panic
			name: "LocalHighWatermarkZero_CommitContextOne_LastCommitZero",
			pref: func(t *testing.T, lse *Executor) {
				storage.TestSetCommitContext(t, lse.stg, storage.CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				})
			},
			postf: func(t *testing.T, lse *Executor) {
				// NOTE: The code prohibits this: Calling Seal RPC with
				// InvalidGLSN means the log stream has no log entries yet.
				// However, the commit context indicates that the log stream
				// replica's local high watermark should be one, and therefore,
				// it panics.
				require.Panics(t, func() {
					_, _, _ = lse.Seal(context.Background(), 0)
				})

				t.Skip("TODO: Should we correct this by replica synchronization?")

				// TODO: Should we correct this by synchronization?
				lss, lhwm, err := lse.Seal(context.Background(), 0)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lss)
				require.Zero(t, lhwm)

				// NOTE: Unstable log stream replica, inconsistent between the
				// log entries and the commit context, sends zero values as a
				// report.
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, types.Version(0), lsrmd.Version)
				require.Equal(t, types.GLSN(0), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// A log stream replica with no log entries due to trim but the
			// commit context indicating GLSN 1 as the local high watermark
			// should become state "sealed" by invoking Seal RPC with GLSN 1 as
			// the last committed GLSN.
			//
			// logs    : _ (empty)
			// cc      : 1
			// seal    : 1
			// expected: sealed
			name: "LocalHighWatermarkZero_CommitContextOne_LastCommitOne",
			pref: func(t *testing.T, lse *Executor) {
				storage.TestSetCommitContext(t, lse.stg, storage.CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				})
			},
			postf: func(t *testing.T, lse *Executor) {
				lss, lhwm, err := lse.Seal(context.Background(), 1)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.Zero(t, lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 2,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         1,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lsrmd.Status)
				require.Equal(t, types.Version(1), lsrmd.Version)
				require.Equal(t, types.GLSN(1), lsrmd.GlobalHighWatermark)
				// NOTE: Watermark values are invalid because all log entries are already trimmed.
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// A log stream replica with no log entries due to trim but the
			// commit context indicating GLSN 1 as the local high watermark
			// should become state "sealing" by invoking Seal RPC with GLSN 2
			// as the last committed GLSN.
			//
			// logs    : _ (empty)
			// cc      : 1
			// seal    : 2
			// expected: sealing
			name: "LocalHighWatermarkZero_CommitContextOne_LastCommitTwo",
			pref: func(t *testing.T, lse *Executor) {
				storage.TestSetCommitContext(t, lse.stg, storage.CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   2,
					CommittedLLSNBegin: 1,
				})
			},
			postf: func(t *testing.T, lse *Executor) {
				lss, lhwm, err := lse.Seal(context.Background(), 2)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lss)
				require.Zero(t, lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 2,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         1,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, types.Version(1), lsrmd.Version)
				require.Equal(t, types.GLSN(1), lsrmd.GlobalHighWatermark)
				// NOTE: Watermark values are invalid because all log entries are already trimmed.
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// A log stream replica, with GLSN 2 as the local high watermark
			// and the commit context indicating GLSN 2 as the local high
			// watermark, should become state "sealing" by invoking Seal RPC
			// with GLSN 1 as the last committed GLSN.
			//
			// logs    : 1 2
			// cc      :   2
			// seal    : 1
			// expected: sealing
			// actual  : panic
			name: "LocalHighWatermarkTwo_CommitContextTwo_LastCommitOne",
			pref: func(t *testing.T, lse *Executor) {
				_, _, err := lse.Seal(context.Background(), types.InvalidGLSN)
				require.NoError(t, err)
				err = lse.Unseal(context.Background(), replicas)
				require.NoError(t, err)

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
				require.Eventually(t, func() bool {
					_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
						TopicID:             tpid,
						LogStreamID:         lsid,
						CommittedLLSNOffset: 1,
						CommittedGLSNOffset: 1,
						CommittedGLSNLength: 2,
						Version:             1,
						HighWatermark:       2,
					})
					rpt, err := lse.Report(context.Background())
					assert.NoError(t, err)
					return rpt.Version == types.Version(1)
				}, time.Second, 10*time.Millisecond)
				wg.Wait()
			},
			postf: func(t *testing.T, lse *Executor) {
				// NOTE: That the metadata repository calls Seal RPC with the
				// last committed GLSN that is lower than the local high
				// watermark of the log stream replica triggers panic.
				require.Panics(t, func() {
					_, _, _ = lse.Seal(context.Background(), types.GLSN(1))
				})

				t.Skip("TODO: Should we correct this by replica synchronization?")

				// TODO: Should we correct this by synchronization?
				lss, lhwm, err := lse.Seal(context.Background(), types.GLSN(1))
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lss)
				require.Equal(t, types.GLSN(2), lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 3,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         2,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, types.Version(1), lsrmd.Version)
				require.Equal(t, types.GLSN(2), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 2, GLSN: 2}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// A log stream replica, with GLSN 2 as the local high watermark
			// and the commit context indicating GLSN 2 as the local high
			// watermark, should become state "sealed" by invoking Seal RPC
			// with GLSN 2 as the last committed GLSN.
			//
			// logs    : 1 2
			// cc      :   2
			// seal    :   2
			// expected: sealed
			name: "LocalHighWatermarkTwo_CommitContextTwo_LastCommitTwo",
			pref: func(t *testing.T, lse *Executor) {
				_, _, err := lse.Seal(context.Background(), types.InvalidGLSN)
				require.NoError(t, err)
				err = lse.Unseal(context.Background(), replicas)
				require.NoError(t, err)

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
				require.Eventually(t, func() bool {
					_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
						TopicID:             tpid,
						LogStreamID:         lsid,
						CommittedLLSNOffset: 1,
						CommittedGLSNOffset: 1,
						CommittedGLSNLength: 2,
						Version:             1,
						HighWatermark:       2,
					})
					rpt, err := lse.Report(context.Background())
					assert.NoError(t, err)
					return rpt.Version == types.Version(1)
				}, time.Second, 10*time.Millisecond)
				wg.Wait()
			},
			postf: func(t *testing.T, lse *Executor) {
				lss, lhwm, err := lse.Seal(context.Background(), types.GLSN(2))
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.Equal(t, types.GLSN(2), lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 3,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         2,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lsrmd.Status)
				require.Equal(t, types.Version(1), lsrmd.Version)
				require.Equal(t, types.GLSN(2), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 2, GLSN: 2}, lsrmd.LocalHighWatermark)
			},
		},
		{
			// A log stream replica, with GLSN 2 as the local high watermark
			// and the commit context indicating GLSN 2 as the local high
			// watermark, should become state "sealing" by invoking Seal RPC
			// with GLSN 3 as the last committed GLSN.
			//
			// logs    : 1 2
			// cc      :   2
			// seal    :     3
			// expected: sealing
			name: "LocalHighWatermarkTwo_CommitContextTwo_LastCommitThree",
			pref: func(t *testing.T, lse *Executor) {
				_, _, err := lse.Seal(context.Background(), types.InvalidGLSN)
				require.NoError(t, err)
				err = lse.Unseal(context.Background(), replicas)
				require.NoError(t, err)

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
				go func() {
					defer wg.Done()
					_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
				require.Eventually(t, func() bool {
					_ = lse.Commit(context.Background(), snpb.LogStreamCommitResult{
						TopicID:             tpid,
						LogStreamID:         lsid,
						CommittedLLSNOffset: 1,
						CommittedGLSNOffset: 1,
						CommittedGLSNLength: 2,
						Version:             1,
						HighWatermark:       2,
					})
					rpt, err := lse.Report(context.Background())
					assert.NoError(t, err)
					return rpt.Version == types.Version(1)
				}, time.Second, 10*time.Millisecond)
				wg.Wait()
			},
			postf: func(t *testing.T, lse *Executor) {
				lss, lhwm, err := lse.Seal(context.Background(), types.GLSN(3))
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lss)
				require.Equal(t, types.GLSN(2), lhwm)

				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 3,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         2,
				}, rpt)

				lsrmd, err := lse.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, types.Version(1), lsrmd.Version)
				require.Equal(t, types.GLSN(2), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 2, GLSN: 2}, lsrmd.LocalHighWatermark)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()

			func() {
				lse := newExecutor(t, path)
				defer func() {
					require.NoError(t, lse.Close())
				}()
				tc.pref(t, lse)
			}()

			if tc.postf != nil {
				lse := newExecutor(t, path)
				defer func() {
					require.NoError(t, lse.Close())
				}()
				tc.postf(t, lse)
			}
		})
	}
}

func TestExecutorSyncInit_InvalidState(t *testing.T) {
	tcs := []struct {
		name  string
		state executorState
	}{
		{
			name:  "Closed",
			state: executorStateClosed,
		},
		{
			name:  "Appendable",
			state: executorStateAppendable,
		},
		{
			name:  "Sealed",
			state: executorStateSealed,
		},
		{
			name:  "Learning",
			state: executorStateLearning,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			lse := &Executor{
				esm: newExecutorStateManager(executorStateSealing),
			}
			lse.syncTimeout = time.Minute
			lse.dstSyncInfo.lastSyncTime = time.Now().Add(-time.Second)
			lse.esm.store(tc.state)
			_, err := lse.SyncInit(context.Background(), varlogpb.LogStreamReplica{}, snpb.SyncRange{
				FirstLLSN: 1,
				LastLLSN:  10,
			}, 10)
			assert.Error(t, err)
		})
	}
}

func TestExecutorSyncInit(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)

		dstLastLSN  = 10
		dstVersion  = 1
		syncTimeout = time.Second
	)

	newExecutor := func(t *testing.T, path string) *Executor {
		var stg *storage.Storage
		if path == "" {
			stg = storage.TestNewStorage(t)
		} else {
			stg = storage.TestNewStorage(t, storage.WithPath(path))
		}
		lse := testNewExecutor(t,
			WithClusterID(cid),
			WithStorageNodeID(snid),
			WithTopicID(tpid),
			WithLogStreamID(lsid),
			WithStorage(stg),
			WithSyncTimeout(syncTimeout),
		)
		return lse
	}

	tcs := []struct {
		name  string
		pref  func(t *testing.T, dst *Executor)
		testf func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica)
	}{
		{
			// dst logs     :   1, 2, ......, 10 (any)
			// dst last llsn:                 10 (any)
			// src logs     : 0               10
			// src last llsn:                 10 (any)
			// response     : InvalidArgument
			name: "InvalidSyncRange_InvalidFirstLLSN_ValidLastLLSN",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: types.InvalidLLSN,
					LastLLSN:  types.LLSN(dstLastLSN + 10),
				}, 10)
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
			},
		},
		{
			// dst logs     : 1, 2, ......, 10 (any)
			// dst last llsn:               10 (any)
			// src logs     : 10, ........., 1
			// src last llsn:               10 (any)
			// response     : InvalidArgument
			name: "InvalidSyncRange_Reversed",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: types.LLSN(dstLastLSN + 10),
					LastLLSN:  1,
				}, 10)
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10 (any)
			// src logs     : 1, 2, ......9
			// src last llsn:             9 (any)
			// response     : Panic
			//
			// The source replica proposed a narrow synchronization range than
			// the destination replica, and it is no way.
			// TODO: Return an error rather than panic.
			name: "DestinationHasMoreLogsThanSource",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				assert.Panics(t, func() {
					_, _ = dst.SyncInit(context.Background(), src, snpb.SyncRange{
						FirstLLSN: 1,
						LastLLSN:  dstLastLSN - 1,
					}, 9)
				})
			},
		},
		{
			// dst logs     : 1, 2, ......, 10 (any)
			// dst last llsn:               10 (any)
			// src logs     : 1, 2, ......, 10
			// src last llsn:             9
			// response     : InvalidArgument
			//
			// The source replica is abnormal because it proposed the wrong
			// state. This request should fail regardless of the status of the
			// destination replica.
			name: "SourceTooLowerLastLLSN",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  10,
				}, 9)
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
			},
		},
		{
			// dst logs     : 1, 2, ......, 10 (any)
			// dst last llsn:               10 (any)
			// src logs     : 1, 2, ......, 10
			// src last llsn:                  11
			// response     : InvalidArgument
			//
			// The source replica is abnormal because it proposed the wrong
			// state. This request should fail regardless of the status of the
			// destination replica.
			name: "SourceTooHigherLastLLSN",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  10,
				}, 11)
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn: no commit context
			// src logs     : 1, 2, ......, 10
			// src last llsn:               10
			// response     : [first, last] = [0, 0]
			//
			// The destination replica has all log entries but no commit
			// context; thus, it replies with a sync range whose fields are
			// InvalidLLSNs. It will lead the source to copy only the commit
			// context to the destination.
			name: "DestinationWithoutOnlyCommitContext",
			pref: func(t *testing.T, dst *Executor) {
				storage.TestDeleteCommitContext(t, dst.stg)
			},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				storage.TestDeleteCommitContext(t, dst.stg)
				syncRange, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  10,
				}, 10)
				require.NoError(t, err)
				require.Equal(t, snpb.SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}, syncRange)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.InvalidVersion, lsrmd.Version)
				require.Equal(t, types.InvalidGLSN, lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateLearning, dst.esm.load())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     : 1, 2, ......, 10
			// src last llsn:               10
			// response     : AlreadyExists
			name: "AlreadySynchronized",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  dstLastLSN,
				}, 10)
				assert.Error(t, err)
				require.Equal(t, codes.AlreadyExists, status.Code(err))

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     : no logs
			// src last llsn:              9
			// response     : Panic
			//
			// The proposed sync range has InvalidLLSN for both FirstLLSN and
			// LastLLSN. Therefore, all log entries in the source replica have
			// been trimmed.
			// SyncInitRequest represents that the last committed LLSN of the
			// source replica is 9. However, the destination replica has more
			// log entries, up to 10. It must be no way.
			name: "TrimmedSource_BadLastCommittedLLSN",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				require.Panics(t, func() {
					_, _ = dst.SyncInit(context.Background(), src, snpb.SyncRange{}, 9)
				})
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     : no logs
			// src last llsn:               10
			// response     : AlreadyExists
			// trimmed      : 1, 2, ......, 10
			name: "TrimmedSource_AlreadySynchronized",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{}, 10)
				assert.Error(t, err)
				require.Equal(t, codes.AlreadyExists, status.Code(err))

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn: no commit context
			// src logs     : no logs
			// src last llsn:               10
			// response     : [first, last] = [0, 0]
			// trimmed      : 1, 2, ......, 10
			//
			// Since the source replica already trimmed log entries, the
			// destination replica also trims them. The destination replica
			// needs to receive the commit context from the source replica.
			name: "TrimmedSource_DestinationWithoutOnlyCommitContext",
			pref: func(t *testing.T, dst *Executor) {
				storage.TestDeleteCommitContext(t, dst.stg)
			},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				syncRange, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{}, 10)
				require.NoError(t, err)
				require.Equal(t, snpb.SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}, syncRange)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.InvalidVersion, lsrmd.Version)
				require.Equal(t, types.InvalidGLSN, lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateLearning, dst.esm.load())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     : no logs
			// src last llsn:                               20
			// response     : [first, last] = [0, 0]
			// trimmed      : 1, 2, ......, 10
			//
			// Since the source replica already trimmed log entries, the
			// destination replica also trims them. The destination replica
			// needs to receive the commit context from the source replica.
			name: "TrimmedSource_Synchronize",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				syncRange, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{}, 20)
				require.NoError(t, err)
				require.Equal(t, snpb.SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}, syncRange)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.InvalidVersion, lsrmd.Version)
				require.Equal(t, types.InvalidGLSN, lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateLearning, dst.esm.load())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     : 1, 2, ......, 10, 11, ......, 20
			// src last llsn:                               20
			// response     :                   11, ......, 20
			name: "SynchronizeNext",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				syncRange, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  dstLastLSN + 10,
				}, 20)
				assert.NoError(t, err)
				assert.Equal(t, snpb.SyncRange{
					FirstLLSN: dstLastLSN + 1,
					LastLLSN:  dstLastLSN + 10,
				}, syncRange)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.InvalidVersion, lsrmd.Version)
				require.Equal(t, types.InvalidGLSN, lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateLearning, dst.esm.load())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     :        5, .., 10, 11, ......, 20
			// src last llsn:                               20
			// response     :                   11, ......, 20
			// trimmed      : 1 .. 4
			name: "SynchronizeMiddle",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				syncRange, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: dstLastLSN - 5,
					LastLLSN:  dstLastLSN + 10,
				}, 20)
				assert.NoError(t, err)
				assert.Equal(t, snpb.SyncRange{
					FirstLLSN: dstLastLSN + 1,
					LastLLSN:  dstLastLSN + 10,
				}, syncRange)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 5, GLSN: 5}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.InvalidVersion, lsrmd.Version)
				require.Equal(t, types.InvalidGLSN, lsrmd.GlobalHighWatermark)

				require.Equal(t, executorStateLearning, dst.esm.load())

				localLWM, localHWM, _ := dst.lsc.localWatermarks()
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 10, GLSN: 10,
				}, localHWM)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 5, GLSN: 5,
				}, localLWM)

				scanner := dst.stg.NewScanner(storage.WithLLSN(types.LLSN(1), types.LLSN(5)))
				defer func() {
					require.NoError(t, scanner.Close())
				}()
				require.False(t, scanner.Valid())
			},
		},
		{
			// dst logs     : 1, 2, ......, 10 (cannot find 4)
			// dst last llsn:               10
			// src logs     :        5, .., 10, 11, ......, 20
			// src last llsn:                               20
			// response     : InternalError
			//
			// The destination replica has a hole; it does not have the log
			// entry at LLSN 4. It is a bizarre situation.
			name: "SynchronizeMiddleButCannotFindTrimPosition",
			pref: func(t *testing.T, dst *Executor) {
				storage.TestDeleteLogEntry(t, dst.stg, varlogpb.LogSequenceNumber{
					LLSN: 4, GLSN: 4,
				})
			},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: dstLastLSN - 5,
					LastLLSN:  dstLastLSN + 10,
				}, 20)
				assert.Error(t, err)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
			},
		},
		{
			// dst logs     : 1, 2, ......, 10 (cannot find 5)
			// dst last llsn:               10
			// src logs     :        5, .., 10, 11, ......, 20
			// src last llsn:                               20
			// response     : InternalError
			//
			// The destination replica has a hole; it does not have the log
			// entry at LLSN 5. It is a bizarre situation.
			name: "SynchronizeMiddleButCannotFindNextLWM",
			pref: func(t *testing.T, dst *Executor) {
				storage.TestDeleteLogEntry(t, dst.stg, varlogpb.LogSequenceNumber{
					LLSN: 5, GLSN: 5,
				})
			},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: dstLastLSN - 5,
					LastLLSN:  dstLastLSN + 10,
				}, 20)
				assert.Error(t, err)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
			},
		},
		{
			// dst logs     : 1, 2, ......, 10
			// dst last llsn:               10
			// src logs     :                       15, .., 20
			// src last llsn:                               20
			// response     :                       15, .., 20
			// trimmed      : 1, 2, ......, 10
			name: "SynchronizeFarFromNext",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				syncRange, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: dstLastLSN + 5,
					LastLLSN:  dstLastLSN + 10,
				}, 20)
				assert.NoError(t, err)
				assert.Equal(t, snpb.SyncRange{
					FirstLLSN: dstLastLSN + 5,
					LastLLSN:  dstLastLSN + 10,
				}, syncRange)

				rpt, err := dst.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 0, GLSN: 0}, lsrmd.LocalHighWatermark)
				require.Equal(t, types.InvalidVersion, lsrmd.Version)
				require.Equal(t, types.InvalidGLSN, lsrmd.GlobalHighWatermark)

				ver, hwm, uncommittedBegin, _ := dst.lsc.reportCommitBase()
				require.True(t, ver.Invalid())
				require.True(t, hwm.Invalid())
				// NOTE: Checking values of uncommittedBegin is implementation-detail.
				require.Equal(t, types.LLSN(dstLastLSN+5), uncommittedBegin.LLSN)
				require.True(t, uncommittedBegin.GLSN.Invalid())

				// Check whether there are no log entries in the replica.
				localLWM, localHWM, _ := dst.lsc.localWatermarks()
				require.True(t, localHWM.Invalid())
				require.True(t, localLWM.Invalid())
				scanner := dst.stg.NewScanner(storage.WithLLSN(types.MinLLSN, types.MaxLLSN))
				defer func() {
					require.NoError(t, scanner.Close())
				}()
				require.False(t, scanner.Valid())
			},
		},
		{
			name: "SyncInitTimeout",
			pref: func(t *testing.T, dst *Executor) {},
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				newSrcPtr, ok := proto.Clone(&src).(*varlogpb.LogStreamReplica)
				require.True(t, ok)
				newSrcPtr.StorageNodeID++
				newSrc := *newSrcPtr

				_, err := dst.SyncInit(context.Background(), src, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  dstLastLSN + 10,
				}, dstLastLSN+10)
				assert.NoError(t, err)

				newSrc.StorageNodeID++
				assert.Eventually(t, func() bool {
					_, err := dst.SyncInit(context.Background(), newSrc, snpb.SyncRange{
						FirstLLSN: 1,
						LastLLSN:  dstLastLSN + 10,
					}, dstLastLSN+10)
					return err == nil
				}, syncTimeout*3, syncTimeout)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			func() {
				dst := newExecutor(t, path)
				defer func() {
					require.NoError(t, dst.Close())
				}()

				replicas := []varlogpb.LogStreamReplica{{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				}}
				_, _, err := dst.Seal(context.Background(), types.InvalidGLSN)
				require.NoError(t, err)
				err = dst.Unseal(context.Background(), replicas)
				require.NoError(t, err)

				var wg sync.WaitGroup
				for i := 0; i < dstLastLSN; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_, err := dst.Append(context.Background(), [][]byte{[]byte("foo")})
						assert.NoError(t, err)
					}()
				}

				assert.Eventually(t, func() bool {
					_ = dst.Commit(context.Background(), snpb.LogStreamCommitResult{
						TopicID:             dst.tpid,
						LogStreamID:         dst.lsid,
						CommittedLLSNOffset: types.LLSN(1),
						CommittedGLSNOffset: types.GLSN(1),
						CommittedGLSNLength: dstLastLSN,
						Version:             types.Version(dstVersion),
						HighWatermark:       types.GLSN(dstLastLSN),
					})

					rpt, err := dst.Report(context.Background())
					assert.NoError(t, err)

					return rpt.UncommittedLLSNOffset == types.LLSN(dstLastLSN+1) &&
						rpt.UncommittedLLSNLength == 0 &&
						rpt.HighWatermark == types.GLSN(dstLastLSN) &&
						rpt.Version == types.Version(1)

				}, time.Second, 10*time.Millisecond)
				wg.Wait()

				lsrmd, err := dst.Metadata()
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusRunning, lsrmd.Status)
				require.Equal(t, types.Version(dstVersion), lsrmd.Version)
				require.Equal(t, types.GLSN(dstLastLSN), lsrmd.GlobalHighWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{LLSN: dstLastLSN, GLSN: dstLastLSN}, lsrmd.LocalHighWatermark)

				tc.pref(t, dst)
			}()

			dst := newExecutor(t, path)
			defer func() {
				require.NoError(t, dst.Close())
			}()

			lsrmd, err := dst.Metadata()
			require.NoError(t, err)
			require.Equal(t, varlogpb.LogStreamStatusSealing, lsrmd.Status)

			/*
				status, localHWM, err := dst.Seal(context.Background(), dstLastLSN+10)
				assert.NoError(t, err)
				assert.Equal(t, varlogpb.LogStreamStatusSealing, status)
				assert.Equal(t, types.GLSN(dstLastLSN), localHWM)
			*/

			// dst: Sealing
			// LLSN: 1..numLogs
			// GLSN: 1..numLogs
			tc.testf(t, dst, varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: dst.snid + 1,
					Address:       "fake-addr",
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     dst.tpid,
					LogStreamID: dst.lsid,
				},
			})
		})
	}
}

func TestExecutorSyncReplicate(t *testing.T) {
	const numLogs = 10

	makeLearningState := func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica, lastCommittedGLSN types.GLSN, proposed, agreed snpb.SyncRange) {
		st, localHWM, err := dst.Seal(context.Background(), lastCommittedGLSN)
		require.NoError(t, err)
		require.Equal(t, varlogpb.LogStreamStatusSealing, st)
		require.Equal(t, types.GLSN(numLogs), localHWM)

		actual, err := dst.SyncInit(context.Background(), src, proposed, proposed.LastLLSN)
		require.NoError(t, err)
		require.Equal(t, agreed, actual)
		require.Equal(t, executorStateLearning, dst.esm.load())
	}

	tcs := []struct {
		name  string
		testf func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica)
	}{
		{
			name: "AppendableState",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{})
				require.Error(t, err)
			},
		},
		{
			name: "SealingState",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				st, localHWM, err := dst.Seal(context.Background(), numLogs+10)
				assert.NoError(t, err)
				assert.Equal(t, varlogpb.LogStreamStatusSealing, st)
				assert.Equal(t, types.GLSN(numLogs), localHWM)

				err = dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{})
				require.Error(t, err)
			},
		},
		{
			name: "SealedState",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				st, localHWM, err := dst.Seal(context.Background(), numLogs)
				assert.NoError(t, err)
				assert.Equal(t, varlogpb.LogStreamStatusSealed, st)
				assert.Equal(t, types.GLSN(numLogs), localHWM)

				err = dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{})
				require.Error(t, err)
			},
		},
		{
			name: "IncorrectSourceReplica",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				src.StorageNodeID++
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{})
				require.Error(t, err)
			},
		},
		{
			name: "EmptyPayload",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{})
				require.Error(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			name: "WrongLogSequenceNumber",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					LogEntry: &varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     dst.tpid,
							LogStreamID: dst.lsid,
							LLSN:        numLogs + 2,
							GLSN:        numLogs + 2,
						},
						Data: nil,
					},
				})
				require.Error(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			name: "NotSequentialLogEntries",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					LogEntry: &varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     dst.tpid,
							LogStreamID: dst.lsid,
							LLSN:        numLogs + 1,
							GLSN:        numLogs + 1,
						},
						Data: nil,
					},
				})
				require.NoError(t, err)
				err = dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					LogEntry: &varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     dst.tpid,
							LogStreamID: dst.lsid,
							LLSN:        numLogs + 3,
							GLSN:        numLogs + 3,
						},
						Data: nil,
					},
				})
				require.Error(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			name: "IncorrectCommitContext_LessLastLLSN",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				for lsn := numLogs + 1; lsn <= lastCommittedLSN; lsn++ {
					err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
						LogEntry: &varlogpb.LogEntry{
							LogEntryMeta: varlogpb.LogEntryMeta{
								TopicID:     dst.tpid,
								LogStreamID: dst.lsid,
								LLSN:        types.LLSN(lsn),
								GLSN:        types.GLSN(lsn),
							},
							Data: nil,
						},
					})
					require.NoError(t, err)
				}
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					CommitContext: &varlogpb.CommitContext{
						Version:            types.Version(2),
						HighWatermark:      lastCommittedLSN,
						CommittedGLSNBegin: numLogs + 1,
						CommittedGLSNEnd:   lastCommittedLSN,
						CommittedLLSNBegin: numLogs + 1,
					},
				})
				require.Error(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			name: "IncorrectCommitContext_GreaterLastLLSN",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				for lsn := numLogs + 1; lsn <= lastCommittedLSN; lsn++ {
					err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
						LogEntry: &varlogpb.LogEntry{
							LogEntryMeta: varlogpb.LogEntryMeta{
								TopicID:     dst.tpid,
								LogStreamID: dst.lsid,
								LLSN:        types.LLSN(lsn),
								GLSN:        types.GLSN(lsn),
							},
							Data: nil,
						},
					})
					require.NoError(t, err)
				}
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					CommitContext: &varlogpb.CommitContext{
						Version:            types.Version(2),
						HighWatermark:      lastCommittedLSN,
						CommittedGLSNBegin: numLogs + 1,
						CommittedGLSNEnd:   lastCommittedLSN + 2,
						CommittedLLSNBegin: numLogs + 1,
					},
				})
				require.Error(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())
			},
		},
		{
			name: "SucceedWithPiggyBack",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				for lsn := numLogs + 1; lsn < lastCommittedLSN; lsn++ {
					err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
						LogEntry: &varlogpb.LogEntry{
							LogEntryMeta: varlogpb.LogEntryMeta{
								TopicID:     dst.tpid,
								LogStreamID: dst.lsid,
								LLSN:        types.LLSN(lsn),
								GLSN:        types.GLSN(lsn),
							},
							Data: nil,
						},
					})
					require.NoError(t, err)
				}
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					LogEntry: &varlogpb.LogEntry{
						LogEntryMeta: varlogpb.LogEntryMeta{
							TopicID:     dst.tpid,
							LogStreamID: dst.lsid,
							LLSN:        lastCommittedLSN,
							GLSN:        lastCommittedLSN,
						},
						Data: nil,
					},
					CommitContext: &varlogpb.CommitContext{
						Version:            types.Version(2),
						HighWatermark:      lastCommittedLSN,
						CommittedGLSNBegin: numLogs + 1,
						CommittedGLSNEnd:   lastCommittedLSN + 1,
						CommittedLLSNBegin: numLogs + 1,
					},
				})
				require.NoError(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())

				st, localHWM, err := dst.Seal(context.Background(), lastCommittedLSN)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, st)
				require.Equal(t, types.GLSN(lastCommittedLSN), localHWM)
			},
		},
		{
			name: "Succeed",
			testf: func(t *testing.T, dst *Executor, src varlogpb.LogStreamReplica) {
				const lastCommittedLSN = numLogs + 10
				makeLearningState(t, dst, src, lastCommittedLSN, snpb.SyncRange{
					FirstLLSN: 1,
					LastLLSN:  lastCommittedLSN,
				}, snpb.SyncRange{
					FirstLLSN: numLogs + 1,
					LastLLSN:  lastCommittedLSN,
				})
				for lsn := numLogs + 1; lsn <= lastCommittedLSN; lsn++ {
					err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
						LogEntry: &varlogpb.LogEntry{
							LogEntryMeta: varlogpb.LogEntryMeta{
								TopicID:     dst.tpid,
								LogStreamID: dst.lsid,
								LLSN:        types.LLSN(lsn),
								GLSN:        types.GLSN(lsn),
							},
							Data: nil,
						},
					})
					require.NoError(t, err)
				}
				err := dst.SyncReplicate(context.Background(), src, snpb.SyncPayload{
					CommitContext: &varlogpb.CommitContext{
						Version:            types.Version(2),
						HighWatermark:      lastCommittedLSN,
						CommittedGLSNBegin: numLogs + 1,
						CommittedGLSNEnd:   lastCommittedLSN + 1,
						CommittedLLSNBegin: numLogs + 1,
					},
				})
				require.NoError(t, err)

				assert.Equal(t, executorStateSealing, dst.esm.load())

				st, localHWM, err := dst.Seal(context.Background(), lastCommittedLSN)
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogStreamStatusSealed, st)
				require.Equal(t, types.GLSN(lastCommittedLSN), localHWM)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			dst := testNewPrimaryExecutor(t)
			defer func() {
				require.NoError(t, dst.Close())
			}()

			var wg sync.WaitGroup
			for i := 0; i < numLogs; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := dst.Append(context.Background(), [][]byte{[]byte("foo")})
					assert.NoError(t, err)
				}()
			}

			assert.Eventually(t, func() bool {
				_ = dst.Commit(context.Background(), snpb.LogStreamCommitResult{
					TopicID:             dst.tpid,
					LogStreamID:         dst.lsid,
					CommittedLLSNOffset: types.LLSN(1),
					CommittedGLSNOffset: types.GLSN(1),
					CommittedGLSNLength: numLogs,
					Version:             types.Version(1),
					HighWatermark:       types.GLSN(numLogs),
				})

				rpt, err := dst.Report(context.Background())
				assert.NoError(t, err)

				return rpt.UncommittedLLSNOffset == types.LLSN(numLogs+1) &&
					rpt.UncommittedLLSNLength == 0 &&
					rpt.HighWatermark == types.GLSN(numLogs) &&
					rpt.Version == types.Version(1)

			}, time.Second, 10*time.Millisecond)
			wg.Wait()

			src := varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: dst.snid + 1,
					Address:       "fake-addr",
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     dst.tpid,
					LogStreamID: dst.lsid,
				},
			}

			tc.testf(t, dst, src)
		})
	}
}

func TestExecutorSync_InvalidState(t *testing.T) {
	lse := &Executor{
		esm: newExecutorStateManager(executorStateSealing),
	}
	for _, st := range []executorState{executorStateClosed, executorStateSealing, executorStateLearning, executorStateAppendable} {
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
	lsrmd, err := lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}, lsrmd.LocalLowWatermark)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)

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
	lsrmd, err = lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 5, GLSN: 5}, lsrmd.LocalLowWatermark)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)

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
	lsrmd, err = lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 5, GLSN: 5}, lsrmd.LocalLowWatermark)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)

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
	lsrmd, err = lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 6, GLSN: 6}, lsrmd.LocalLowWatermark)
	assert.Equal(t, varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}, lsrmd.LocalHighWatermark)

	// CC:   +-- 1 --+ +--  2 --+
	// LLSN: _ _ _ _ _ _ _ _ _ __
	// GLSN: _ _ _ _ _ _ _ _ _ __
	err = lse.Trim(context.Background(), 10)
	assert.NoError(t, err)
	_, err = lse.SubscribeWithGLSN(1, types.MaxGLSN)
	assert.Error(t, err)

	lsrmd, err = lse.Metadata()
	assert.NoError(t, err)
	assert.Equal(t, varlogpb.LogSequenceNumber{
		LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN,
	}, lsrmd.LocalLowWatermark)
	assert.Equal(t, varlogpb.LogSequenceNumber{
		LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN,
	}, lsrmd.LocalHighWatermark)

	assert.NoError(t, lse.Close())
}

func TestExecutorRestore(t *testing.T) {
	tcs := []struct {
		name   string
		golden string
		testf  func(t *testing.T, lse *Executor)
	}{
		{
			name:   "NoLogEntry",
			golden: "./testdata/datadir-00",
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.InvalidVersion, ver)
				require.Equal(t, types.InvalidGLSN, hwm)
				require.Equal(t, types.MinLLSN, uncommittedBegin.LLSN)
				require.False(t, invalid)
				require.Equal(t, types.MinLLSN, lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				//localLWM := lse.lsc.localLowWatermark()
				require.True(t, localLWM.LLSN.Invalid())
				require.True(t, localLWM.GLSN.Invalid())
				// localHWM := lse.lsc.localHighWatermark()
				require.True(t, localHWM.LLSN.Invalid())
				require.True(t, localHWM.GLSN.Invalid())
			},
		},
		{
			name:   "TenLogEntries",
			golden: "./testdata/datadir-01",
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               10,
					HighWatermark:         10,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.Version(10), ver)
				require.Equal(t, types.GLSN(10), hwm)
				require.Equal(t, types.LLSN(11), uncommittedBegin.LLSN)
				require.False(t, invalid)
				require.Equal(t, types.LLSN(11), lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				// localLWM := lse.lsc.localLowWatermark()
				require.Equal(t, types.LLSN(1), localLWM.LLSN)
				require.Equal(t, types.GLSN(1), localLWM.GLSN)
				// localHWM := lse.lsc.localHighWatermark()
				require.Equal(t, types.LLSN(10), localHWM.LLSN)
				require.Equal(t, types.GLSN(10), localHWM.GLSN)
			},
		},
		{
			name:   "TenLogEntriesFollowedByTenEmptyCommitContexts",
			golden: "./testdata/datadir-02",
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               20,
					HighWatermark:         20,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.Version(20), ver)
				require.Equal(t, types.GLSN(20), hwm)
				require.Equal(t, types.LLSN(11), uncommittedBegin.LLSN)
				require.False(t, invalid)
				require.Equal(t, types.LLSN(11), lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				// localLWM := lse.lsc.localLowWatermark()
				require.Equal(t, types.LLSN(1), localLWM.LLSN)
				require.Equal(t, types.GLSN(1), localLWM.GLSN)
				// localHWM := lse.lsc.localHighWatermark()
				require.Equal(t, types.LLSN(10), localHWM.LLSN)
				require.Equal(t, types.GLSN(10), localHWM.GLSN)
			},
		},
		{
			name:   "TenEmptyCommitContextsFollowedByTenLogEntries",
			golden: "./testdata/datadir-03",
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 0,
					Version:               20,
					HighWatermark:         20,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.Version(20), ver)
				require.Equal(t, types.GLSN(20), hwm)
				require.Equal(t, types.LLSN(11), uncommittedBegin.LLSN)
				require.False(t, invalid)
				require.Equal(t, types.LLSN(11), lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				// localLWM := lse.lsc.localLowWatermark()
				require.Equal(t, types.LLSN(1), localLWM.LLSN)
				require.Equal(t, types.GLSN(11), localLWM.GLSN)
				// localHWM := lse.lsc.localHighWatermark()
				require.Equal(t, types.LLSN(10), localHWM.LLSN)
				require.Equal(t, types.GLSN(20), localHWM.GLSN)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			stg := storage.TestNewStorage(t, storage.WithPath(tc.golden), storage.ReadOnly())
			lse, err := NewExecutor(
				WithStorage(stg),
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, lse.Close())
			}()
			tc.testf(t, lse)
		})
	}
}

func TestExecutorResotre_Invalid(t *testing.T) {
	records := []struct {
		num  int
		data []byte
	}{
		{num: 1, data: []byte("one")},
		{num: 2, data: []byte("two")},
		{num: 3, data: []byte("three")},
	}

	tcs := []struct {
		name    string
		golden  string
		updatef func(t *testing.T, stg *storage.Storage)
		testf   func(t *testing.T, lse *Executor)
	}{
		{
			name:   "NoCommitContext",
			golden: "./testdata/datadir-04",
			updatef: func(t *testing.T, stg *storage.Storage) {
				for _, record := range records {
					llsn := types.LLSN(record.num)
					glsn := types.GLSN(record.num)
					storage.TestAppendLogEntryWithoutCommitContext(t, stg, llsn, glsn, record.data)
				}
			},
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.InvalidVersion, ver)
				require.Equal(t, types.InvalidGLSN, hwm)
				require.Equal(t, types.LLSN(4), uncommittedBegin.LLSN)
				require.True(t, invalid)
				require.Equal(t, types.LLSN(4), lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				// localLWM := lse.lsc.localLowWatermark()
				require.Equal(t, types.LLSN(1), localLWM.LLSN)
				require.Equal(t, types.GLSN(1), localLWM.GLSN)
				// localHWM := lse.lsc.localHighWatermark()
				require.Equal(t, types.LLSN(3), localHWM.LLSN)
				require.Equal(t, types.GLSN(3), localHWM.GLSN)
			},
		},
		{
			name:   "FastCommitContext",
			golden: "./testdata/datadir-05",
			updatef: func(t *testing.T, stg *storage.Storage) {
				for _, record := range records {
					llsn := types.LLSN(record.num)
					glsn := types.GLSN(record.num)
					storage.TestAppendLogEntryWithoutCommitContext(t, stg, llsn, glsn, record.data)
				}
				storage.TestSetCommitContext(t, stg, storage.CommitContext{
					Version:            10,
					HighWatermark:      10,
					CommittedGLSNBegin: 11,
					CommittedGLSNEnd:   12,
					CommittedLLSNBegin: 11,
				})
			},
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.InvalidVersion, ver)
				require.Equal(t, types.InvalidGLSN, hwm)
				require.Equal(t, types.LLSN(4), uncommittedBegin.LLSN)
				require.True(t, invalid)
				require.Equal(t, types.LLSN(4), lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				// localLWM := lse.lsc.localLowWatermark()
				require.Equal(t, types.LLSN(1), localLWM.LLSN)
				require.Equal(t, types.GLSN(1), localLWM.GLSN)
				// localHWM := lse.lsc.localHighWatermark()
				require.Equal(t, types.LLSN(3), localHWM.LLSN)
				require.Equal(t, types.GLSN(3), localHWM.GLSN)
			},
		},
		{
			name:   "SlowCommitContext",
			golden: "./testdata/datadir-06",
			updatef: func(t *testing.T, stg *storage.Storage) {
				for _, record := range records {
					llsn := types.LLSN(record.num)
					glsn := types.GLSN(record.num)
					storage.TestAppendLogEntryWithoutCommitContext(t, stg, llsn, glsn, record.data)
				}
				storage.TestSetCommitContext(t, stg, storage.CommitContext{
					Version:            1,
					HighWatermark:      1,
					CommittedGLSNBegin: 2,
					CommittedGLSNEnd:   3,
					CommittedLLSNBegin: 2,
				})
			},
			testf: func(t *testing.T, lse *Executor) {
				rpt, err := lse.Report(context.Background())
				require.NoError(t, err)
				require.Equal(t, snpb.LogStreamUncommitReport{
					LogStreamID:           lse.lsid,
					UncommittedLLSNOffset: 0,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				}, rpt)

				ver, hwm, uncommittedBegin, invalid := lse.lsc.reportCommitBase()
				require.Equal(t, types.InvalidVersion, ver)
				require.Equal(t, types.InvalidGLSN, hwm)
				require.Equal(t, types.LLSN(4), uncommittedBegin.LLSN)
				require.True(t, invalid)
				require.Equal(t, types.LLSN(4), lse.lsc.uncommittedLLSNEnd.Load())
				localLWM, localHWM, _ := lse.lsc.localWatermarks()
				// localLWM := lse.lsc.localLowWatermark()
				require.Equal(t, types.LLSN(1), localLWM.LLSN)
				require.Equal(t, types.GLSN(1), localLWM.GLSN)
				// localHWM := lse.lsc.localHighWatermark()
				require.Equal(t, types.LLSN(3), localHWM.LLSN)
				require.Equal(t, types.GLSN(3), localHWM.GLSN)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			stgOpts := []storage.Option{storage.WithPath(tc.golden)}

			if *update {
				stg := storage.TestNewStorage(t, stgOpts...)
				defer func() {
					assert.NoError(t, stg.Close())
				}()
				tc.updatef(t, stg)
				return
			}

			stgOpts = append(stgOpts, storage.ReadOnly())
			stg := storage.TestNewStorage(t, stgOpts...)
			lse, err := NewExecutor(
				WithStorage(stg),
			)
			require.NoError(t, err)
			defer func() {
				assert.NoError(t, lse.Close())
			}()
			tc.testf(t, lse)
		})
	}
}
