package executor

import (
	"context"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logio"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/stopchannel"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestExecutorClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	err = lse.Close()
	require.NoError(t, err)
}

type testStorageConfig struct {
	writeBatchPutFailProb   atomicutil.AtomicFloat64
	writeBatchApplyFailProb atomicutil.AtomicFloat64
	writeBatchCloseSignal   chan struct{}

	commitBatchPutFailProb   atomicutil.AtomicFloat64
	commitBatchApplyFailProb atomicutil.AtomicFloat64
	commitBatchCloseSignal   chan struct{}
}

func newTestStorageConfig() *testStorageConfig {
	tsc := &testStorageConfig{}
	tsc.writeBatchCloseSignal = make(chan struct{}, 1<<10)
	tsc.commitBatchCloseSignal = make(chan struct{}, 1<<10)
	return tsc
}

func newTestStorage(ctrl *gomock.Controller, cfg *testStorageConfig) storage.Storage {
	writeBatch := storage.NewMockWriteBatch(ctrl)
	writeBatch.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(
		func(llsn types.LLSN, data []byte) error {
			if llsn == types.InvalidLLSN {
				return errors.Wrapf(verrors.ErrInvalid, "llsn == %d", llsn)
			}
			if rand.Float64() < cfg.writeBatchPutFailProb.Load() {
				return errors.New("fake error")
			}
			return nil
		},
	).AnyTimes()
	writeBatch.EXPECT().Apply().DoAndReturn(func() error {
		if rand.Float64() < cfg.writeBatchApplyFailProb.Load() {
			return errors.New("fake error")
		}
		return nil
	}).AnyTimes()
	writeBatch.EXPECT().Close().DoAndReturn(func() error {
		cfg.writeBatchCloseSignal <- struct{}{}
		return nil
	}).AnyTimes()

	commitBatch := storage.NewMockCommitBatch(ctrl)
	commitBatch.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(
		func(llsn types.LLSN, glsn types.GLSN) error {
			if llsn == types.InvalidLLSN {
				return errors.Wrapf(verrors.ErrInvalid, "llsn == %d", llsn)
			}
			if glsn == types.InvalidGLSN {
				return errors.Wrapf(verrors.ErrInvalid, "glsn == %d", glsn)
			}
			if rand.Float64() < cfg.commitBatchPutFailProb.Load() {
				return errors.New("fake error")
			}
			return nil
		},
	).AnyTimes()
	commitBatch.EXPECT().Apply().DoAndReturn(func() error {
		if rand.Float64() < cfg.commitBatchApplyFailProb.Load() {
			return errors.New("fake error")
		}
		return nil
	}).AnyTimes()
	commitBatch.EXPECT().Close().DoAndReturn(func() error {
		cfg.commitBatchCloseSignal <- struct{}{}
		return nil
	}).AnyTimes()

	strg := storage.NewMockStorage(ctrl)
	strg.EXPECT().NewWriteBatch().Return(writeBatch).AnyTimes()
	strg.EXPECT().NewCommitBatch(gomock.Any()).Return(commitBatch, nil).AnyTimes()
	strg.EXPECT().ReadRecoveryInfo().Return(storage.RecoveryInfo{}, nil).AnyTimes()
	strg.EXPECT().Close().Return(nil).AnyTimes()
	return strg
}

func TestExecutorAppend(t *testing.T) {
	const numAppends = 100

	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	for hwm := types.GLSN(1); hwm <= types.GLSN(numAppends); hwm++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			glsn, err := lse.Append(context.TODO(), []byte("foo"))
			require.NoError(t, err)
			require.Equal(t, hwm, glsn)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport(context.TODO())
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
				HighWatermark:       hwm,
				PrevHighWatermark:   hwm - 1,
				CommittedGLSNOffset: hwm,
				CommittedGLSNLength: 1,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport(context.TODO())
			require.NoError(t, err)
			return report.HighWatermark == hwm && report.UncommittedLLSNOffset == types.LLSN(hwm)+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}
}

func TestExecutorRead(t *testing.T) {
	const numAppends = 100

	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	// read invalid position
	_, err = lse.Read(context.TODO(), 0)
	require.Error(t, err)

	// HWM:       3      6   ...
	// COMMIT:  +---+  +---+ ...
	// GLSN:    1 2 3  4 5 6 ...
	// HAVE:    X O X  X O X ...
	for i := 1; i <= numAppends; i++ {
		expectedLLSN := types.LLSN(i)
		expectedHWM := types.GLSN(i * 3)
		expectedGLSN := expectedHWM - 1

		wg := sync.WaitGroup{}
		wg.Add(5)
		go func() {
			defer wg.Done()
			_, err := lse.Read(context.TODO(), expectedGLSN+1)
			require.Error(t, err)
		}()
		go func() {
			defer wg.Done()
			_, err := lse.Read(context.TODO(), expectedGLSN-1)
			require.Error(t, err)
		}()
		go func() {
			defer wg.Done()
			le, err := lse.Read(context.TODO(), expectedGLSN)
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, le.GLSN)
			require.Equal(t, expectedLLSN, le.LLSN)
		}()
		go func() {
			defer wg.Done()
			glsn, err := lse.Append(context.TODO(), []byte("foo"))
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, glsn)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport(context.TODO())
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
				HighWatermark:       expectedHWM,
				PrevHighWatermark:   expectedHWM - 3,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport(context.TODO())
			require.NoError(t, err)
			return report.HighWatermark == expectedHWM &&
				report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}
}

func TestExecutorTrim(t *testing.T) {
	const numAppends = 10
	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	// HWM:         5          10
	// COMMIT:  +-------+  +---- ...
	// GLSN:    1 2 3 4 5  6 7 8 ...
	// HAVE:    X X O X X  X X O ...
	for i := 1; i <= numAppends; i++ {
		expectedHWM := types.GLSN(i * 5)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := expectedHWM - 2

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			glsn, err := lse.Append(context.TODO(), []byte("foo"))
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, glsn)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport(context.TODO())
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
				HighWatermark:       expectedHWM,
				PrevHighWatermark:   expectedHWM - 5,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport(context.TODO())
			require.NoError(t, err)
			return report.HighWatermark == expectedHWM && report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}

	_, err = lse.Read(context.TODO(), 1)
	require.Error(t, err)
	_, err = lse.Read(context.TODO(), 3)
	require.NoError(t, err)
	_, err = lse.Read(context.TODO(), 48)
	require.NoError(t, err)
	_, err = lse.Read(context.TODO(), 50)
	require.Error(t, err)

	// trim 50 (globalHWM), 51
	require.Error(t, lse.Trim(context.TODO(), 50))
	require.Error(t, lse.Trim(context.TODO(), 51))

	// trim [1, 2]
	require.NoError(t, lse.Trim(context.TODO(), 2))
	_, err = lse.Read(context.TODO(), 3)
	require.NoError(t, err)

	// trim [1, 10]
	require.NoError(t, lse.Trim(context.TODO(), 10))
	_, err = lse.Read(context.TODO(), 3)
	require.Error(t, err)
	_, err = lse.Read(context.TODO(), 8)
	require.Error(t, err)
	_, err = lse.Read(context.TODO(), 13)
	require.NoError(t, err)

	// trim [1, 3]
	require.NoError(t, lse.Trim(context.TODO(), 3))

	// trim 49 (localHWM=48)
	require.NoError(t, lse.Trim(context.TODO(), 49))
	_, err = lse.Read(context.TODO(), 48)
	require.Error(t, err)
}

func TestExecutorSubscribe(t *testing.T) {
	const numAppends = 10
	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	// HWM:         5          10
	// COMMIT:  +-------+  +---- ...
	// GLSN:    1 2 3 4 5  6 7 8 ...
	// HAVE:    X X O X X  X X O ...
	for i := 1; i <= numAppends; i++ {
		expectedHWM := types.GLSN(i * 5)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := expectedHWM - 2

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			glsn, err := lse.Append(context.TODO(), []byte("foo"))
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, glsn)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport(context.TODO())
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
				HighWatermark:       expectedHWM,
				PrevHighWatermark:   expectedHWM - 5,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport(context.TODO())
			require.NoError(t, err)
			return report.HighWatermark == expectedHWM && report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}

	var (
		subEnv logio.SubscribeEnv
		ok     bool
	)

	// subscribe [1,1)
	subEnv, err = lse.Subscribe(context.TODO(), 1, 1)
	require.Error(t, err)

	// subscribe [2,1)
	subEnv, err = lse.Subscribe(context.TODO(), 2, 1)
	require.Error(t, err)

	// subscribe [1,2)
	subEnv, err = lse.Subscribe(context.TODO(), 1, 2)
	require.NoError(t, err)
	_, ok = <-subEnv.ScanResultC()
	require.False(t, ok)
	require.ErrorIs(t, subEnv.Err(), io.EOF)
	subEnv.Stop()

	// subscribe [1,51)
	subEnv, err = lse.Subscribe(context.TODO(), 1, 51)
	require.NoError(t, err)
	for i := 1; i <= numAppends; i++ {
		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := types.GLSN(i*5 - 2)
		require.True(t, sr.Valid())
		require.Nil(t, sr.Err)
		require.Equal(t, expectedLLSN, sr.LogEntry.LLSN)
		require.Equal(t, expectedGLSN, sr.LogEntry.GLSN)
	}
	_, ok = <-subEnv.ScanResultC()
	require.False(t, ok)
	require.ErrorIs(t, subEnv.Err(), io.EOF)
	subEnv.Stop()

	// subscribe [48,52)
	// append 53 (hwm=55)
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		subEnv, err := lse.Subscribe(context.TODO(), 48, 52)
		require.NoError(t, err)
		defer subEnv.Stop()

		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.GLSN(48), sr.LogEntry.GLSN)

		sr, ok = <-subEnv.ScanResultC()
		require.False(t, ok)
		require.ErrorIs(t, subEnv.Err(), io.EOF)
	}()
	go func() {
		defer wg.Done()
		glsn, err := lse.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, types.GLSN(53), glsn)
	}()
	go func() {
		defer wg.Done()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport(context.TODO())
			require.NoError(t, err)
			return report.UncommittedLLSNLength > 0
		}, time.Second, time.Millisecond)
		err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
			HighWatermark:       55,
			PrevHighWatermark:   50,
			CommittedGLSNOffset: 53,
			CommittedGLSNLength: 1,
		})
		require.NoError(t, err)
	}()
	wg.Wait()

	// subscribe [56, 57)
	wg.Add(1)
	subEnv, err = lse.Subscribe(context.TODO(), 56, 57)
	go func() {
		defer wg.Done()
		require.NoError(t, err)
		_, ok := <-subEnv.ScanResultC()
		require.False(t, ok)
		require.Error(t, subEnv.Err())
		require.NotErrorIs(t, subEnv.Err(), io.EOF)
	}()
	time.Sleep(5 * time.Millisecond)
	subEnv.Stop()
	wg.Wait()
}

func TestReplicate(t *testing.T) {
	const numAppends = 10

	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	for i := 1; i <= numAppends; i++ {
		expectedHWM := types.GLSN(i * 5)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := expectedHWM - 2

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			err := lse.Replicate(context.TODO(), expectedLLSN, []byte("foo"))
			require.NoError(t, err)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport(context.TODO())
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
				HighWatermark:       expectedHWM,
				PrevHighWatermark:   expectedHWM - 5,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport(context.TODO())
			require.NoError(t, err)
			return report.HighWatermark == expectedHWM && report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}
}

func TestExecutorSeal(t *testing.T) {
	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	// append
	wg := sync.WaitGroup{}
	errC := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.TODO(), []byte("foo"))
			errC <- err
		}()
	}

	require.Eventually(t, func() bool {
		report, err := lse.GetReport(context.TODO())
		require.NoError(t, err)
		return report.UncommittedLLSNLength == 10
	}, time.Second, time.Millisecond)

	err = lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
		HighWatermark:       2,
		PrevHighWatermark:   0,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 2,
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		report, err := lse.GetReport(context.TODO())
		require.NoError(t, err)
		return report.HighWatermark == 2 && report.UncommittedLLSNOffset == 3 && report.UncommittedLLSNLength == 8
	}, time.Second, time.Millisecond)

	// sealing
	status, glsn, err := lse.Seal(context.TODO(), types.GLSN(3))
	assert.NoError(t, err)
	assert.Equal(t, types.GLSN(2), glsn)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, status)

	err = lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
		HighWatermark:       3,
		PrevHighWatermark:   2,
		CommittedGLSNOffset: 3,
		CommittedGLSNLength: 1,
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		report, err := lse.GetReport(context.TODO())
		require.NoError(t, err)
		return report.HighWatermark == 3 && report.UncommittedLLSNOffset == 4 && report.UncommittedLLSNLength == 7
	}, time.Second, time.Millisecond)

	// sealed
	status, glsn, err = lse.Seal(context.TODO(), types.GLSN(3))
	assert.NoError(t, err)
	assert.Equal(t, types.GLSN(3), glsn)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, status)

	// clients receive error
	wg.Wait()
	numErrs := 0
	for i := 0; i < 10; i++ {
		err := <-errC
		if err != nil {
			numErrs++
		}
	}
	assert.Equal(t, 7, numErrs)

	// unseal
	err = lse.Unseal(context.TODO())
	assert.NoError(t, err)

	report, err := lse.GetReport(context.TODO())
	require.NoError(t, err)
	assert.Equal(t, types.GLSN(3), report.HighWatermark)
	assert.Equal(t, types.LLSN(4), report.UncommittedLLSNOffset)
	assert.Zero(t, report.UncommittedLLSNLength)

	// append
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.TODO(), []byte("foo"))
			errC <- err
		}()
	}

	require.Eventually(t, func() bool {
		report, err := lse.GetReport(context.TODO())
		require.NoError(t, err)
		return report.UncommittedLLSNLength == 10
	}, time.Second, time.Millisecond)

	err = lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
		HighWatermark:       13,
		PrevHighWatermark:   3,
		CommittedGLSNOffset: 4,
		CommittedGLSNLength: 10,
	})
	assert.NoError(t, err)

	wg.Wait()

	require.Eventually(t, func() bool {
		report, err := lse.GetReport(context.TODO())
		require.NoError(t, err)
		return report.HighWatermark == 13 && report.UncommittedLLSNOffset == 14 && report.UncommittedLLSNLength == 0
	}, time.Second, time.Millisecond)

	// check LLSN is sequential
	subEnv, err := lse.Subscribe(context.TODO(), types.GLSN(1), types.GLSN(14))
	assert.NoError(t, err)
	defer subEnv.Stop()

	for i := 1; i < 13; i++ {
		sr, ok := <-subEnv.ScanResultC()
		assert.True(t, ok)
		assert.Equal(t, types.LLSN(i), sr.LogEntry.LLSN)
		assert.Equal(t, types.GLSN(i), sr.LogEntry.GLSN)
	}
}

func TestExecutorWithRecover(t *testing.T) {
	defer goleak.VerifyNone(t)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	err = lse.withRecover(func() error {
		return nil
	})
	require.NoError(t, err)

	err = lse.withRecover(func() error {
		return errors.New("fake")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fake")

	err = lse.withRecover(func() error {
		panic(errors.New("fake"))
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fake")
}

func TestExecutorCloseSuddenly(t *testing.T) {
	//defer goleak.VerifyNone(t)

	const (
		numWriter = 100
		numReader = 10
	)

	strg, err := storage.NewStorage(
		storage.WithPath(t.TempDir()),
		storage.WithoutWriteSync(),
		storage.WithoutCommitSync(),
	)
	require.NoError(t, err)

	lse, err := New(WithStorage(strg))
	require.NoError(t, err)

	var lastGLSNs [numWriter]types.AtomicGLSN
	var wg sync.WaitGroup
	for i := 0; i < numWriter; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				glsn, err := lse.Append(context.TODO(), []byte("foo"))
				if err != nil {
					return
				}
				lastGLSNs[idx].Store(glsn)
			}
		}()
	}

	wg.Add(1)
	done := stopchannel.New()
	var alive atomicutil.AtomicBool
	alive.Store(true)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-done.StopC():
				return
			default:
			}
			report, err := lse.GetReport(context.TODO())
			if err != nil {
				return
			}
			if report.UncommittedLLSNLength == 0 {
				runtime.Gosched()
				continue
			}
			if err := lse.Commit(context.TODO(), &snpb.LogStreamCommitResult{
				HighWatermark:       report.GetHighWatermark() + types.GLSN(report.GetUncommittedLLSNLength()),
				PrevHighWatermark:   report.GetHighWatermark(),
				CommittedGLSNOffset: types.GLSN(report.GetUncommittedLLSNOffset()),
				CommittedGLSNLength: report.GetUncommittedLLSNLength(),
			}); err != nil {
				runtime.Gosched()
				continue
			}
		}
	}()

	for j := 0; j < numReader; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done.StopC():
					return
				default:
				}
				i := rand.Intn(numWriter)
				glsn := lastGLSNs[i].Load()
				if glsn.Invalid() {
					continue
				}
				_, err := lse.Read(context.TODO(), glsn)
				if alive.Load() {
					require.NoError(t, err)
				}
			}
		}()
	}

	time.Sleep(time.Second)
	alive.Store(false)
	require.NoError(t, lse.Close())
	time.Sleep(2 * time.Millisecond)
	done.Stop()

	maxGLSN := types.InvalidGLSN
	for i := 0; i < numWriter; i++ {
		if lastGLSNs[i].Load() > maxGLSN {
			maxGLSN = lastGLSNs[i].Load()
		}
	}
	t.Logf("MaxGLSN: %d", maxGLSN)

	wg.Wait()
}
