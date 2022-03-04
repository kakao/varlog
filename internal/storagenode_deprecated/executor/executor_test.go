package executor

import (
	"context"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/stopchannel"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/logio"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/replication"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/storage"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/telemetry"
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

	lse, err := New(
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
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
	writeBatch.EXPECT().Size().Return(0).AnyTimes()

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
	strg.EXPECT().RestoreStorage(gomock.Any(), gomock.Any(), gomock.Any()).Return().AnyTimes()
	return strg
}

func TestExecutorAppend(t *testing.T) {
	const (
		numAppends = 100
		topicID    = 1
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	for ver := types.Version(1); ver <= types.Version(numAppends); ver++ {
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
			require.NoError(t, err)
			require.Equal(t, types.GLSN(ver), res[0].Meta.GLSN)
			require.Equal(t, types.LLSN(ver), res[0].Meta.LLSN)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport()
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
				Version:             ver,
				CommittedGLSNOffset: types.GLSN(ver),
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: types.LLSN(ver),
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == ver && report.UncommittedLLSNOffset == types.LLSN(ver)+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)

		localLWM := lse.lsc.localLowWatermark()
		require.Equal(t, types.MinLLSN, localLWM.LLSN)
		require.Equal(t, types.MinGLSN, localLWM.GLSN)

		localHWM := lse.lsc.localHighWatermark()
		require.Equal(t, types.LLSN(ver), localHWM.LLSN)
		require.Equal(t, types.GLSN(ver), localHWM.GLSN)
	}
}

func TestExecutorRead(t *testing.T) {
	const (
		numAppends = 100
		topicID    = 1
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

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
		expectedVer := types.Version(i)

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
			res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, res[0].Meta.GLSN)
			require.Equal(t, expectedLLSN, res[0].Meta.LLSN)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport()
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
				Version:             expectedVer,
				HighWatermark:       expectedHWM,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: expectedLLSN,
			})
			require.NoError(t, err)
		}()
		wg.Wait()

		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == expectedVer &&
				report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}
}

func TestExecutorTrim(t *testing.T) {
	const (
		numAppends = 10
		topicID    = 1
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	// HWM:         5          10
	// COMMIT:  +-------+  +---- ...
	// GLSN:    1 2 3 4 5  6 7 8 ...
	// HAVE:    X X O X X  X X O ...
	for i := 1; i <= numAppends; i++ {
		expectedHWM := types.GLSN(i * 5)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := expectedHWM - 2
		expectedVer := types.Version(i)

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, res[0].Meta.GLSN)
			require.Equal(t, expectedLLSN, res[0].Meta.LLSN)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport()
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
				Version:             expectedVer,
				HighWatermark:       expectedHWM,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: expectedLLSN,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == expectedVer && report.UncommittedLLSNOffset == expectedLLSN+1 &&
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

	localLWM := lse.lsc.localLowWatermark()
	require.Equal(t, types.MinLLSN, localLWM.LLSN)
	require.Equal(t, types.GLSN(3), localLWM.GLSN)

	localHWM := lse.lsc.localHighWatermark()
	require.Equal(t, types.LLSN(numAppends), localHWM.LLSN)
	require.Equal(t, types.GLSN(48), localHWM.GLSN)

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

	localLWM = lse.lsc.localLowWatermark()
	require.Equal(t, types.LLSN(3), localLWM.LLSN)
	require.Equal(t, types.GLSN(13), localLWM.GLSN)

	localHWM = lse.lsc.localHighWatermark()
	require.Equal(t, types.LLSN(numAppends), localHWM.LLSN)
	require.Equal(t, types.GLSN(48), localHWM.GLSN)

	// trim 49 (localHWM=48)
	t.Skip("SafetyGap should be considered in terms of local watermarks since there is no way to set local watermarks after trimming all of the local logs.")
	require.NoError(t, lse.Trim(context.TODO(), 49))
	_, err = lse.Read(context.TODO(), 48)
	require.Error(t, err)
}

func TestExecutorSubscribe(t *testing.T) {
	const (
		numAppends = 10
		topicID    = 1
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	// HWM:         5          10             50
	// COMMIT:  +-------+  +---- ...   +-------------+
	// GLSN:    1 2 3 4 5  6 7 8 ...   46 47 48 49 50
	// LLSN:    X X 1 X X  X X 2 ...    X X  10  X  X
	for i := 1; i <= numAppends; i++ {
		expectedHWM := types.GLSN(i * 5)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := expectedHWM - 2
		expectedVer := types.Version(i)

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
			require.NoError(t, err)
			require.Equal(t, expectedGLSN, res[0].Meta.GLSN)
			require.Equal(t, expectedLLSN, res[0].Meta.LLSN)
		}()
		go func() {
			defer wg.Done()
			require.Eventually(t, func() bool {
				report, err := lse.GetReport()
				require.NoError(t, err)
				return report.UncommittedLLSNLength > 0
			}, time.Second, time.Millisecond)
			err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
				Version:             expectedVer,
				HighWatermark:       expectedHWM,
				CommittedGLSNOffset: expectedGLSN,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: expectedLLSN,
			})
			require.NoError(t, err)
		}()
		wg.Wait()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == expectedVer && report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, time.Millisecond)
	}

	var (
		subEnv logio.SubscribeEnv
		ok     bool
		sr     storage.ScanResult
	)

	// subscribe [1,1)
	_, err = lse.Subscribe(context.TODO(), 1, 1)
	require.Error(t, err)

	// subscribe [2,1)
	_, err = lse.Subscribe(context.TODO(), 2, 1)
	require.Error(t, err)

	// subscribe [1,2)
	subEnv, err = lse.Subscribe(context.TODO(), 1, 2)
	require.NoError(t, err)
	_, ok = <-subEnv.ScanResultC()
	require.False(t, ok)
	require.ErrorIs(t, subEnv.Err(), io.EOF)
	subEnv.Stop()

	// subscribeTo [1, 2)
	subEnv, err = lse.SubscribeTo(context.Background(), types.LLSN(1), types.LLSN(2))
	require.NoError(t, err)
	sr, ok = <-subEnv.ScanResultC()
	require.True(t, ok)
	require.Equal(t, types.LLSN(1), sr.LogEntry.LLSN)

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

	// subscribeTo [1,11)
	subEnv, err = lse.SubscribeTo(context.TODO(), types.LLSN(1), types.LLSN(numAppends+1))
	require.NoError(t, err)
	for i := 1; i <= numAppends; i++ {
		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		expectedLLSN := types.LLSN(i)
		require.True(t, sr.Valid())
		require.Nil(t, sr.Err)
		require.Equal(t, expectedLLSN, sr.LogEntry.LLSN)
	}
	sr, ok = <-subEnv.ScanResultC()
	require.False(t, ok)
	require.ErrorIs(t, subEnv.Err(), io.EOF)
	subEnv.Stop()

	// subscribe [1,max)
	subEnv, err = lse.Subscribe(context.TODO(), 1, types.MaxGLSN)
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
	subEnv.Stop()

	// subscribeTo [1, max)
	subEnv, err = lse.SubscribeTo(context.TODO(), types.MinLLSN, types.MaxLLSN)
	require.NoError(t, err)
	for i := 1; i <= numAppends; i++ {
		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		expectedLLSN := types.LLSN(i)
		require.True(t, sr.Valid())
		require.Nil(t, sr.Err)
		require.Equal(t, expectedLLSN, sr.LogEntry.LLSN)
	}
	subEnv.Stop()

	// subscribe [48,52)
	// subscribe [48, max)
	// subscribeTo [10, 12)
	// subscribeTo [10, max)
	// append 53 (hwm=55)
	// HWM:         5          10             50               55
	// COMMIT:  +-------+  +---- ...   +-------------+  +-----------
	// GLSN:    1 2 3 4 5  6 7 8 ...   46 47 48 49 50    51 52 53 54 55
	// LLSN:    X X 1 X X  X X 2 ...    X X  10  X  X     X  X 11  X  X
	wg := sync.WaitGroup{}
	wg.Add(6)
	go func() {
		defer wg.Done()
		subEnv, err := lse.Subscribe(context.TODO(), 48, 52)
		require.NoError(t, err)
		defer subEnv.Stop()

		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.GLSN(48), sr.LogEntry.GLSN)

		_, ok = <-subEnv.ScanResultC()
		require.False(t, ok)
		require.ErrorIs(t, subEnv.Err(), io.EOF)
	}()
	go func() {
		defer wg.Done()
		subEnv, err := lse.Subscribe(context.TODO(), 48, types.MaxGLSN)
		require.NoError(t, err)
		defer subEnv.Stop()

		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.GLSN(48), sr.LogEntry.GLSN)

		sr, ok = <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.GLSN(53), sr.LogEntry.GLSN)
	}()
	go func() {
		defer wg.Done()
		subEnv, err := lse.SubscribeTo(context.TODO(), 10, 12)
		require.NoError(t, err)
		defer subEnv.Stop()

		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.LLSN(10), sr.LogEntry.LLSN)

		sr, ok = <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.LLSN(11), sr.LogEntry.LLSN)

		_, ok = <-subEnv.ScanResultC()
		require.False(t, ok)
		require.ErrorIs(t, subEnv.Err(), io.EOF)
	}()
	go func() {
		defer wg.Done()
		subEnv, err := lse.SubscribeTo(context.TODO(), 10, types.MaxLLSN)
		require.NoError(t, err)
		defer subEnv.Stop()

		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.LLSN(10), sr.LogEntry.LLSN)

		sr, ok = <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.LLSN(11), sr.LogEntry.LLSN)
	}()
	go func() {
		defer wg.Done()
		res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
		require.NoError(t, err)
		require.Equal(t, types.GLSN(53), res[0].Meta.GLSN)
	}()
	go func() {
		defer wg.Done()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.UncommittedLLSNLength > 0
		}, time.Second, time.Millisecond)
		err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
			Version:             11,
			HighWatermark:       55,
			CommittedGLSNOffset: 53,
			CommittedGLSNLength: 1,
			CommittedLLSNOffset: 11,
		})
		require.NoError(t, err)
	}()
	wg.Wait()

	// subscribe [56, 57)
	// subscribe [56, max)
	// subscribeTo [12, 13)
	// subscribeTo [12, max)
	wg.Add(4)
	subEnv1, err := lse.Subscribe(context.TODO(), 56, 57)
	require.NoError(t, err)
	subEnv2, err := lse.Subscribe(context.TODO(), 56, types.MaxGLSN)
	require.NoError(t, err)
	subEnv3, err := lse.SubscribeTo(context.TODO(), 12, 13)
	require.NoError(t, err)
	subEnv4, err := lse.SubscribeTo(context.TODO(), 12, types.MaxLLSN)
	require.NoError(t, err)
	go func() {
		defer wg.Done()
		_, ok := <-subEnv1.ScanResultC()
		require.False(t, ok)
		require.Error(t, subEnv1.Err())
		require.NotErrorIs(t, subEnv1.Err(), io.EOF)
	}()
	go func() {
		defer wg.Done()
		_, ok := <-subEnv2.ScanResultC()
		require.False(t, ok)
		require.Error(t, subEnv2.Err())
		require.NotErrorIs(t, subEnv2.Err(), io.EOF)
	}()
	go func() {
		defer wg.Done()
		_, ok := <-subEnv3.ScanResultC()
		require.False(t, ok)
		require.Error(t, subEnv3.Err())
		require.NotErrorIs(t, subEnv3.Err(), io.EOF)
	}()
	go func() {
		defer wg.Done()
		_, ok := <-subEnv4.ScanResultC()
		require.False(t, ok)
		require.Error(t, subEnv4.Err())
		require.NotErrorIs(t, subEnv4.Err(), io.EOF)
	}()
	time.Sleep(5 * time.Millisecond)
	subEnv1.Stop()
	subEnv2.Stop()
	subEnv3.Stop()
	subEnv4.Stop()
	wg.Wait()

	// subscribeTo before appended
	// HWM:         5          10             50               55              60
	// COMMIT:  +-------+  +---- ...   +-------------+  +----------------+
	// GLSN:    1 2 3 4 5  6 7 8 ...   46 47 48 49 50    51 52 53 54 55    56 57 58 59 60
	// LLSN:    X X 1 X X  X X 2 ...    X X  10  X  X     X  X 11  X  X     X  X 12  X  X
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
		require.NoError(t, err)
	}()

	// data is stored, but not committed
	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.UncommittedLLSNLength > 0
	}, time.Second, time.Millisecond)

	// subscribeTo [1,13), but last one (12) is not committed yet.
	subEnv, err = lse.SubscribeTo(context.TODO(), 1, 13)
	require.NoError(t, err)
	for i := 0; i < 11; i++ {
		sr, ok := <-subEnv.ScanResultC()
		require.True(t, ok)
		require.Equal(t, types.LLSN(i+1), sr.LogEntry.LLSN)
	}

	// it blocks until last one can be decided.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, ok = <-subEnv.ScanResultC()
		require.True(t, ok)
	}()

	err = lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
		Version:             12,
		HighWatermark:       60,
		CommittedGLSNOffset: 58,
		CommittedGLSNLength: 1,
		CommittedLLSNOffset: 12,
	})
	require.NoError(t, err)
	wg.Wait()
}

func TestExecutorReplicate(t *testing.T) {
	const (
		numAppends  = 10
		logStreamID = 1
		primarySNID = 1
		backupSNID  = 2
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithStorageNodeID(backupSNID),
		WithLogStreamID(logStreamID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: primarySNID,
			},
			LogStreamID: logStreamID,
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: backupSNID,
			},
			LogStreamID: logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	for i := 1; i <= numAppends; i++ {
		expectedHWM := types.GLSN(i * 5)
		expectedLLSN := types.LLSN(i)
		expectedGLSN := expectedHWM - 2
		expectedVer := types.Version(i)

		assert.NoError(t, lse.Replicate(context.TODO(), expectedLLSN, []byte("foo")))

		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.UncommittedLLSNLength == 1
		}, time.Second, 10*time.Millisecond)

		require.NoError(t, lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
			Version:             expectedVer,
			HighWatermark:       expectedHWM,
			CommittedGLSNOffset: expectedGLSN,
			CommittedGLSNLength: 1,
			CommittedLLSNOffset: expectedLLSN,
		}))

		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == expectedVer && report.UncommittedLLSNOffset == expectedLLSN+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, 10*time.Millisecond)
	}
}

func TestExecutorSealSuddenly(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		numWriters = 10
		topicID    = 1
	)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, lse.Close())
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	var wg sync.WaitGroup
	maxGLSNs := make([]types.GLSN, numWriters)
	lastErrs := make([]error, numWriters)
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
				if err == nil {
					maxGLSNs[idx] = res[0].Meta.GLSN
				}
				lastErrs[idx] = err
				if err != nil {
					return
				}
			}
		}(i)
	}

	var commitResults []snpb.LogStreamCommitResult
	var lastCommittedGLSN types.GLSN
	var sealed bool
	var muSealed sync.Mutex
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}

			report, err := lse.GetReport()
			assert.NoError(t, err)

			if report.UncommittedLLSNLength == 0 {
				continue
			}

			var cr snpb.LogStreamCommitResult
			i := sort.Search(len(commitResults), func(i int) bool {
				return report.GetVersion() <= commitResults[i].GetVersion()-1
			})
			if i < len(commitResults) {
				assert.Equal(t, commitResults[i].GetVersion()-1, report.GetVersion())
				for i < len(commitResults) {
					lse.Commit(context.TODO(), commitResults[i])
					i++
				}
				continue
			}

			muSealed.Lock()
			if sealed {
				muSealed.Unlock()
				continue
			}

			cr = snpb.LogStreamCommitResult{
				Version:             report.GetVersion() + 1,
				HighWatermark:       types.GLSN(report.GetUncommittedLLSNOffset()) + types.GLSN(report.GetUncommittedLLSNLength()) - 1,
				CommittedGLSNOffset: types.GLSN(report.GetUncommittedLLSNOffset()),
				CommittedGLSNLength: report.GetUncommittedLLSNLength(),
				CommittedLLSNOffset: report.GetUncommittedLLSNOffset(),
			}
			lastCommittedGLSN = cr.GetCommittedGLSNOffset() + types.GLSN(cr.GetCommittedGLSNLength()) - 1
			commitResults = append(commitResults, cr)
			lse.Commit(context.TODO(), cr)
			muSealed.Unlock()
		}
	}()

	time.Sleep(time.Second)
	muSealed.Lock()
	sealed = true
	muSealed.Unlock()

	require.Eventually(t, func() bool {
		status, glsn, err := lse.Seal(context.TODO(), lastCommittedGLSN)

		require.NoError(t, err)
		return status == varlogpb.LogStreamStatusSealed && glsn == lastCommittedGLSN
	}, time.Second, 10*time.Millisecond)

	close(done)
	wg.Wait()
}

func TestExecutorSeal(t *testing.T) {
	const topicID = 1

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	// append
	wg := sync.WaitGroup{}
	errC := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
			// FIXME(jun): Batch append can report the error by using either return
			// value or field of AppendResult, which makes it hard to handle the error.
			if err == nil && len(res[0].Error) > 0 {
				err = errors.New(res[0].Error)
			}
			errC <- err
		}()
	}

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.UncommittedLLSNLength == 10
	}, time.Second, time.Millisecond)

	err = lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
		Version:             1,
		HighWatermark:       2,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 2,
		CommittedLLSNOffset: 1,
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.Version == 1 && report.UncommittedLLSNOffset == 3 && report.UncommittedLLSNLength == 8
	}, time.Second, time.Millisecond)

	// sealing
	status, glsn, err := lse.Seal(context.TODO(), types.GLSN(3))
	assert.NoError(t, err)
	assert.Equal(t, types.GLSN(2), glsn)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, status)

	err = lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
		Version:             2,
		HighWatermark:       3,
		CommittedGLSNOffset: 3,
		CommittedGLSNLength: 1,
		CommittedLLSNOffset: 3,
	})
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.Version == 2 && report.UncommittedLLSNOffset == 4 && report.UncommittedLLSNLength == 7
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
	err = lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	})
	assert.NoError(t, err)

	report, err := lse.GetReport()
	require.NoError(t, err)
	assert.Equal(t, types.Version(2), report.Version)
	assert.Equal(t, types.LLSN(4), report.UncommittedLLSNOffset)
	assert.Zero(t, report.UncommittedLLSNLength)

	// append
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
			errC <- err
		}()
	}

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.UncommittedLLSNLength == 10
	}, time.Second, 100*time.Millisecond)

	err = lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
		Version:             3,
		HighWatermark:       13,
		CommittedGLSNOffset: 4,
		CommittedGLSNLength: 10,
		CommittedLLSNOffset: 4,
	})
	assert.NoError(t, err)

	wg.Wait()

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.Version == 3 && report.UncommittedLLSNOffset == 14 && report.UncommittedLLSNLength == 0
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

func TestExecutorSealReason(t *testing.T) {
	const topicID = types.TopicID(1)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	path := t.TempDir()

	strg, err := storage.NewStorage(storage.WithPath(path))
	require.NoError(t, err)
	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	// SEALING (initial state)
	_, err = lse.Append(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "initial state")

	// SEALED
	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	// RUNNING
	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	status, _, err = lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	_, err = lse.Append(context.Background(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "seal rpc")
}

func TestExecutorWithRecover(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	defer func() {
		err = lse.Close()
		require.NoError(t, err)
	}()

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

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
	// defer goleak.VerifyNone(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		numWriter = 100
		numReader = 10
		topicID   = 1
	)

	strg, err := storage.NewStorage(
		storage.WithPath(t.TempDir()),
		storage.WithoutWriteSync(),
		storage.WithoutCommitSync(),
	)
	require.NoError(t, err)

	lse, err := New(
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	status, sealedGLSN, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	var lastGLSNs [numWriter]types.AtomicGLSN
	var wg sync.WaitGroup
	for i := 0; i < numWriter; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				res, err := lse.Append(context.TODO(), [][]byte{[]byte("foo")})
				if err != nil {
					return
				}
				lastGLSNs[idx].Store(res[0].Meta.GLSN)
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
			report, err := lse.GetReport()
			if err != nil {
				return
			}
			if report.UncommittedLLSNLength == 0 {
				runtime.Gosched()
				continue
			}
			if err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
				Version:             report.GetVersion() + 1,
				HighWatermark:       types.GLSN(report.GetUncommittedLLSNOffset()) + types.GLSN(report.GetUncommittedLLSNLength()) - 1,
				CommittedGLSNOffset: types.GLSN(report.GetUncommittedLLSNOffset()),
				CommittedGLSNLength: report.GetUncommittedLLSNLength(),
				CommittedLLSNOffset: report.GetUncommittedLLSNOffset(),
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

func TestExecutorNew(t *testing.T) {
	const topicID = 1

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	path := t.TempDir()

	strg, err := storage.NewStorage(storage.WithPath(path))
	require.NoError(t, err)
	lse, err := New(
		WithStorage(strg),
		WithTopicID(topicID),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))

	var (
		appendWg sync.WaitGroup
		commitWg sync.WaitGroup
	)

	appendWg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer appendWg.Done()
			_, _ = lse.Append(context.TODO(), [][]byte{[]byte("foo")})
		}()
	}

	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.UncommittedLLSNLength == 10
		}, time.Second, time.Millisecond)
		err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
			Version:             1,
			HighWatermark:       5,
			CommittedGLSNOffset: 1,
			CommittedGLSNLength: 5,
			CommittedLLSNOffset: 1,
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == 1
		}, time.Second, time.Millisecond)
	}()

	commitWg.Wait()

	report, err := lse.GetReport()
	require.NoError(t, err)
	require.Equal(t, types.Version(1), report.Version)
	require.Equal(t, types.LLSN(6), report.UncommittedLLSNOffset)
	require.EqualValues(t, 5, report.UncommittedLLSNLength)

	require.NoError(t, lse.Close())
	appendWg.Wait()

	// Restart executor
	strg, err = storage.NewStorage(storage.WithPath(path))
	require.NoError(t, err)
	lse, err = New(
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)

	report, err = lse.GetReport()
	require.NoError(t, err)
	require.Equal(t, types.Version(1), report.Version)
	require.Equal(t, types.LLSN(6), report.UncommittedLLSNOffset)
	require.EqualValues(t, 5, report.UncommittedLLSNLength)

	err = lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
		Version:             2,
		HighWatermark:       8,
		CommittedGLSNOffset: 6,
		CommittedGLSNLength: 3,
		CommittedLLSNOffset: 6,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.Version == 2
	}, time.Second, time.Millisecond)

	// Seal
	status, _, err = lse.Seal(context.TODO(), types.GLSN(8))
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	// Check if uncommitted logs are deleted
	report, err = lse.GetReport()
	require.NoError(t, err)
	require.Equal(t, types.Version(2), report.Version)
	require.Equal(t, types.LLSN(9), report.UncommittedLLSNOffset)
	require.EqualValues(t, 0, report.UncommittedLLSNLength)

	// Unseal
	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	require.NoError(t, lse.Close())
}

func TestExecutorGetPrevCommitInfo(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		logStreamID = types.LogStreamID(1)
		topicID     = 1
	)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithLogStreamID(logStreamID),
		WithTopicID(topicID),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
			_, _ = lse.Append(context.TODO(), [][]byte{[]byte("foo")})
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.UncommittedLLSNLength == 10
		}, time.Second, time.Millisecond)

		err := lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
			Version:             1,
			HighWatermark:       5,
			CommittedGLSNOffset: 1,
			CommittedGLSNLength: 5,
			CommittedLLSNOffset: 1,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == 1
		}, time.Second, time.Millisecond)

		err = lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
			Version:             2,
			HighWatermark:       20,
			CommittedGLSNOffset: 11,
			CommittedGLSNLength: 5,
			CommittedLLSNOffset: 6,
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == 2
		}, time.Second, time.Millisecond)
	}()
	wg.Wait()

	// LLSN | GLSN | HWM | PrevHWM
	//    1 |    1 |   5 |       0
	//    2 |    2 |   5 |       0
	//    3 |    3 |   5 |       0
	//    4 |    4 |   5 |       0
	//    5 |    5 |   5 |       0
	//    6 |   11 |  20 |       5
	//    7 |   12 |  20 |       5
	//    8 |   13 |  20 |       5
	//    9 |   14 |  20 |       5
	//   10 |   15 |  20 |       5

	commitInfo, err := lse.GetPrevCommitInfo(0)
	require.NoError(t, err)
	require.Equal(t, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  10,
		Version:             1,
	}, commitInfo)

	commitInfo, err = lse.GetPrevCommitInfo(1)
	require.NoError(t, err)
	require.Equal(t, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 6,
		CommittedGLSNOffset: 11,
		CommittedGLSNLength: 5,
		HighestWrittenLLSN:  10,
		Version:             2,
	}, commitInfo)

	commitInfo, err = lse.GetPrevCommitInfo(2)
	require.NoError(t, err)
	require.Equal(t, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: types.InvalidLLSN,
		CommittedGLSNOffset: types.InvalidGLSN,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  10,
		Version:             0,
	}, commitInfo)

	commitInfo, err = lse.GetPrevCommitInfo(3)
	require.NoError(t, err)
	require.Equal(t, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID,
		Status:              snpb.GetPrevCommitStatusNotFound,
		CommittedLLSNOffset: types.InvalidLLSN,
		CommittedGLSNOffset: types.InvalidGLSN,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  10,
		Version:             0,
	}, commitInfo)
}

func TestExecutorGetPrevCommitInfoWithEmptyCommitContext(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const logStreamID = types.LogStreamID(1)

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithLogStreamID(logStreamID),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: lse.storageNodeID,
			},
			LogStreamID: lse.logStreamID,
		},
	}))

	require.NoError(t, lse.Commit(context.TODO(), snpb.LogStreamCommitResult{
		Version:             1,
		HighWatermark:       5,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 0,
		CommittedLLSNOffset: 1,
	}))

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		require.NoError(t, err)
		return report.Version == 1
	}, time.Second, time.Millisecond)

	commitInfo, err := lse.GetPrevCommitInfo(0)
	require.NoError(t, err)

	require.Equal(t, &snpb.LogStreamCommitInfo{
		LogStreamID:         logStreamID,
		Status:              snpb.GetPrevCommitStatusOK,
		CommittedLLSNOffset: 1,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 0,
		HighestWrittenLLSN:  0,
		Version:             1,
	}, commitInfo)
}

func TestExecutorUnsealWithInvalidReplicas(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithLogStreamID(1),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.Error(t, lse.Unseal(context.TODO(), nil))
	require.Error(t, lse.Unseal(context.TODO(), []varlogpb.Replica{}))
	require.Error(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 2,
			},
			LogStreamID: 2,
		},
	}))
	require.Error(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
	}))
}

func TestExecutorPrimaryBackup(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithStorageNodeID(1),
		WithLogStreamID(1),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	// Primary
	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)
	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
	}))
	require.True(t, lse.isPrimay())

	// Backup
	status, _, err = lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)
	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 2,
			},
			LogStreamID: 1,
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
	}))
	require.False(t, lse.isPrimay())

	// Not found
	status, _, err = lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)
	require.Error(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 2,
			},
			LogStreamID: 1,
		},
	}))
	require.Error(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 2,
		},
	}))
}

func TestExecutorSyncInitNewReplica(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithStorageNodeID(1),
		WithLogStreamID(1),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	// state: sealing
	span, err := lse.SyncInit(
		context.Background(),
		snpb.SyncRange{FirstLLSN: 1, LastLLSN: 10},
	)
	require.NoError(t, err)
	require.Equal(t, snpb.SyncRange{FirstLLSN: 1, LastLLSN: 10}, span)
	require.Equal(t, executorLearning, lse.stateBarrier.state.load())
}

func TestExecutorSyncInitInvalidState(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithStorageNodeID(1),
		WithLogStreamID(1),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	// SEALED
	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)
	_, err = lse.SyncInit(
		context.Background(),
		snpb.SyncRange{FirstLLSN: 1, LastLLSN: 10},
	)
	require.Error(t, err)

	// RUNNING
	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
	}))
	// FIXME (jun): use varlogpb.LogStreamStatus!
	require.Equal(t, executorMutable, lse.stateBarrier.state.load())
	_, err = lse.SyncInit(
		context.Background(),
		snpb.SyncRange{FirstLLSN: 1, LastLLSN: 10},
	)
	require.Error(t, err)
}

func TestExecutorSyncBackupReplica(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithStorageNodeID(1),
		WithLogStreamID(1),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 2,
			},
			LogStreamID: 1,
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
	}))
	require.False(t, lse.isPrimay())

	for i := 1; i <= 2; i++ {
		llsn := types.LLSN(i)
		glsn := types.GLSN(i)
		ver := types.Version(i)

		assert.NoError(t, lse.Replicate(context.Background(), llsn, []byte("foo")))

		assert.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.UncommittedLLSNLength == 1
		}, time.Second, 10*time.Millisecond)

		assert.NoError(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
			Version:             ver,
			HighWatermark:       glsn,
			CommittedGLSNOffset: glsn,
			CommittedGLSNLength: 1,
			CommittedLLSNOffset: llsn,
		}))

		require.Eventually(t, func() bool {
			report, err := lse.GetReport()
			require.NoError(t, err)
			return report.Version == ver && report.UncommittedLLSNOffset == llsn+1 &&
				report.UncommittedLLSNLength == 0
		}, time.Second, 10*time.Millisecond)
	}

	for i := 3; i <= 4; i++ {
		llsn := types.LLSN(i)
		assert.NoError(t, lse.Replicate(context.Background(), llsn, []byte("foo")))
	}

	require.Eventually(t, func() bool {
		rpt, err := lse.GetReport()
		assert.NoError(t, err)
		return rpt.GetUncommittedLLSNOffset() == 3 && rpt.GetUncommittedLLSNLength() == 2
	}, time.Second, 10*time.Millisecond)

	// LLSN | GLSN
	//    1 |    1
	//    2 |    2
	//    3 |    -
	//    4 |    -

	status, _, err = lse.Seal(context.Background(), 4)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealing, status)

	last, err := lse.SyncInit(context.Background(), snpb.SyncRange{FirstLLSN: 1, LastLLSN: 4})
	require.NoError(t, err)
	require.Equal(t, snpb.SyncRange{FirstLLSN: 3, LastLLSN: 4}, last)
	// NOTE: This assertion checks private variable.
	require.Equal(t, executorLearning, lse.stateBarrier.state.load())

	// Replica in learning state should not accept commit request.
	assert.Error(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
		Version:             3,
		HighWatermark:       4,
		CommittedGLSNOffset: 3,
		CommittedGLSNLength: 2,
		CommittedLLSNOffset: 3,
	}))

	rpt, err := lse.GetReport()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, rpt.GetUncommittedLLSNOffset())
	assert.EqualValues(t, 0, rpt.GetUncommittedLLSNLength())

	require.NoError(t, lse.SyncReplicate(context.Background(), snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            3,
			CommittedGLSNBegin: 3,
			CommittedGLSNEnd:   5,
			CommittedLLSNBegin: 3,
		},
	}))
	require.NoError(t, lse.SyncReplicate(context.Background(), snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{LLSN: 3, GLSN: 3},
		},
	}))
	require.NoError(t, lse.SyncReplicate(context.Background(), snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{LLSN: 4, GLSN: 4},
		},
	}))

	require.Equal(t, executorSealing, lse.stateBarrier.state.load())
}

func TestExecutorSyncPrimaryReplica(t *testing.T) {
	const topicID = 1

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithStorageNodeID(1),
		WithLogStreamID(1),
		WithTopicID(topicID),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
	}))
	require.True(t, lse.isPrimay())

	for i := 1; i <= 2; i++ {
		var wg sync.WaitGroup
		llsn := types.LLSN(i)
		glsn := types.GLSN(i)
		ver := types.Version(i)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
			assert.NoError(t, err)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.Eventually(t, func() bool {
				report, err := lse.GetReport()
				require.NoError(t, err)
				return report.UncommittedLLSNLength == 1
			}, time.Second, 10*time.Millisecond)

			assert.NoError(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
				Version:             ver,
				HighWatermark:       glsn,
				CommittedGLSNOffset: glsn,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: llsn,
			}))
			assert.Eventually(t, func() bool {
				report, err := lse.GetReport()
				assert.NoError(t, err)
				return report.Version == ver && report.UncommittedLLSNOffset == llsn+1 &&
					report.UncommittedLLSNLength == 0
			}, time.Second, 10*time.Millisecond)
		}()
		wg.Wait()
	}

	var wg sync.WaitGroup
	for i := 3; i <= 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")})
			assert.NoError(t, err)
		}()
	}

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		assert.NoError(t, err)
		return report.UncommittedLLSNOffset == 3 && report.UncommittedLLSNLength == 2
	}, time.Second, 10*time.Millisecond)

	// LLSN | GLSN
	//    1 |    1
	//    2 |    2
	//    3 |    -
	//    4 |    -

	status, _, err = lse.Seal(context.Background(), 4)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealing, status)

	last, err := lse.SyncInit(context.Background(), snpb.SyncRange{FirstLLSN: 1, LastLLSN: 4})
	require.NoError(t, err)
	require.Equal(t, snpb.SyncRange{FirstLLSN: 3, LastLLSN: 4}, last)
	// NOTE: This assertion checks private variable.
	require.Equal(t, executorLearning, lse.stateBarrier.state.load())

	// Replica in learning state should not accept commit request.
	assert.Error(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
		Version:             3,
		HighWatermark:       4,
		CommittedGLSNOffset: 3,
		CommittedGLSNLength: 2,
		CommittedLLSNOffset: 3,
	}))

	rpt, err := lse.GetReport()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, rpt.GetUncommittedLLSNOffset())
	assert.EqualValues(t, 0, rpt.GetUncommittedLLSNLength())

	require.NoError(t, lse.SyncReplicate(context.Background(), snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            3,
			CommittedGLSNBegin: 3,
			CommittedGLSNEnd:   5,
			CommittedLLSNBegin: 3,
		},
	}))
	require.NoError(t, lse.SyncReplicate(context.Background(), snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{LLSN: 3, GLSN: 3},
		},
	}))
	require.NoError(t, lse.SyncReplicate(context.Background(), snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{LLSN: 4, GLSN: 4},
		},
	}))
	require.Equal(t, executorSealing, lse.stateBarrier.state.load())
	wg.Wait()
}

func TestExecutorSync(t *testing.T) {
	const topicID = 1

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse, err := New(
		WithStorageNodeID(1),
		WithLogStreamID(1),
		WithTopicID(topicID),
		WithStorage(strg),
		WithMetrics(telemetry.NewMetrics()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lse.Close())
	}()

	dstClient := replication.NewMockClient(ctrl)
	dstClient.EXPECT().Close().Return(nil).AnyTimes()
	connector := replication.NewMockConnector(ctrl)
	connector.EXPECT().Get(gomock.Any(), gomock.Any()).Return(dstClient, nil).AnyTimes()

	rp := NewMockReplicator(ctrl)
	rp.EXPECT().send(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	rp.EXPECT().stop().Return().AnyTimes()
	rp.EXPECT().waitForDrainage(gomock.Any()).Return(nil).AnyTimes()
	rp.EXPECT().clientOf(gomock.Any(), gomock.Any()).Return(dstClient, nil).AnyTimes()
	rp.EXPECT().resetConnector().Return(nil).AnyTimes()
	lse.writer.(*writerImpl).replicator.stop()
	lse.rp = rp
	lse.writer.(*writerImpl).replicator = rp

	status, _, err := lse.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.NoError(t, lse.Unseal(context.TODO(), []varlogpb.Replica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
			LogStreamID: 1,
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 2,
			},
			LogStreamID: 1,
		},
	}))
	require.True(t, lse.isPrimay())
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse.Metadata().Status)

	require.NoError(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
		Version:             1,
		HighWatermark:       5,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 0,
		CommittedLLSNOffset: 1,
	}))

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		assert.NoError(t, err)
		return report.Version == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
		Version:             2,
		HighWatermark:       10,
		CommittedGLSNOffset: 1,
		CommittedGLSNLength: 0,
		CommittedLLSNOffset: 1,
	}))

	require.Eventually(t, func() bool {
		report, err := lse.GetReport()
		assert.NoError(t, err)
		return report.Version == 2
	}, time.Second, 10*time.Millisecond)

	for i := 1; i <= 2; i++ {
		llsn := types.LLSN(i)
		glsn := types.GLSN(10 + i)
		ver := types.Version(2 + i)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := lse.Append(context.Background(), [][]byte{[]byte("foo")}, varlogpb.Replica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: 2,
				},
				LogStreamID: 1,
			})
			assert.NoError(t, err)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.Eventually(t, func() bool {
				report, err := lse.GetReport()
				require.NoError(t, err)
				return report.UncommittedLLSNLength == 1
			}, time.Second, 10*time.Millisecond)

			assert.NoError(t, lse.Commit(context.Background(), snpb.LogStreamCommitResult{
				Version:             ver,
				HighWatermark:       glsn,
				CommittedGLSNOffset: glsn,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: llsn,
			}))
			assert.Eventually(t, func() bool {
				report, err := lse.GetReport()
				assert.NoError(t, err)
				return report.Version == ver && report.UncommittedLLSNOffset == llsn+1 &&
					report.UncommittedLLSNLength == 0
			}, time.Second, 10*time.Millisecond)
		}()
		wg.Wait()
	}

	// Type | LLSN | GLSN | HWM | PrevHWM | GLSNBegin | GLSNEnd | LLSNBegin
	//   cc |      |      |   5 |       0 |         1 |       1 |         1
	//   cc |      |      |  10 |       5 |         1 |       1 |         1
	// * cc |      |      |  11 |      10 |        11 |      12 |         1
	// * le |    1 |   11 |     |         |           |         |
	// * cc |      |      |  12 |      11 |        12 |      13 |         2
	// * le |    2 |   12 |     |         |           |         |

	done := make(chan struct{})
	dstClient.EXPECT().SyncInit(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, srcRnage snpb.SyncRange) (snpb.SyncRange, error) {
			select {
			default:
				return snpb.SyncRange{FirstLLSN: 1, LastLLSN: 2}, nil
			case <-done:
				return snpb.InvalidSyncRange(), errors.WithStack(verrors.ErrExist)
			}
		},
	).AnyTimes()

	step := 0
	expectedLLSN := types.LLSN(1)
	exptectedGLSN := types.GLSN(11)
	expectedVer := types.Version(3)
	dstClient.EXPECT().SyncReplicate(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, replica varlogpb.Replica, payload snpb.SyncPayload) error {
			defer func() {
				step++
			}()

			if step%2 == 0 { // cc
				assert.NotNil(t, payload.CommitContext)
				assert.Equal(t, expectedVer, payload.CommitContext.Version)
			} else { // le
				assert.NotNil(t, payload.LogEntry)
				assert.Equal(t, exptectedGLSN, payload.LogEntry.GLSN)
				assert.Equal(t, expectedLLSN, payload.LogEntry.LLSN)
				expectedLLSN++
				exptectedGLSN++
				expectedVer++
			}
			if expectedLLSN > types.LLSN(2) {
				close(done)
			}
			return nil
		},
	).Times(4)

	status, _, err = lse.Seal(context.TODO(), 12)
	require.NoError(t, err)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)

	require.Eventually(t, func() bool {
		sts, err := lse.Sync(context.Background(), varlogpb.Replica{})
		assert.NoError(t, err)
		return sts != nil && sts.State == snpb.SyncStateComplete
	}, time.Second, 10*time.Millisecond)
}
