package executor

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/verrors"
)

type testCommitter struct {
	mock           *MockCommitter
	incommingLLSNs []types.LLSN
	mu             sync.Mutex
}

func newTestCommitter(ctrl *gomock.Controller) *testCommitter {
	cmt := &testCommitter{}
	cmt.mock = NewMockCommitter(ctrl)
	cmt.mock.EXPECT().sendCommitWaitTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, cwt *commitWaitTask) error {
			defer cwt.twg.wg.Done()

			cmt.mu.Lock()
			defer cmt.mu.Unlock()
			cmt.incommingLLSNs = append(cmt.incommingLLSNs, cwt.llsn)
			return nil
		},
	).AnyTimes()
	return cmt
}

type testReplicator struct {
	mock *MockReplicator
	rts  []*replicateTask
	mu   sync.Mutex
}

func newTestReplicator(ctrl *gomock.Controller) *testReplicator {
	ret := &testReplicator{}
	ret.mock = NewMockReplicator(ctrl)
	ret.mock.EXPECT().send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, rtb *replicateTask) error {
			ret.mu.Lock()
			defer ret.mu.Unlock()
			ret.rts = append(ret.rts, rtb)
			return nil
		},
	).AnyTimes()
	return ret
}

func TestWriterInvalidArgument(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := storage.NewMockStorage(ctrl)
	lsc := newLogStreamContext()
	committer := NewMockCommitter(ctrl)
	replicator := NewMockReplicator(ctrl)
	state := NewMockStateProvider(ctrl)

	var err error

	// bad queueSize
	_, err = newWriter(writerConfig{
		batchSize:  1,
		strg:       strg,
		lsc:        lsc,
		committer:  committer,
		replicator: replicator,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)

	// bad batchSize
	_, err = newWriter(writerConfig{
		queueSize:  1,
		strg:       strg,
		lsc:        lsc,
		committer:  committer,
		replicator: replicator,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)

	// bad storage
	_, err = newWriter(writerConfig{
		queueSize:  1,
		batchSize:  1,
		lsc:        lsc,
		committer:  committer,
		replicator: replicator,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)

	// bad log stream context
	_, err = newWriter(writerConfig{
		queueSize:  1,
		batchSize:  1,
		strg:       strg,
		committer:  committer,
		replicator: replicator,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)

	// bad committer
	_, err = newWriter(writerConfig{
		queueSize:  1,
		batchSize:  1,
		strg:       strg,
		lsc:        lsc,
		replicator: replicator,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)

	// bad replicator
	_, err = newWriter(writerConfig{
		queueSize: 1,
		batchSize: 1,
		strg:      strg,
		lsc:       lsc,
		committer: committer,
		state:     state,
		me:        NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)

	// bad state provider
	_, err = newWriter(writerConfig{
		queueSize:  1,
		batchSize:  1,
		strg:       strg,
		lsc:        lsc,
		committer:  committer,
		replicator: replicator,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.Error(t, err)
}

func TestWriterStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()

	testCommitter := newTestCommitter(ctrl)
	testReplicator := newTestReplicator(ctrl)
	state := NewMockStateProvider(ctrl)

	state.EXPECT().mutableWithBarrier().Return(nil).AnyTimes()
	state.EXPECT().releaseBarrier().Return().AnyTimes()
	state.EXPECT().mutable().Return(nil).AnyTimes()

	// create writer
	writer, err := newWriter(writerConfig{
		queueSize:  1,
		batchSize:  1,
		strg:       strg,
		lsc:        lsc,
		committer:  testCommitter.mock,
		replicator: testReplicator.mock,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)

	// add new task
	twg := newTaskWaitGroup()
	defer twg.release()
	wt := newPrimaryWriteTask(twg, nil, nil)
	wt.validate = func() error { return nil }
	defer wt.release()

	err = writer.send(context.TODO(), wt)
	require.NoError(t, err)

	// wait for transferring the task to committer and replicator
	require.Eventually(t, func() bool {
		testCommitter.mu.Lock()
		defer testCommitter.mu.Unlock()
		return len(testCommitter.incommingLLSNs) == 1 && testCommitter.incommingLLSNs[0] == 1
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		testReplicator.mu.Lock()
		defer testReplicator.mu.Unlock()
		return len(testReplicator.rts) == 1 && testReplicator.rts[0].llsn == 1
	}, time.Second, 10*time.Millisecond)

	// stop
	state.EXPECT().setSealingWithReason(gomock.Any()).Return().AnyTimes()
	writer.stop()
	require.Equal(t, 0, writer.q.size())

	// stopping already stopped writer is okay
	writer.stop()
	require.Equal(t, 0, writer.q.size())
}

func TestWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 1024
		batchSize = 128
		numProd   = 64
		numSeal   = 1
	)

	probs := []struct {
		putFailProb   float64
		applyFailProb float64
	}{
		{
			putFailProb:   0,
			applyFailProb: 0,
		},
		{
			putFailProb:   0.01,
			applyFailProb: 0.01,
		},
	}

	rand.Seed(time.Now().Unix())

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, prob := range probs {
		lsc := newLogStreamContext()

		var (
			writeBatchLen     = 0
			writeBatchLLSNEnd = lsc.uncommittedLLSNEnd.Load()
		)

		writeBatch := storage.NewMockWriteBatch(ctrl)
		writeBatch.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(
			func(llsn types.LLSN, data []byte) error {
				if rand.Float64() < prob.putFailProb {
					writeBatchLen = 0
					return errors.New("fake error")
				}
				require.Equal(t, writeBatchLLSNEnd+types.LLSN(writeBatchLen), llsn)
				writeBatchLen++
				return nil
			},
		).AnyTimes()
		writeBatch.EXPECT().Apply().DoAndReturn(func() error {
			defer func() {
				writeBatchLen = 0
			}()
			if rand.Float64() < prob.applyFailProb {
				return errors.New("fake error")
			}
			writeBatchLLSNEnd += types.LLSN(writeBatchLen)
			return nil
		}).AnyTimes()
		writeBatch.EXPECT().Close().Return(nil).AnyTimes()

		strg := storage.NewMockStorage(ctrl)
		strg.EXPECT().NewWriteBatch().Return(writeBatch).AnyTimes()

		testCommitter := newTestCommitter(ctrl)
		testReplicator := newTestReplicator(ctrl)
		state := NewMockStateProvider(ctrl)

		var sealed atomicutil.AtomicBool
		sealed.Store(false)
		state.EXPECT().mutableWithBarrier().DoAndReturn(func() error {
			if sealed.Load() {
				return verrors.ErrSealed
			}
			return nil
		}).AnyTimes()
		state.EXPECT().releaseBarrier().Return().AnyTimes()
		state.EXPECT().mutable().DoAndReturn(func() error {
			if sealed.Load() {
				return verrors.ErrSealed
			}
			return nil
		}).AnyTimes()
		state.EXPECT().setSealingWithReason(gomock.Any()).DoAndReturn(func(_ error) {
			sealed.Store(true)
		}).AnyTimes()

		// create writer
		writer, err := newWriter(writerConfig{
			queueSize:  queueSize,
			batchSize:  batchSize,
			strg:       strg,
			lsc:        lsc,
			committer:  testCommitter.mock,
			replicator: testReplicator.mock,
			state:      state,
			me:         NewTestMeasurableExecutor(ctrl, 1, 1),
		})
		require.NoError(t, err)

		for k := 0; k < numSeal; k++ {
			// send tasks to writer concurrently
			prodWg := sync.WaitGroup{}
			for i := 0; i < numProd; i++ {
				prodID := i + 1
				prodWg.Add(1)
				go func() {
					numSent := 0
					defer func() {
						t.Logf("producer-%d sent %d tasks", prodID, numSent)
						prodWg.Done()
					}()
					for {
						// NOTE: twg and t do not call release method, it is
						// okay.
						twg := newTaskWaitGroup()
						t := newPrimaryWriteTask(twg, nil, nil)
						t.validate = func() error { return nil }
						if err := writer.send(context.TODO(), t); err != nil {
							return
						}
						numSent++
					}
				}()
			}

			// seal LSE
			sealWg := sync.WaitGroup{}
			sealWg.Add(1)
			go func() {
				defer sealWg.Done()
				time.Sleep(100 * time.Millisecond)
				state.setSealingWithReason(nil)
			}()

			sealWg.Wait()
			prodWg.Wait()

			// writeQ should be empty.
			require.Eventually(t, func() bool {
				return writer.q.size() == 0
			}, time.Second, 10*time.Millisecond)

			// unseal
			sealed.Store(false)
		}

		writer.stop()
		require.Equal(t, 0, writer.q.size())
	}
}

func TestWriterCleanup(t *testing.T) {
	const (
		numSender = 1000

		queueSize = 1
		batchSize = 1
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()
	testCommitter := newTestCommitter(ctrl)
	testReplicator := newTestReplicator(ctrl)
	state := NewMockStateProvider(ctrl)
	state.EXPECT().mutableWithBarrier().Return(nil).AnyTimes()
	state.EXPECT().releaseBarrier().Return().AnyTimes()
	state.EXPECT().mutable().Return(nil).AnyTimes()
	state.EXPECT().setSealingWithReason(gomock.Any()).Return().AnyTimes()

	// create writer
	writer, err := newWriter(writerConfig{
		queueSize:  queueSize,
		batchSize:  batchSize,
		strg:       strg,
		lsc:        lsc,
		committer:  testCommitter.mock,
		replicator: testReplicator.mock,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)

	sendWg := sync.WaitGroup{}
	for i := 0; i < numSender; i++ {
		sendWg.Add(1)
		go func() {
			defer sendWg.Done()

			twg := newTaskWaitGroup()
			wt := newPrimaryWriteTask(twg, nil, nil)
			wt.validate = func() error { return nil }
			defer wt.release()

			if err := writer.send(context.TODO(), wt); err != nil {
				twg.wg.Done()
			}
			twg.wg.Wait()
		}()
	}
	time.Sleep(100 * time.Millisecond)
	writer.stop()
	sendWg.Wait()
}

func TestWriterVarlog444(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 1
		batchSize = 1
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lsc := newLogStreamContext()
	testCommitter := NewMockCommitter(ctrl)
	testReplicator := newTestReplicator(ctrl)
	state := NewMockStateProvider(ctrl)
	state.EXPECT().mutableWithBarrier().Return(nil).AnyTimes()
	state.EXPECT().releaseBarrier().Return().AnyTimes()
	state.EXPECT().mutable().Return(nil).AnyTimes()
	state.EXPECT().setSealingWithReason(gomock.Any()).Return().AnyTimes()

	// create writer
	writer, err := newWriter(writerConfig{
		queueSize:  queueSize,
		batchSize:  batchSize,
		strg:       strg,
		lsc:        lsc,
		committer:  testCommitter,
		replicator: testReplicator.mock,
		state:      state,
		me:         NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)

	defer func() {
		writer.stop()
		require.NoError(t, strg.Close())
	}()

	// initial state
	require.Equal(t, lsc.uncommittedLLSNEnd.Load(), types.MinLLSN)

	var wg sync.WaitGroup

	// LLSN=1, Write=OK
	testCommitter.EXPECT().sendCommitWaitTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, cwt *commitWaitTask) error {
			defer cwt.twg.wg.Done()
			return nil
		},
	).Times(1)

	wg.Add(1)
	go func() {
		defer wg.Done()

		twg := newTaskWaitGroup()
		wt := newPrimaryWriteTask(twg, nil, nil)
		wt.validate = func() error { return nil }
		defer wt.release()

		if !assert.NoError(t, writer.send(context.Background(), wt)) {
			twg.wg.Done()
		}
		twg.wg.Wait()
		assert.NoError(t, twg.err)
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		return lsc.uncommittedLLSNEnd.Load() == types.LLSN(2)
	}, time.Second, 10*time.Millisecond)

	// LLSN=2, Write=OK, SendToCommit=Fail
	testCommitter.EXPECT().sendCommitWaitTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *commitWaitTask) error {
			return errors.New("fake")
		},
	).Times(2)

	wg.Add(1)
	go func() {
		defer wg.Done()

		twg := newTaskWaitGroup()
		wt := newPrimaryWriteTask(twg, nil, nil)
		wt.validate = func() error { return nil }
		defer wt.release()

		if !assert.NoError(t, writer.send(context.Background(), wt)) {
			twg.wg.Done()
			return
		}
		twg.wg.Wait()
		assert.Error(t, twg.err)
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		return lsc.uncommittedLLSNEnd.Load() == types.LLSN(3)
	}, time.Second, 10*time.Millisecond)

	// LLSN=3, Write=OK, SendToCommit=Fail
	wg.Add(1)
	go func() {
		defer wg.Done()

		twg := newTaskWaitGroup()
		wt := newPrimaryWriteTask(twg, nil, nil)
		wt.validate = func() error { return nil }
		defer wt.release()

		if !assert.NoError(t, writer.send(context.Background(), wt)) {
			twg.wg.Done()
		}
		twg.wg.Wait()
		assert.Error(t, twg.err)
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		return lsc.uncommittedLLSNEnd.Load() == types.LLSN(4)
	}, time.Second, 10*time.Millisecond)
}
