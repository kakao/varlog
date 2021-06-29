package executor

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/pkg/verrors"
)

func TestCommitterConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()
	decider := newDecidableCondition(lsc)
	state := NewMockStateProvider(ctrl)

	var cfg committerConfig

	// okay
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.NoError(t, cfg.validate())

	// bad task queue size
	cfg = committerConfig{
		commitTaskQueueSize: 0,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// bad batch size
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 0,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// bad commit queue size
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     0,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// no storage
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                nil,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// no lsc
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 nil,
		decider:             decider,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// no decider
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 lsc,
		decider:             nil,
		state:               state,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// no state
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               nil,
		me:                  NewMockMeasurableExecutor(ctrl),
	}
	require.Error(t, cfg.validate())

	// no measurable
	cfg = committerConfig{
		commitTaskQueueSize: 1,
		commitTaskBatchSize: 1,
		commitQueueSize:     1,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
	}
	require.Error(t, cfg.validate())
}

func TestCommitterStop(t *testing.T) {
	const (
		commitTaskQueueSize = 128
		commitQueueSize     = 256
		committerBatchSize  = 128
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()
	decider := newDecidableCondition(lsc)
	state := NewMockStateProvider(ctrl)

	state.EXPECT().mutableWithBarrier().Return(nil)
	state.EXPECT().releaseBarrier().Return().Times(2)
	state.EXPECT().setSealing().Return()
	state.EXPECT().committableWithBarrier().Return(nil)

	// create committer
	committer, err := newCommitter(committerConfig{
		commitTaskQueueSize: commitTaskQueueSize,
		commitTaskBatchSize: committerBatchSize,
		commitQueueSize:     commitQueueSize,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)

	defer committer.stop()

	// send task
	twg := taskWaitGroup{}
	twg.wg.Add(1)
	cwt := newCommitWaitTask(1, &twg)
	// NOTE: cwt is released by commiter.

	err = committer.sendCommitWaitTask(context.TODO(), cwt)
	require.NoError(t, err)
	lsc.uncommittedLLSNEnd.Add(1)

	err = committer.sendCommitTask(context.TODO(), &commitTask{
		highWatermark:      1,
		prevHighWatermark:  0,
		committedGLSNBegin: 1,
		committedGLSNEnd:   2,
		committedLLSNBegin: 1,
	})
	require.NoError(t, err)

	twg.wg.Wait()
	require.NoError(t, twg.err)
	require.EqualValues(t, 1, twg.glsn)

	// commitLoop works
	require.Eventually(t, func() bool {
		return committer.commitWaitQ.size() == 0 && committer.commitTaskQ.size() == 0
	}, time.Second, 10*time.Millisecond)
}

func TestCommitter(t *testing.T) {
	const (
		commitTaskQueueSize = 128
		commitQueueSize     = 256
		committerBatchSize  = 128
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()
	decider := newDecidableCondition(lsc)
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
	state.EXPECT().setSealing().DoAndReturn(func() {
		sealed.Store(true)
	}).AnyTimes()
	state.EXPECT().committableWithBarrier().Return(nil).AnyTimes()

	// create committer
	committer, err := newCommitter(committerConfig{
		commitTaskQueueSize: commitTaskQueueSize,
		commitTaskBatchSize: committerBatchSize,
		commitQueueSize:     commitQueueSize,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)

	// write logs, LLSN=[1, 10]
	for i := 1; i <= 10; i++ {
		// Because twg is nil, it is handled like backup replication.
		cwt := newCommitWaitTask(types.LLSN(i), nil)
		err := committer.sendCommitWaitTask(context.TODO(), cwt)
		require.NoError(t, err)
		lsc.uncommittedLLSNEnd.Add(1)
	}

	require.Equal(t, 10, committer.commitWaitQ.size())

	// commit,
	err = committer.sendCommitTask(context.TODO(), &commitTask{
		highWatermark:      2,
		prevHighWatermark:  0,
		committedGLSNBegin: 1,
		committedGLSNEnd:   3,
		committedLLSNBegin: 1,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return committer.commitWaitQ.size() == 8
	}, time.Second, 10*time.Millisecond)

	// sealing
	state.setSealing()

	// sending new task into commitQ should fail.
	cwt := newCommitWaitTask(11, nil)
	err = committer.sendCommitWaitTask(context.TODO(), cwt)
	require.Error(t, err)

	// sealed
	committer.drainCommitWaitQ(verrors.ErrSealed)
	lsc.uncommittedLLSNEnd.Store(3)
	sealed.Store(false)

	// write new logs, LLSN=[3,4]
	for i := 3; i <= 4; i++ {
		cwt := newCommitWaitTask(types.LLSN(i), nil)
		err := committer.sendCommitWaitTask(context.TODO(), cwt)
		require.NoError(t, err)
		lsc.uncommittedLLSNEnd.Add(1)
	}

	// commit,
	err = committer.sendCommitTask(context.TODO(), &commitTask{
		highWatermark:      4,
		prevHighWatermark:  2,
		committedGLSNBegin: 3,
		committedGLSNEnd:   5,
		committedLLSNBegin: 3,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return committer.commitWaitQ.size() == 0
	}, time.Second, 10*time.Millisecond)

	committer.stop()
}

func TestCommitterCatchupCommitVarlog459(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		commitTaskQueueSize = 1000
		commitQueueSize     = 1000
		committerBatchSize  = 1000
		goal                = 100
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()
	decider := newDecidableCondition(lsc)
	state := NewMockStateProvider(ctrl)

	state.EXPECT().mutableWithBarrier().Return(nil).AnyTimes()
	state.EXPECT().releaseBarrier().Return().AnyTimes()
	state.EXPECT().setSealing().Return().AnyTimes()
	state.EXPECT().committableWithBarrier().Return(nil).AnyTimes()

	// create committer
	committer, err := newCommitter(committerConfig{
		commitTaskQueueSize: commitTaskQueueSize,
		commitTaskBatchSize: committerBatchSize,
		commitQueueSize:     commitQueueSize,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)
	defer committer.stop()

	require.Zero(t, committer.commitTaskQ.size())

	for i := 0; i < goal; i++ {
		committer.sendCommitTask(context.Background(), &commitTask{
			highWatermark:      types.GLSN(i + 1),
			prevHighWatermark:  types.GLSN(i),
			committedGLSNBegin: types.MinGLSN,
			committedGLSNEnd:   types.MinGLSN,
			committedLLSNBegin: types.MinLLSN,
		})
		if i > 0 {
			committer.sendCommitTask(context.Background(), &commitTask{
				highWatermark:      types.GLSN(i),
				prevHighWatermark:  types.GLSN(i - 1),
				committedGLSNBegin: types.MinGLSN,
				committedGLSNEnd:   types.MinGLSN,
				committedLLSNBegin: types.MinLLSN,
			})
		}
	}

	require.Eventually(t, func() bool {
		return committer.commitTaskQ.size() == 0
	}, 5*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		hwm, _ := lsc.reportCommitBase()
		return hwm == goal
	}, 5*time.Second, 10*time.Millisecond)
}

func TestCommitterState(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		commitTaskQueueSize = 16
		commitQueueSize     = 16
		committerBatchSize  = 16
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg := newTestStorage(ctrl, newTestStorageConfig())
	lsc := newLogStreamContext()
	decider := newDecidableCondition(lsc)
	state := NewMockStateProvider(ctrl)

	// create committer
	committer, err := newCommitter(committerConfig{
		commitTaskQueueSize: commitTaskQueueSize,
		commitTaskBatchSize: committerBatchSize,
		commitQueueSize:     commitQueueSize,
		strg:                strg,
		lsc:                 lsc,
		decider:             decider,
		state:               state,
		me:                  NewTestMeasurableExecutor(ctrl, 1, 1),
	})
	require.NoError(t, err)
	defer committer.stop()

	state.EXPECT().setSealing().Return().AnyTimes()

	// check initial conditions
	require.Zero(t, committer.commitTaskQ.size())
	require.Zero(t, committer.inflightCommitTasks.cnt)
	require.Zero(t, committer.commitWaitQ.size())

	// state == mutable
	// - writing new log entries are enabled.
	// - committing written logs are enabled.
	state.EXPECT().mutableWithBarrier().Return(nil).Times(2)
	state.EXPECT().releaseBarrier().Return().Times(2)
	state.EXPECT().committableWithBarrier().Return(nil)
	state.EXPECT().releaseBarrier().Return()

	// push commitWaitTask
	cwt := newCommitWaitTask(1, nil)
	require.NoError(t, committer.sendCommitWaitTask(context.Background(), cwt))
	lsc.uncommittedLLSNEnd.Add(1)

	// #commitWaitTasks = 1
	// TODO (jun): add assertion for inflightCommitWaitTasks
	require.EqualValues(t, 1, committer.commitWaitQ.size())

	// push commitWaitTask
	cwt = newCommitWaitTask(2, nil)
	require.NoError(t, committer.sendCommitWaitTask(context.Background(), cwt))
	lsc.uncommittedLLSNEnd.Add(1)

	// #commitWaitTasks = 2
	// TODO (jun): add assertion for inflightCommitWaitTasks
	require.EqualValues(t, 2, committer.commitWaitQ.size())

	// push commitTask
	require.NoError(t, committer.sendCommitTask(context.Background(), &commitTask{
		highWatermark:      1,
		prevHighWatermark:  0,
		committedGLSNBegin: 1,
		committedGLSNEnd:   2,
		committedLLSNBegin: 1,
	}))

	// committed
	require.Eventually(t, func() bool {
		hwm, _ := lsc.reportCommitBase()
		return committer.commitWaitQ.size() == 1 && hwm == 1
	}, time.Second, 10*time.Millisecond)

	// state == sealing
	// - writing new log entries are disabled.
	// - committing written logs are enabled.
	state.EXPECT().mutableWithBarrier().Return(verrors.ErrSealed)
	state.EXPECT().committableWithBarrier().Return(nil)
	state.EXPECT().releaseBarrier().Return()

	cwt = newCommitWaitTask(3, nil)
	require.Error(t, committer.sendCommitWaitTask(context.Background(), cwt))

	require.NoError(t, committer.sendCommitTask(context.Background(), &commitTask{
		highWatermark:      2,
		prevHighWatermark:  1,
		committedGLSNBegin: 2,
		committedGLSNEnd:   3,
		committedLLSNBegin: 2,
	}))

	// committed
	require.Eventually(t, func() bool {
		hwm, _ := lsc.reportCommitBase()
		return committer.commitWaitQ.size() == 0 && hwm == 2
	}, time.Second, 10*time.Millisecond)

	// state == learning | sealed
	// - writing new log entries are disabled.
	// - committing written logs are disabled.
	state.EXPECT().mutableWithBarrier().Return(verrors.ErrSealed)
	state.EXPECT().committableWithBarrier().Return(verrors.ErrInvalid)

	cwt = newCommitWaitTask(3, nil)
	require.Error(t, committer.sendCommitWaitTask(context.Background(), cwt))

	require.Error(t, committer.sendCommitTask(context.Background(), &commitTask{
		highWatermark:      3,
		prevHighWatermark:  2,
		committedGLSNBegin: 3,
		committedGLSNEnd:   3,
		committedLLSNBegin: 3,
	}))
}
