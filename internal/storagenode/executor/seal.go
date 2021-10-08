package executor

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

type SealUnsealer interface {
	Seal(ctx context.Context, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error)
	Unseal(ctx context.Context, replicas []varlogpb.Replica) error
}

func (e *executor) Seal(ctx context.Context, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	if err := e.guard(); err != nil {
		return varlogpb.LogStreamStatusRunning, types.InvalidGLSN, err
	}
	defer e.unguard()

	e.muState.Lock()
	defer e.muState.Unlock()

	return e.seal(ctx, lastCommittedGLSN)
}

func (e *executor) seal(ctx context.Context, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	// TODO: need lock to run setSealed not to run concurrently
	// use lock or singleflight in Seal API
	//
	e.setSealingWithReason(errors.New("seal rpc"))

	if e.stateBarrier.state.load() == executorSealed {
		// TODO: need check localHWM == lastCommittedGLSN ?
		return varlogpb.LogStreamStatusSealed, lastCommittedGLSN, nil
	}

	// NOTE: writeQ, commitTaskQ, replicateQ should not be increased, since executor is
	// immutable status. To prevent each queue from pushing tasks in an immutable executor, use
	// `mutableWithBarrier` to check whether the executor is mutable before pushing.

	localHighWatermark := e.lsc.localGLSN.localHighWatermark.Load()
	if localHighWatermark > lastCommittedGLSN {
		// bad seal request
		panic("MR may be behind of LSE")
	}
	if localHighWatermark < lastCommittedGLSN {
		// sealing
		e.tsp.Touch()
		return varlogpb.LogStreamStatusSealing, localHighWatermark, nil
	}

	// NB: localHighWatermark == lastCommittedGLSN == InvalidGLSN
	// LSE has no logs written either committed, and it will be sealed.
	if lastCommittedGLSN.Invalid() {
		return e.sealInternal(lastCommittedGLSN)
	}

	lastLE, err := e.storage.Read(localHighWatermark)
	if err != nil {
		panic("unreadable last log entry")
	}
	lastCommittedLLSN := lastLE.LLSN

	if err := e.resetInternalState(ctx, lastCommittedLLSN, lastCommittedGLSN, true); err != nil {
		return varlogpb.LogStreamStatusSealing, lastCommittedGLSN, err
	}

	return e.sealInternal(lastCommittedGLSN)
}

func (e *executor) sealInternal(lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	e.stateBarrier.lock.Lock()
	defer e.stateBarrier.lock.Unlock()
	e.stateBarrier.state.store(executorSealed)
	e.tsp.Touch()
	return varlogpb.LogStreamStatusSealed, lastCommittedGLSN, nil
}

func (e *executor) Unseal(_ context.Context, replicas []varlogpb.Replica) error {
	if err := varlogpb.ValidReplicas(replicas); err != nil {
		return err
	}

	if err := e.guard(); err != nil {
		return err
	}
	defer e.unguard()

	e.stateBarrier.lock.Lock()
	defer e.stateBarrier.lock.Unlock()

	found := false
	for _, replica := range replicas {
		if replica.StorageNode.StorageNodeID == e.storageNodeID && replica.LogStreamID == e.logStreamID {
			found = true
			break
		}
	}
	if !found {
		return errors.Wrap(verrors.ErrInvalid, "no replica")
	}

	if !e.stateBarrier.state.compareAndSwap(executorSealed, executorMutable) {
		// FIXME: error type and message
		return errors.Wrap(verrors.ErrInvalid, "state not ready")
	}
	e.tsp.Touch()
	e.primaryBackups = replicas
	return nil
}

// resetInternalState resets internal components of executor when it becomes SEALED or LEARNING.
func (e *executor) resetInternalState(ctx context.Context, lastCommittedLLSN types.LLSN, lastCommittedGLSN types.GLSN, discardCommitWaitTasks bool) error {
	// reset writer
	if err := e.writer.waitForDrainage(ctx); err != nil {
		return err
	}

	// reset replicator and its connector
	if err := e.rp.resetConnector(); err != nil {
		return err
	}
	if err := e.rp.waitForDrainage(ctx); err != nil {
		return err
	}

	// reset committer
	if err := e.committer.waitForDrainageOfCommitTasks(ctx); err != nil {
		return err
	}
	if discardCommitWaitTasks {
		e.committer.drainCommitWaitQ(errors.WithStack(verrors.ErrSealed))
	}

	// remove uncommitted logs
	if err := e.storage.DeleteUncommitted(lastCommittedLLSN + 1); err != nil {
		return err
	}

	// reset storage state
	e.storage.RestoreStorage(lastCommittedLLSN, lastCommittedLLSN, lastCommittedGLSN)

	// reset log stream context
	e.lsc.uncommittedLLSNEnd.Store(lastCommittedLLSN + 1)

	return nil
}
