package executor

//go:generate mockgen -self_package github.com/kakao/varlog/internal/storagenode_deprecated/executor -package executor -source state.go -destination state_mock.go -mock_names stateProvider=MockStateProvider

import (
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/verrors"
)

type executorState int32

const (
	executorMutable executorState = iota + 1
	executorSealing
	executorLearning
	executorSealed
)

type atomicExecutorState executorState

func (aes *atomicExecutorState) load() executorState {
	return executorState(atomic.LoadInt32((*int32)(aes)))
}

func (aes *atomicExecutorState) store(state executorState) {
	atomic.StoreInt32((*int32)(aes), int32(state))
}

func (aes *atomicExecutorState) compareAndSwap(oldState, newState executorState) (swapped bool) {
	return atomic.CompareAndSwapInt32((*int32)(aes), int32(oldState), int32(newState))
}

type stateProvider interface {
	mutable() error
	mutableWithBarrier() error
	committableWithBarrier() error
	releaseBarrier()
	setSealing()
	setSealingWithReason(reason error)
}

func (e *executor) mutable() error {
	state := e.stateBarrier.state.load()
	if state == executorMutable {
		return nil
	}
	e.stateBarrier.muReasonToSeal.RLock()
	defer e.stateBarrier.muReasonToSeal.RUnlock()
	return errors.Wrapf(verrors.ErrSealed, "reason: %+v", e.stateBarrier.reasonToSeal)
}

func (e *executor) committable() error {
	state := e.stateBarrier.state.load()
	if state == executorMutable || state == executorSealing {
		return nil
	}
	return errors.Wrapf(verrors.ErrInvalid, "not committable state (%+v)", state)
}

func (e *executor) mutableWithBarrier() error {
	e.stateBarrier.lock.RLock()
	if err := e.mutable(); err != nil {
		e.stateBarrier.lock.RUnlock()
		return err
	}
	return nil
}

func (e *executor) committableWithBarrier() error {
	e.stateBarrier.lock.RLock()
	if err := e.committable(); err != nil {
		e.stateBarrier.lock.RUnlock()
		return err
	}
	return nil
}

func (e *executor) releaseBarrier() {
	e.stateBarrier.lock.RUnlock()
}

func (e *executor) setSealing() {
	e.stateBarrier.lock.Lock()
	defer e.stateBarrier.lock.Unlock()
	e.stateBarrier.state.compareAndSwap(executorMutable, executorSealing)
}

func (e *executor) setSealingWithReason(reason error) {
	e.stateBarrier.lock.Lock()
	defer e.stateBarrier.lock.Unlock()
	if e.stateBarrier.state.compareAndSwap(executorMutable, executorSealing) {
		if reason == nil {
			reason = errors.New("unknown")
		}
		e.stateBarrier.muReasonToSeal.Lock()
		defer e.stateBarrier.muReasonToSeal.Unlock()
		e.stateBarrier.reasonToSeal = reason
	}
}

func (e *executor) setLearning() bool {
	e.stateBarrier.lock.Lock()
	defer e.stateBarrier.lock.Unlock()
	return e.stateBarrier.state.compareAndSwap(executorSealing, executorLearning)
}
