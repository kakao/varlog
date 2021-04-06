package executor

//go:generate mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/executor -package executor -source state.go -destination state_mock.go -mock_names stateProvider=MockStateProvider

import (
	"sync/atomic"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

type executorState int32

const (
	executorMutable executorState = iota
	executorSealing
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
	releaseBarrier()
	setSealing()
}

func (e *executor) mutable() error {
	state := e.stateBarrier.state.load()
	if state == executorMutable {
		return nil
	}
	return errors.WithStack(verrors.ErrSealed)
}

func (e *executor) mutableWithBarrier() error {
	e.stateBarrier.lock.RLock()
	if err := e.mutable(); err != nil {
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
