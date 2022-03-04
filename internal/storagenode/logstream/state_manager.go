package logstream

import "sync/atomic"

type executorState int8

const (
	executorStateSealing executorState = iota
	executorStateSealed
	executorStateLearning
	executorStateAppendable
	executorStateClosed
)

type executorStateManager struct {
	state atomic.Value
}

func newExecutorStateManager(initState executorState) *executorStateManager {
	esm := new(executorStateManager)
	esm.state.Store(initState)
	return esm
}

func (esm *executorStateManager) load() executorState {
	return esm.state.Load().(executorState)
}

func (esm *executorStateManager) store(state executorState) {
	esm.state.Store(state)
}

func (esm *executorStateManager) compareAndSwap(oldState, newState executorState) bool {
	return esm.state.CompareAndSwap(oldState, newState)
}
