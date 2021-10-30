// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/kakao/varlog/pkg/varlog (interfaces: Varlog)

// Package varlog is a generated GoMock package.
package varlog

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"

	types "github.com/kakao/varlog/pkg/types"
)

// MockVarlog is a mock of Varlog interface.
type MockVarlog struct {
	ctrl     *gomock.Controller
	recorder *MockVarlogMockRecorder
}

// MockVarlogMockRecorder is the mock recorder for MockVarlog.
type MockVarlogMockRecorder struct {
	mock *MockVarlog
}

// NewMockVarlog creates a new mock instance.
func NewMockVarlog(ctrl *gomock.Controller) *MockVarlog {
	mock := &MockVarlog{ctrl: ctrl}
	mock.recorder = &MockVarlogMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockVarlog) EXPECT() *MockVarlogMockRecorder {
	return m.recorder
}

// Append mocks base method.
func (m *MockVarlog) Append(arg0 context.Context, arg1 types.TopicID, arg2 []byte, arg3 ...AppendOption) (types.GLSN, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2}
	for _, a := range arg3 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Append", varargs...)
	ret0, _ := ret[0].(types.GLSN)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Append indicates an expected call of Append.
func (mr *MockVarlogMockRecorder) Append(arg0, arg1, arg2 interface{}, arg3 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2}, arg3...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Append", reflect.TypeOf((*MockVarlog)(nil).Append), varargs...)
}

// AppendTo mocks base method.
func (m *MockVarlog) AppendTo(arg0 context.Context, arg1 types.TopicID, arg2 types.LogStreamID, arg3 []byte, arg4 ...AppendOption) (types.GLSN, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3}
	for _, a := range arg4 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AppendTo", varargs...)
	ret0, _ := ret[0].(types.GLSN)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AppendTo indicates an expected call of AppendTo.
func (mr *MockVarlogMockRecorder) AppendTo(arg0, arg1, arg2, arg3 interface{}, arg4 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3}, arg4...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendTo", reflect.TypeOf((*MockVarlog)(nil).AppendTo), varargs...)
}

// Close mocks base method.
func (m *MockVarlog) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockVarlogMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockVarlog)(nil).Close))
}

// Read mocks base method.
func (m *MockVarlog) Read(arg0 context.Context, arg1 types.TopicID, arg2 types.LogStreamID, arg3 types.GLSN) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockVarlogMockRecorder) Read(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockVarlog)(nil).Read), arg0, arg1, arg2, arg3)
}

// Subscribe mocks base method.
func (m *MockVarlog) Subscribe(arg0 context.Context, arg1 types.TopicID, arg2, arg3 types.GLSN, arg4 OnNext, arg5 ...SubscribeOption) (SubscribeCloser, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1, arg2, arg3, arg4}
	for _, a := range arg5 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Subscribe", varargs...)
	ret0, _ := ret[0].(SubscribeCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockVarlogMockRecorder) Subscribe(arg0, arg1, arg2, arg3, arg4 interface{}, arg5 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1, arg2, arg3, arg4}, arg5...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockVarlog)(nil).Subscribe), varargs...)
}

// Trim mocks base method.
func (m *MockVarlog) Trim(arg0 context.Context, arg1 types.TopicID, arg2 types.GLSN, arg3 TrimOption) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Trim", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// Trim indicates an expected call of Trim.
func (mr *MockVarlogMockRecorder) Trim(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Trim", reflect.TypeOf((*MockVarlog)(nil).Trim), arg0, arg1, arg2, arg3)
}