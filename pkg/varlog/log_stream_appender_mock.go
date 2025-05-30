// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/kakao/varlog/pkg/varlog (interfaces: LogStreamAppender)
//
// Generated by this command:
//
//	mockgen -package varlog -destination log_stream_appender_mock.go . LogStreamAppender
//

// Package varlog is a generated GoMock package.
package varlog

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockLogStreamAppender is a mock of LogStreamAppender interface.
type MockLogStreamAppender struct {
	ctrl     *gomock.Controller
	recorder *MockLogStreamAppenderMockRecorder
	isgomock struct{}
}

// MockLogStreamAppenderMockRecorder is the mock recorder for MockLogStreamAppender.
type MockLogStreamAppenderMockRecorder struct {
	mock *MockLogStreamAppender
}

// NewMockLogStreamAppender creates a new mock instance.
func NewMockLogStreamAppender(ctrl *gomock.Controller) *MockLogStreamAppender {
	mock := &MockLogStreamAppender{ctrl: ctrl}
	mock.recorder = &MockLogStreamAppenderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockLogStreamAppender) EXPECT() *MockLogStreamAppenderMockRecorder {
	return m.recorder
}

// AppendBatch mocks base method.
func (m *MockLogStreamAppender) AppendBatch(dataBatch [][]byte, callback BatchCallback) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AppendBatch", dataBatch, callback)
	ret0, _ := ret[0].(error)
	return ret0
}

// AppendBatch indicates an expected call of AppendBatch.
func (mr *MockLogStreamAppenderMockRecorder) AppendBatch(dataBatch, callback any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AppendBatch", reflect.TypeOf((*MockLogStreamAppender)(nil).AppendBatch), dataBatch, callback)
}

// Close mocks base method.
func (m *MockLogStreamAppender) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockLogStreamAppenderMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockLogStreamAppender)(nil).Close))
}
