// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/kakao/varlog/proto/mrpb (interfaces: ManagementClient,ManagementServer,MetadataRepositoryServiceClient,MetadataRepositoryServiceServer)
//
// Generated by this command:
//
//	mockgen -build_flags -mod=vendor -package mock -destination mock/mrpb_mock.go . ManagementClient,ManagementServer,MetadataRepositoryServiceClient,MetadataRepositoryServiceServer
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	types "github.com/gogo/protobuf/types"
	gomock "go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"

	mrpb "github.com/kakao/varlog/proto/mrpb"
)

// MockManagementClient is a mock of ManagementClient interface.
type MockManagementClient struct {
	ctrl     *gomock.Controller
	recorder *MockManagementClientMockRecorder
}

// MockManagementClientMockRecorder is the mock recorder for MockManagementClient.
type MockManagementClientMockRecorder struct {
	mock *MockManagementClient
}

// NewMockManagementClient creates a new mock instance.
func NewMockManagementClient(ctrl *gomock.Controller) *MockManagementClient {
	mock := &MockManagementClient{ctrl: ctrl}
	mock.recorder = &MockManagementClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockManagementClient) EXPECT() *MockManagementClientMockRecorder {
	return m.recorder
}

// AddPeer mocks base method.
func (m *MockManagementClient) AddPeer(arg0 context.Context, arg1 *mrpb.AddPeerRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AddPeer", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddPeer indicates an expected call of AddPeer.
func (mr *MockManagementClientMockRecorder) AddPeer(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddPeer", reflect.TypeOf((*MockManagementClient)(nil).AddPeer), varargs...)
}

// GetClusterInfo mocks base method.
func (m *MockManagementClient) GetClusterInfo(arg0 context.Context, arg1 *mrpb.GetClusterInfoRequest, arg2 ...grpc.CallOption) (*mrpb.GetClusterInfoResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetClusterInfo", varargs...)
	ret0, _ := ret[0].(*mrpb.GetClusterInfoResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterInfo indicates an expected call of GetClusterInfo.
func (mr *MockManagementClientMockRecorder) GetClusterInfo(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterInfo", reflect.TypeOf((*MockManagementClient)(nil).GetClusterInfo), varargs...)
}

// RemovePeer mocks base method.
func (m *MockManagementClient) RemovePeer(arg0 context.Context, arg1 *mrpb.RemovePeerRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RemovePeer", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RemovePeer indicates an expected call of RemovePeer.
func (mr *MockManagementClientMockRecorder) RemovePeer(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemovePeer", reflect.TypeOf((*MockManagementClient)(nil).RemovePeer), varargs...)
}

// MockManagementServer is a mock of ManagementServer interface.
type MockManagementServer struct {
	ctrl     *gomock.Controller
	recorder *MockManagementServerMockRecorder
}

// MockManagementServerMockRecorder is the mock recorder for MockManagementServer.
type MockManagementServerMockRecorder struct {
	mock *MockManagementServer
}

// NewMockManagementServer creates a new mock instance.
func NewMockManagementServer(ctrl *gomock.Controller) *MockManagementServer {
	mock := &MockManagementServer{ctrl: ctrl}
	mock.recorder = &MockManagementServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockManagementServer) EXPECT() *MockManagementServerMockRecorder {
	return m.recorder
}

// AddPeer mocks base method.
func (m *MockManagementServer) AddPeer(arg0 context.Context, arg1 *mrpb.AddPeerRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddPeer", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddPeer indicates an expected call of AddPeer.
func (mr *MockManagementServerMockRecorder) AddPeer(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddPeer", reflect.TypeOf((*MockManagementServer)(nil).AddPeer), arg0, arg1)
}

// GetClusterInfo mocks base method.
func (m *MockManagementServer) GetClusterInfo(arg0 context.Context, arg1 *mrpb.GetClusterInfoRequest) (*mrpb.GetClusterInfoResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetClusterInfo", arg0, arg1)
	ret0, _ := ret[0].(*mrpb.GetClusterInfoResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetClusterInfo indicates an expected call of GetClusterInfo.
func (mr *MockManagementServerMockRecorder) GetClusterInfo(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetClusterInfo", reflect.TypeOf((*MockManagementServer)(nil).GetClusterInfo), arg0, arg1)
}

// RemovePeer mocks base method.
func (m *MockManagementServer) RemovePeer(arg0 context.Context, arg1 *mrpb.RemovePeerRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemovePeer", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RemovePeer indicates an expected call of RemovePeer.
func (mr *MockManagementServerMockRecorder) RemovePeer(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemovePeer", reflect.TypeOf((*MockManagementServer)(nil).RemovePeer), arg0, arg1)
}

// MockMetadataRepositoryServiceClient is a mock of MetadataRepositoryServiceClient interface.
type MockMetadataRepositoryServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockMetadataRepositoryServiceClientMockRecorder
}

// MockMetadataRepositoryServiceClientMockRecorder is the mock recorder for MockMetadataRepositoryServiceClient.
type MockMetadataRepositoryServiceClientMockRecorder struct {
	mock *MockMetadataRepositoryServiceClient
}

// NewMockMetadataRepositoryServiceClient creates a new mock instance.
func NewMockMetadataRepositoryServiceClient(ctrl *gomock.Controller) *MockMetadataRepositoryServiceClient {
	mock := &MockMetadataRepositoryServiceClient{ctrl: ctrl}
	mock.recorder = &MockMetadataRepositoryServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMetadataRepositoryServiceClient) EXPECT() *MockMetadataRepositoryServiceClientMockRecorder {
	return m.recorder
}

// GetCommitResult mocks base method.
func (m *MockMetadataRepositoryServiceClient) GetCommitResult(arg0 context.Context, arg1 *mrpb.GetCommitResultRequest, arg2 ...grpc.CallOption) (*mrpb.GetCommitResultResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetCommitResult", varargs...)
	ret0, _ := ret[0].(*mrpb.GetCommitResultResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCommitResult indicates an expected call of GetCommitResult.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) GetCommitResult(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCommitResult", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).GetCommitResult), varargs...)
}

// GetMetadata mocks base method.
func (m *MockMetadataRepositoryServiceClient) GetMetadata(arg0 context.Context, arg1 *mrpb.GetMetadataRequest, arg2 ...grpc.CallOption) (*mrpb.GetMetadataResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetMetadata", varargs...)
	ret0, _ := ret[0].(*mrpb.GetMetadataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMetadata indicates an expected call of GetMetadata.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) GetMetadata(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetadata", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).GetMetadata), varargs...)
}

// GetReports mocks base method.
func (m *MockMetadataRepositoryServiceClient) GetReports(arg0 context.Context, arg1 *mrpb.GetReportsRequest, arg2 ...grpc.CallOption) (*mrpb.GetReportsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetReports", varargs...)
	ret0, _ := ret[0].(*mrpb.GetReportsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReports indicates an expected call of GetReports.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) GetReports(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReports", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).GetReports), varargs...)
}

// RegisterLogStream mocks base method.
func (m *MockMetadataRepositoryServiceClient) RegisterLogStream(arg0 context.Context, arg1 *mrpb.LogStreamRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RegisterLogStream", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterLogStream indicates an expected call of RegisterLogStream.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) RegisterLogStream(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterLogStream", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).RegisterLogStream), varargs...)
}

// RegisterStorageNode mocks base method.
func (m *MockMetadataRepositoryServiceClient) RegisterStorageNode(arg0 context.Context, arg1 *mrpb.StorageNodeRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RegisterStorageNode", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterStorageNode indicates an expected call of RegisterStorageNode.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) RegisterStorageNode(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterStorageNode", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).RegisterStorageNode), varargs...)
}

// RegisterTopic mocks base method.
func (m *MockMetadataRepositoryServiceClient) RegisterTopic(arg0 context.Context, arg1 *mrpb.TopicRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RegisterTopic", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterTopic indicates an expected call of RegisterTopic.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) RegisterTopic(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterTopic", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).RegisterTopic), varargs...)
}

// Seal mocks base method.
func (m *MockMetadataRepositoryServiceClient) Seal(arg0 context.Context, arg1 *mrpb.SealRequest, arg2 ...grpc.CallOption) (*mrpb.SealResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Seal", varargs...)
	ret0, _ := ret[0].(*mrpb.SealResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Seal indicates an expected call of Seal.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) Seal(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Seal", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).Seal), varargs...)
}

// UnregisterLogStream mocks base method.
func (m *MockMetadataRepositoryServiceClient) UnregisterLogStream(arg0 context.Context, arg1 *mrpb.LogStreamRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UnregisterLogStream", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnregisterLogStream indicates an expected call of UnregisterLogStream.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) UnregisterLogStream(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterLogStream", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).UnregisterLogStream), varargs...)
}

// UnregisterStorageNode mocks base method.
func (m *MockMetadataRepositoryServiceClient) UnregisterStorageNode(arg0 context.Context, arg1 *mrpb.StorageNodeRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UnregisterStorageNode", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnregisterStorageNode indicates an expected call of UnregisterStorageNode.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) UnregisterStorageNode(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterStorageNode", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).UnregisterStorageNode), varargs...)
}

// UnregisterTopic mocks base method.
func (m *MockMetadataRepositoryServiceClient) UnregisterTopic(arg0 context.Context, arg1 *mrpb.TopicRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UnregisterTopic", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnregisterTopic indicates an expected call of UnregisterTopic.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) UnregisterTopic(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterTopic", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).UnregisterTopic), varargs...)
}

// Unseal mocks base method.
func (m *MockMetadataRepositoryServiceClient) Unseal(arg0 context.Context, arg1 *mrpb.UnsealRequest, arg2 ...grpc.CallOption) (*mrpb.UnsealResponse, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Unseal", varargs...)
	ret0, _ := ret[0].(*mrpb.UnsealResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Unseal indicates an expected call of Unseal.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) Unseal(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unseal", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).Unseal), varargs...)
}

// UpdateLogStream mocks base method.
func (m *MockMetadataRepositoryServiceClient) UpdateLogStream(arg0 context.Context, arg1 *mrpb.LogStreamRequest, arg2 ...grpc.CallOption) (*types.Empty, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateLogStream", varargs...)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateLogStream indicates an expected call of UpdateLogStream.
func (mr *MockMetadataRepositoryServiceClientMockRecorder) UpdateLogStream(arg0, arg1 any, arg2 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateLogStream", reflect.TypeOf((*MockMetadataRepositoryServiceClient)(nil).UpdateLogStream), varargs...)
}

// MockMetadataRepositoryServiceServer is a mock of MetadataRepositoryServiceServer interface.
type MockMetadataRepositoryServiceServer struct {
	ctrl     *gomock.Controller
	recorder *MockMetadataRepositoryServiceServerMockRecorder
}

// MockMetadataRepositoryServiceServerMockRecorder is the mock recorder for MockMetadataRepositoryServiceServer.
type MockMetadataRepositoryServiceServerMockRecorder struct {
	mock *MockMetadataRepositoryServiceServer
}

// NewMockMetadataRepositoryServiceServer creates a new mock instance.
func NewMockMetadataRepositoryServiceServer(ctrl *gomock.Controller) *MockMetadataRepositoryServiceServer {
	mock := &MockMetadataRepositoryServiceServer{ctrl: ctrl}
	mock.recorder = &MockMetadataRepositoryServiceServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockMetadataRepositoryServiceServer) EXPECT() *MockMetadataRepositoryServiceServerMockRecorder {
	return m.recorder
}

// GetCommitResult mocks base method.
func (m *MockMetadataRepositoryServiceServer) GetCommitResult(arg0 context.Context, arg1 *mrpb.GetCommitResultRequest) (*mrpb.GetCommitResultResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCommitResult", arg0, arg1)
	ret0, _ := ret[0].(*mrpb.GetCommitResultResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCommitResult indicates an expected call of GetCommitResult.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) GetCommitResult(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCommitResult", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).GetCommitResult), arg0, arg1)
}

// GetMetadata mocks base method.
func (m *MockMetadataRepositoryServiceServer) GetMetadata(arg0 context.Context, arg1 *mrpb.GetMetadataRequest) (*mrpb.GetMetadataResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMetadata", arg0, arg1)
	ret0, _ := ret[0].(*mrpb.GetMetadataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMetadata indicates an expected call of GetMetadata.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) GetMetadata(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetadata", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).GetMetadata), arg0, arg1)
}

// GetReports mocks base method.
func (m *MockMetadataRepositoryServiceServer) GetReports(arg0 context.Context, arg1 *mrpb.GetReportsRequest) (*mrpb.GetReportsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetReports", arg0, arg1)
	ret0, _ := ret[0].(*mrpb.GetReportsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetReports indicates an expected call of GetReports.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) GetReports(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetReports", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).GetReports), arg0, arg1)
}

// RegisterLogStream mocks base method.
func (m *MockMetadataRepositoryServiceServer) RegisterLogStream(arg0 context.Context, arg1 *mrpb.LogStreamRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterLogStream", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterLogStream indicates an expected call of RegisterLogStream.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) RegisterLogStream(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterLogStream", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).RegisterLogStream), arg0, arg1)
}

// RegisterStorageNode mocks base method.
func (m *MockMetadataRepositoryServiceServer) RegisterStorageNode(arg0 context.Context, arg1 *mrpb.StorageNodeRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterStorageNode", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterStorageNode indicates an expected call of RegisterStorageNode.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) RegisterStorageNode(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterStorageNode", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).RegisterStorageNode), arg0, arg1)
}

// RegisterTopic mocks base method.
func (m *MockMetadataRepositoryServiceServer) RegisterTopic(arg0 context.Context, arg1 *mrpb.TopicRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterTopic", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterTopic indicates an expected call of RegisterTopic.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) RegisterTopic(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterTopic", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).RegisterTopic), arg0, arg1)
}

// Seal mocks base method.
func (m *MockMetadataRepositoryServiceServer) Seal(arg0 context.Context, arg1 *mrpb.SealRequest) (*mrpb.SealResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Seal", arg0, arg1)
	ret0, _ := ret[0].(*mrpb.SealResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Seal indicates an expected call of Seal.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) Seal(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Seal", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).Seal), arg0, arg1)
}

// UnregisterLogStream mocks base method.
func (m *MockMetadataRepositoryServiceServer) UnregisterLogStream(arg0 context.Context, arg1 *mrpb.LogStreamRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnregisterLogStream", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnregisterLogStream indicates an expected call of UnregisterLogStream.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) UnregisterLogStream(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterLogStream", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).UnregisterLogStream), arg0, arg1)
}

// UnregisterStorageNode mocks base method.
func (m *MockMetadataRepositoryServiceServer) UnregisterStorageNode(arg0 context.Context, arg1 *mrpb.StorageNodeRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnregisterStorageNode", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnregisterStorageNode indicates an expected call of UnregisterStorageNode.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) UnregisterStorageNode(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterStorageNode", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).UnregisterStorageNode), arg0, arg1)
}

// UnregisterTopic mocks base method.
func (m *MockMetadataRepositoryServiceServer) UnregisterTopic(arg0 context.Context, arg1 *mrpb.TopicRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnregisterTopic", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnregisterTopic indicates an expected call of UnregisterTopic.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) UnregisterTopic(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnregisterTopic", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).UnregisterTopic), arg0, arg1)
}

// Unseal mocks base method.
func (m *MockMetadataRepositoryServiceServer) Unseal(arg0 context.Context, arg1 *mrpb.UnsealRequest) (*mrpb.UnsealResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unseal", arg0, arg1)
	ret0, _ := ret[0].(*mrpb.UnsealResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Unseal indicates an expected call of Unseal.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) Unseal(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unseal", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).Unseal), arg0, arg1)
}

// UpdateLogStream mocks base method.
func (m *MockMetadataRepositoryServiceServer) UpdateLogStream(arg0 context.Context, arg1 *mrpb.LogStreamRequest) (*types.Empty, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateLogStream", arg0, arg1)
	ret0, _ := ret[0].(*types.Empty)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateLogStream indicates an expected call of UpdateLogStream.
func (mr *MockMetadataRepositoryServiceServerMockRecorder) UpdateLogStream(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateLogStream", reflect.TypeOf((*MockMetadataRepositoryServiceServer)(nil).UpdateLogStream), arg0, arg1)
}
