// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/anyproto/any-sync-coordinator/spacestatus (interfaces: SpaceStatus)
//
// Generated by this command:
//
//	mockgen -destination mock_spacestatus/mock_spacestatus.go github.com/anyproto/any-sync-coordinator/spacestatus SpaceStatus
//
// Package mock_spacestatus is a generated GoMock package.
package mock_spacestatus

import (
	context "context"
	reflect "reflect"

	spacestatus "github.com/anyproto/any-sync-coordinator/spacestatus"
	app "github.com/anyproto/any-sync/app"
	crypto "github.com/anyproto/any-sync/util/crypto"
	gomock "go.uber.org/mock/gomock"
)

// MockSpaceStatus is a mock of SpaceStatus interface.
type MockSpaceStatus struct {
	ctrl     *gomock.Controller
	recorder *MockSpaceStatusMockRecorder
}

// MockSpaceStatusMockRecorder is the mock recorder for MockSpaceStatus.
type MockSpaceStatusMockRecorder struct {
	mock *MockSpaceStatus
}

// NewMockSpaceStatus creates a new mock instance.
func NewMockSpaceStatus(ctrl *gomock.Controller) *MockSpaceStatus {
	mock := &MockSpaceStatus{ctrl: ctrl}
	mock.recorder = &MockSpaceStatusMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSpaceStatus) EXPECT() *MockSpaceStatusMockRecorder {
	return m.recorder
}

// AccountDelete mocks base method.
func (m *MockSpaceStatus) AccountDelete(arg0 context.Context, arg1 spacestatus.AccountDeletion) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AccountDelete", arg0, arg1)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AccountDelete indicates an expected call of AccountDelete.
func (mr *MockSpaceStatusMockRecorder) AccountDelete(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AccountDelete", reflect.TypeOf((*MockSpaceStatus)(nil).AccountDelete), arg0, arg1)
}

// AccountRevertDeletion mocks base method.
func (m *MockSpaceStatus) AccountRevertDeletion(arg0 context.Context, arg1 spacestatus.AccountInfo) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AccountRevertDeletion", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// AccountRevertDeletion indicates an expected call of AccountRevertDeletion.
func (mr *MockSpaceStatusMockRecorder) AccountRevertDeletion(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AccountRevertDeletion", reflect.TypeOf((*MockSpaceStatus)(nil).AccountRevertDeletion), arg0, arg1)
}

// ChangeStatus mocks base method.
func (m *MockSpaceStatus) ChangeStatus(arg0 context.Context, arg1 spacestatus.StatusChange) (spacestatus.StatusEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChangeStatus", arg0, arg1)
	ret0, _ := ret[0].(spacestatus.StatusEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ChangeStatus indicates an expected call of ChangeStatus.
func (mr *MockSpaceStatusMockRecorder) ChangeStatus(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChangeStatus", reflect.TypeOf((*MockSpaceStatus)(nil).ChangeStatus), arg0, arg1)
}

// Close mocks base method.
func (m *MockSpaceStatus) Close(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockSpaceStatusMockRecorder) Close(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockSpaceStatus)(nil).Close), arg0)
}

// Init mocks base method.
func (m *MockSpaceStatus) Init(arg0 *app.App) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockSpaceStatusMockRecorder) Init(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockSpaceStatus)(nil).Init), arg0)
}

// Name mocks base method.
func (m *MockSpaceStatus) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockSpaceStatusMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockSpaceStatus)(nil).Name))
}

// NewStatus mocks base method.
func (m *MockSpaceStatus) NewStatus(arg0 context.Context, arg1 string, arg2, arg3 crypto.PubKey, arg4 spacestatus.SpaceType, arg5 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewStatus", arg0, arg1, arg2, arg3, arg4, arg5)
	ret0, _ := ret[0].(error)
	return ret0
}

// NewStatus indicates an expected call of NewStatus.
func (mr *MockSpaceStatusMockRecorder) NewStatus(arg0, arg1, arg2, arg3, arg4, arg5 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewStatus", reflect.TypeOf((*MockSpaceStatus)(nil).NewStatus), arg0, arg1, arg2, arg3, arg4, arg5)
}

// Run mocks base method.
func (m *MockSpaceStatus) Run(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockSpaceStatusMockRecorder) Run(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockSpaceStatus)(nil).Run), arg0)
}

// SpaceDelete mocks base method.
func (m *MockSpaceStatus) SpaceDelete(arg0 context.Context, arg1 spacestatus.SpaceDeletion) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SpaceDelete", arg0, arg1)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SpaceDelete indicates an expected call of SpaceDelete.
func (mr *MockSpaceStatusMockRecorder) SpaceDelete(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SpaceDelete", reflect.TypeOf((*MockSpaceStatus)(nil).SpaceDelete), arg0, arg1)
}

// Status mocks base method.
func (m *MockSpaceStatus) Status(arg0 context.Context, arg1 string) (spacestatus.StatusEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Status", arg0, arg1)
	ret0, _ := ret[0].(spacestatus.StatusEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Status indicates an expected call of Status.
func (mr *MockSpaceStatusMockRecorder) Status(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Status", reflect.TypeOf((*MockSpaceStatus)(nil).Status), arg0, arg1)
}
