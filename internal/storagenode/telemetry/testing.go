package telemetry

import gomock "github.com/golang/mock/gomock"

func NewTestMeasurable(ctrl *gomock.Controller) *MockMeasurable {
	m := NewMockMeasurable(ctrl)
	nop := NewNopTelmetryStub()
	m.EXPECT().Stub().Return(nop).AnyTimes()
	return m
}
