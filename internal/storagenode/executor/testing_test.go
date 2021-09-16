package executor

import (
	"github.com/golang/mock/gomock"

	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/types"
)

func NewTestMeasurableExecutor(ctrl *gomock.Controller, snid types.StorageNodeID, lsid types.LogStreamID) *MockMeasurableExecutor {
	ret := NewMockMeasurableExecutor(ctrl)
	ret.EXPECT().StorageNodeID().Return(snid).AnyTimes()
	ret.EXPECT().LogStreamID().Return(lsid).AnyTimes()
	ret.EXPECT().Stub().Return(telemetry.NewNopTelmetryStub()).AnyTimes()
	return ret
}

func NewTestMeasurable(ctrl *gomock.Controller) *telemetry.MockMeasurable {
	m := telemetry.NewMockMeasurable(ctrl)
	nop := telemetry.NewNopTelmetryStub()
	m.EXPECT().Stub().Return(nop).AnyTimes()
	return m
}
