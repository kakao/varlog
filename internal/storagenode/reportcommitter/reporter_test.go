package reportcommitter

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestLogStreamReporterEmptyStorageNode(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rcg := NewMockGetter(ctrl)
	rcg.EXPECT().ReportCommitter(gomock.Any()).Return(nil, false).AnyTimes()
	rcg.EXPECT().ReportCommitters().Return(nil).AnyTimes()

	getter := id.NewMockStorageNodeIDGetter(ctrl)
	getter.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
	lsr := New(WithStorageNodeIDGetter(getter), WithReportCommitterGetter(rcg))

	reports, err := lsr.GetReport(context.TODO())
	require.NoError(t, err)
	require.Len(t, reports, 0)
}
