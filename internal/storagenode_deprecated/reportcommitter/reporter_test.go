package reportcommitter

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/id"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

func TestLogStreamReporterEmptyStorageNode(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rcg := NewMockGetter(ctrl)
	rcg.EXPECT().ReportCommitter(gomock.Any(), gomock.Any()).Return(nil, false).AnyTimes()
	rcg.EXPECT().GetReports(gomock.Any(), gomock.Any()).Return().AnyTimes()

	getter := id.NewMockStorageNodeIDGetter(ctrl)
	getter.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
	lsr := New(WithStorageNodeIDGetter(getter), WithReportCommitterGetter(rcg))

	rsp := snpb.GetReportResponse{}
	err := lsr.GetReport(context.TODO(), &rsp)
	require.NoError(t, err)

	reports := rsp.UncommitReports
	require.Len(t, reports, 0)
}
