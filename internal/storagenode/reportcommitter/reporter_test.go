package reportcommitter

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/internal/storagenode/id"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

func TestLogStreamReporterEmptyStorageNode(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rcg := NewMockGetter(ctrl)
	rcg.EXPECT().ReportCommitter(gomock.Any()).Return(nil, false).AnyTimes()
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
