package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination reportcommitter_mock.go . ReportCommitter,Getter

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type ReportCommitter interface {
	GetReport() (snpb.LogStreamUncommitReport, error)
	Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error
}

type Getter interface {
	// ReportCommitter returns reportCommitter corresponded with given logStreamID. If the
	// reportCommitter does not exist, the result ok is false.
	ReportCommitter(logStreamID types.LogStreamID) (reportCommitter ReportCommitter, ok bool)

	// GetReports stores reports of all reportCommitters to given the rsp.
	GetReports(rsp *snpb.GetReportResponse, f func(ReportCommitter, *snpb.GetReportResponse))
}
