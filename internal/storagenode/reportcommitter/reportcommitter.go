package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination reportcommitter_mock.go . ReportCommitter,Getter

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type ReportCommitter interface {
	GetReport(ctx context.Context) (snpb.LogStreamUncommitReport, error)
	Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error
}

type Getter interface {
	ReportCommitter(logStreamID types.LogStreamID) (ReportCommitter, bool)
	ReportCommitters() []ReportCommitter
	ForEachReportCommitter(func(ReportCommitter))
	NumberOfReportCommitters() int
}
