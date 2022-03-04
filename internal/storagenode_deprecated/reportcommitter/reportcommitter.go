package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/reportcommitter -package reportcommitter -destination reportcommitter_mock.go . ReportCommitter,Getter

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

// ReportCommitter is an interface to get a report of the log stream and accept a commit message.
//
// GetReport returns a report of the log stream.
//
// Commit accepts commit message and applies it if possible.
type ReportCommitter interface {
	GetReport() (snpb.LogStreamUncommitReport, error)
	Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error
}

// Getter is an interface that gets ReportCommitter and fills a GetReportResponse.
//
// ReportCommitter returns reportCommitter corresponded with given logStreamID. If the
// reportCommitter does not exist, the result ok is false.
//
// GetReports fills the argument rsp by calling the function f.  The argument f is a function that
// calls GetReport of its first argument and fills its second argument; see the function
// internal/storagenode/reportcommitter.report.
type Getter interface {
	ReportCommitter(topicID types.TopicID, logStreamID types.LogStreamID) (reportCommitter ReportCommitter, ok bool)

	// TODO(jun): Fix confusing method name.
	GetReports(rsp *snpb.GetReportResponse, f func(ReportCommitter, *snpb.GetReportResponse))
}
