package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/reportcommitter -package reportcommitter -destination reporter_mock.go . Reporter

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type Reporter interface {
	io.Closer
	StorageNodeID() types.StorageNodeID
	GetReport(ctx context.Context, rsp *snpb.GetReportResponse) error
	Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error
}

type reporter struct {
	config
	commitWG sync.WaitGroup
	running  struct {
		r  bool
		mu sync.RWMutex
	}
}

var _ Reporter = (*reporter)(nil)

func New(opts ...Option) *reporter {
	cfg := newConfig(opts)
	lsr := &reporter{config: cfg}
	lsr.running.r = true
	return lsr
}

func (r *reporter) StorageNodeID() types.StorageNodeID {
	return r.storageNodeIDGetter.StorageNodeID()
}

func (r *reporter) Close() error {
	r.running.mu.Lock()
	defer r.running.mu.Unlock()
	if !r.running.r {
		return nil
	}
	r.running.r = false

	r.commitWG.Wait()
	r.logger.Info("stop")
	return nil
}

// GetReport collects statuses about uncommitted log entries from log streams in the storage node.
// KnownNextGLSNs from all LogStreamExecutors must be equal to the corresponding in
// Reporter.
func (r *reporter) GetReport(ctx context.Context, rsp *snpb.GetReportResponse) error {
	r.running.mu.RLock()
	defer r.running.mu.RUnlock()
	if !r.running.r {
		return errors.WithStack(verrors.ErrClosed)
	}

	rsp.UncommitReports = rsp.UncommitReports[0:0]
	r.reportCommitterGetter.GetReports(rsp, report)
	return nil
}

func report(reporter ReportCommitter, rsp *snpb.GetReportResponse) {
	rpt, err := reporter.GetReport()
	if err != nil {
		// TODO: is ignoring error safe?
		return
	}
	rsp.UncommitReports = append(rsp.UncommitReports, rpt)
}

func (r *reporter) Commit(ctx context.Context, commitResult snpb.LogStreamCommitResult) error {
	r.running.mu.RLock()
	defer r.running.mu.RUnlock()
	if !r.running.r {
		return errors.WithStack(verrors.ErrClosed)
	}

	committer, ok := r.reportCommitterGetter.ReportCommitter(commitResult.TopicID, commitResult.LogStreamID)
	if !ok {
		return errors.Errorf("no such committer: %d", commitResult.LogStreamID)
	}
	_ = committer.Commit(ctx, commitResult)
	return nil
}
