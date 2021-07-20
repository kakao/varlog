package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination reporter_mock.go . Reporter

import (
	"context"
	"fmt"
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
	Commit(ctx context.Context, commitResults []snpb.LogStreamCommitResult) error
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

	r.report(ctx, rsp)
	return nil
}

func (r *reporter) report(ctx context.Context, rsp *snpb.GetReportResponse) {
	rsp.UncommitReports = rsp.UncommitReports[0:0]
	r.reportCommitterGetter.ForEachReportCommitter(func(reporter ReportCommitter) {
		report, err := reporter.GetReport(ctx)
		if err != nil {
			// TODO: is ignoring error safe?
			return
		}
		rsp.UncommitReports = append(rsp.UncommitReports, report)
	})
}

func (r *reporter) Commit(_ context.Context, commitResults []snpb.LogStreamCommitResult) error {
	r.running.mu.RLock()
	defer r.running.mu.RUnlock()
	if !r.running.r {
		return errors.WithStack(verrors.ErrClosed)
	}

	cnt := len(commitResults)
	r.commitWG.Add(cnt)
	for i := 0; i < cnt; i++ {
		go func(idx int) {
			defer r.commitWG.Done()
			committer, ok := r.reportCommitterGetter.ReportCommitter(commitResults[idx].LogStreamID)
			if !ok {
				// dpanic
				panic(fmt.Sprintf("no such committer: %d", commitResults[idx].LogStreamID))
			}
			if err := committer.Commit(context.Background(), commitResults[idx]); err != nil {
				// logging
			}
		}(i)
	}
	return nil
}
