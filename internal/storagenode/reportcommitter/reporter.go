package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination reporter_mock.go . Reporter

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type Reporter interface {
	io.Closer
	StorageNodeID() types.StorageNodeID
	GetReport(ctx context.Context) ([]*snpb.LogStreamUncommitReport, error)
	Commit(ctx context.Context, commitResults []*snpb.LogStreamCommitResult) error
}

type reporter struct {
	config
	reportSG singleflight.Group
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
func (r *reporter) GetReport(ctx context.Context) ([]*snpb.LogStreamUncommitReport, error) {
	r.running.mu.RLock()
	defer r.running.mu.RUnlock()
	if !r.running.r {
		return nil, errors.WithStack(verrors.ErrClosed)
	}

	reports, err, _ := r.reportSG.Do("report", func() (interface{}, error) {
		reports := r.report(ctx)
		return reports, nil
	})
	return reports.([]*snpb.LogStreamUncommitReport), err
}

func (r *reporter) report(ctx context.Context) (reports []*snpb.LogStreamUncommitReport) {
	reporters := r.reportCommitterGetter.ReportCommitters()
	if len(reporters) == 0 {
		return reports
	}

	reports = make([]*snpb.LogStreamUncommitReport, 0, len(reporters))
	for _, reporter := range reporters {
		report, err := reporter.GetReport(ctx)
		if err != nil {
			// TODO: is ignoring error safe?
			continue
		}
		reports = append(reports, report)
	}
	return reports
}

func (r *reporter) Commit(_ context.Context, commitResults []*snpb.LogStreamCommitResult) error {
	r.running.mu.RLock()
	defer r.running.mu.RUnlock()
	if !r.running.r {
		return errors.WithStack(verrors.ErrClosed)
	}

	for i := range commitResults {
		commitResult := commitResults[i]
		r.commitWG.Add(1)
		go func() {
			defer r.commitWG.Done()
			logStreamID := commitResult.GetLogStreamID()
			committer, ok := r.reportCommitterGetter.ReportCommitter(logStreamID)
			if !ok {
				// dpanic
			}
			if err := committer.Commit(context.Background(), commitResult); err != nil {
				// logging
			}
		}()
	}
	return nil
}
