package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode -package storagenode -destination log_stream_reporter_mock.go . LogStreamReporter

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

var (
	errLSRKilled = errors.New("killed logstreamreporter")
)

type UncommittedLogStreamStatus struct {
	LogStreamID           types.LogStreamID
	KnownHighWatermark    types.GLSN
	UncommittedLLSNOffset types.LLSN
	UncommittedLLSNLength uint64
}

type CommittedLogStreamStatus struct {
	LogStreamID         types.LogStreamID
	HighWatermark       types.GLSN
	PrevHighWatermark   types.GLSN
	CommittedGLSNOffset types.GLSN
	CommittedGLSNLength uint64
}

type lsrReportTask struct {
	reports map[types.LogStreamID]UncommittedLogStreamStatus
	done    chan struct{}
}

type lsrCommitTask struct {
	commitResults []CommittedLogStreamStatus
}

type LogStreamReporter interface {
	Run(ctx context.Context) error
	Close()
	StorageNodeID() types.StorageNodeID
	GetReport(ctx context.Context) (map[types.LogStreamID]UncommittedLogStreamStatus, error)
	Commit(ctx context.Context, commitResults []CommittedLogStreamStatus) error
}

type logStreamReporter struct {
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter

	running     bool
	muRunning   sync.Mutex
	stopped     chan struct{}
	onceStopped sync.Once

	cancel  context.CancelFunc
	runner  *runner.Runner
	reportC chan *lsrReportTask
	commitC chan lsrCommitTask

	tmStub  *telemetryStub
	options *LogStreamReporterOptions
	logger  *zap.Logger
}

func NewLogStreamReporter(logger *zap.Logger, storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter, tmStub *telemetryStub, options *LogStreamReporterOptions) LogStreamReporter {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logstreamreporter")
	return &logStreamReporter{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
		reportC:       make(chan *lsrReportTask, options.ReportCSize),
		commitC:       make(chan lsrCommitTask, options.CommitCSize),
		runner:        runner.New("logstreamreporter", logger),
		stopped:       make(chan struct{}),
		tmStub:        tmStub,
		options:       options,
		logger:        logger,
	}
}

func (lsr *logStreamReporter) StorageNodeID() types.StorageNodeID {
	return lsr.storageNodeID
}

func (lsr *logStreamReporter) Run(ctx context.Context) error {
	lsr.muRunning.Lock()
	defer lsr.muRunning.Unlock()

	if lsr.running {
		return nil
	}
	lsr.running = true

	mctx, cancel := lsr.runner.WithManagedCancel(ctx)
	lsr.cancel = cancel

	var err error
	if err = lsr.runner.RunC(mctx, lsr.dispatchCommit); err != nil {
		lsr.logger.Error("could not run dispatchCommit", zap.Error(err))
		goto errOut
	}
	if err = lsr.runner.RunC(mctx, lsr.dispatchReport); err != nil {
		lsr.logger.Error("could not run dispatchReport", zap.Error(err))
		goto errOut
	}
	lsr.logger.Info("run")
	return nil

errOut:
	cancel()
	lsr.runner.Stop()
	return err
}

func (lsr *logStreamReporter) Close() {
	lsr.muRunning.Lock()
	defer lsr.muRunning.Unlock()

	if !lsr.running {
		return
	}
	lsr.running = false
	if lsr.cancel != nil {
		lsr.cancel()
	}
	lsr.runner.Stop()
	lsr.stop()
	lsr.logger.Info("stop")
}

func (lsr *logStreamReporter) stop() {
	lsr.onceStopped.Do(func() {
		lsr.logger.Info("stopped rpc handlers")
		close(lsr.stopped)
	})
}

func (lsr *logStreamReporter) dispatchReport(ctx context.Context) {
	for {
		select {
		case t := <-lsr.reportC:
			lsr.report(t)
		case <-ctx.Done():
			return
		}
	}
}

func (lsr *logStreamReporter) dispatchCommit(ctx context.Context) {
	for {
		select {
		case t := <-lsr.commitC:
			lsr.commit(ctx, t)
		case <-ctx.Done():
			return
		}
	}
}

// GetReport collects statuses about uncommitted log entries from log streams in the storage node.
// KnownNextGLSNs from all LogStreamExecutors must be equal to the corresponding in
// LogStreamReporter.
func (lsr *logStreamReporter) GetReport(ctx context.Context) (map[types.LogStreamID]UncommittedLogStreamStatus, error) {
	t := lsrReportTask{done: make(chan struct{})}
	if err := lsr.addReportC(ctx, &t); err != nil {
		return nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, lsr.options.ReportWaitTimeout)
	defer cancel()
	select {
	case <-t.done:
	case <-tctx.Done():
		return nil, tctx.Err()
	}

	lsr.logger.Debug("contents of report", zap.Int("size", len(t.reports)), zap.Reflect("reports", t.reports))
	return t.reports, nil
}

func (lsr *logStreamReporter) addReportC(ctx context.Context, t *lsrReportTask) error {
	tctx, cancel := context.WithTimeout(ctx, lsr.options.ReportCTimeout)
	defer cancel()
	select {
	case lsr.reportC <- t:
	case <-tctx.Done():
		return tctx.Err()
	case <-lsr.stopped:
		return errLSRKilled
	}
	return nil
}

func (lsr *logStreamReporter) report(t *lsrReportTask) {
	executors := lsr.lseGetter.GetLogStreamExecutors()
	if len(executors) == 0 {
		close(t.done)
		return
	}

	reports := make(map[types.LogStreamID]UncommittedLogStreamStatus)
	for _, executor := range executors {
		status := executor.GetReport()

		reports[status.LogStreamID] = status
	}

	t.reports = reports
	close(t.done)
}

func (lsr *logStreamReporter) Commit(ctx context.Context, commitResults []CommittedLogStreamStatus) error {
	if len(commitResults) == 0 {
		lsr.logger.Error("could not try to commit: no commit results")
		return verrors.ErrInternal
	}

	t := lsrCommitTask{
		commitResults: commitResults,
	}
	tctx, cancel := context.WithTimeout(ctx, lsr.options.CommitCTimeout)
	defer cancel()
	select {
	case lsr.commitC <- t:
		return nil
	case <-tctx.Done():
		lsr.logger.Error("could not try to commit: failed to append commitTask to commitC", zap.Error(tctx.Err()))
		return tctx.Err()
	case <-lsr.stopped:
		return errLSRKilled
	}
}

func (lsr *logStreamReporter) commit(ctx context.Context, t lsrCommitTask) {
	for i := range t.commitResults {
		go func(idx int) {
			logStreamID := t.commitResults[idx].LogStreamID
			executor, ok := lsr.lseGetter.GetLogStreamExecutor(logStreamID)
			if !ok {
				lsr.logger.Panic("no such executor", zap.Any("target_lsid", "logStreamID"))
			}
			lsr.logger.Debug("try to commit", zap.Reflect("commit", t.commitResults[idx]))
			executor.Commit(ctx, t.commitResults[idx])
		}(i)
	}
}
