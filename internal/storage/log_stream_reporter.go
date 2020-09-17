package storage

import (
	"context"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	"go.uber.org/zap"
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
	knownHighWatermark types.GLSN
	reports            []UncommittedLogStreamStatus
	done               chan struct{}
}

type lsrCommitTask struct {
	highWatermark     types.GLSN
	prevHighWatermark types.GLSN
	commitResults     []CommittedLogStreamStatus
}

type LogStreamReporter interface {
	Run(ctx context.Context) error
	Close()
	StorageNodeID() types.StorageNodeID
	GetReport(ctx context.Context) (types.GLSN, []UncommittedLogStreamStatus, error)
	Commit(ctx context.Context, highWatermark, prevHighWatermark types.GLSN, commitResults []CommittedLogStreamStatus) error
}

type logStreamReporter struct {
	storageNodeID      types.StorageNodeID
	knownHighWatermark types.AtomicGLSN
	lseGetter          LogStreamExecutorGetter
	history            map[types.GLSN][]UncommittedLogStreamStatus

	running   bool
	muRunning sync.Mutex

	cancel  context.CancelFunc
	runner  *runner.Runner
	reportC chan *lsrReportTask
	commitC chan lsrCommitTask

	options *LogStreamReporterOptions
	logger  *zap.Logger
}

func NewLogStreamReporter(logger *zap.Logger, storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter, options *LogStreamReporterOptions) LogStreamReporter {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logstreamreporter")
	return &logStreamReporter{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
		history:       make(map[types.GLSN][]UncommittedLogStreamStatus),
		reportC:       make(chan *lsrReportTask, options.ReportCSize),
		commitC:       make(chan lsrCommitTask, options.CommitCSize),
		runner:        runner.New("logstreamreporter", logger),
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
		goto err_out
	}
	if err = lsr.runner.RunC(mctx, lsr.dispatchReport); err != nil {
		lsr.logger.Error("could not run dispatchReport", zap.Error(err))
		goto err_out
	}
	return nil

err_out:
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
	lsr.logger.Info("stop")
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
func (lsr *logStreamReporter) GetReport(ctx context.Context) (types.GLSN, []UncommittedLogStreamStatus, error) {
	t := lsrReportTask{done: make(chan struct{})}
	if err := lsr.addReportC(ctx, &t); err != nil {
		return types.InvalidGLSN, nil, err
	}
	tctx, cancel := context.WithTimeout(ctx, lsr.options.ReportWaitTimeout)
	defer cancel()
	select {
	case <-t.done:
	case <-tctx.Done():
		return types.InvalidGLSN, nil, tctx.Err()
	}

	return t.knownHighWatermark, t.reports, nil
}

func (lsr *logStreamReporter) addReportC(ctx context.Context, t *lsrReportTask) error {
	tctx, cancel := context.WithTimeout(ctx, lsr.options.ReportCTimeout)
	defer cancel()
	select {
	case lsr.reportC <- t:
	case <-tctx.Done():
		return tctx.Err()
	}
	return nil
}

func (lsr *logStreamReporter) report(t *lsrReportTask) {
	executors := lsr.lseGetter.GetLogStreamExecutors()
	reports := make([]UncommittedLogStreamStatus, 0, len(executors))
	knownHighWatermark := types.InvalidGLSN
	nrUncommitted := uint64(0)
	for _, executor := range executors {
		status := executor.GetReport()

		// get non-zero, minimum KnwonNextGLSN (zero means just added LS)
		if !status.KnownHighWatermark.Invalid() && (knownHighWatermark.Invalid() || knownHighWatermark > status.KnownHighWatermark) {
			knownHighWatermark = status.KnownHighWatermark
		}
		reports = append(reports, status)
		nrUncommitted += status.UncommittedLLSNLength
	}

	//if len(reports) > 0 { // skip when no meaningful statuses in reports
	if nrUncommitted > 0 { // skip when no meaningful statuses in reports
		// for simplicity, it uses old reports as possible
		// TODO (jun.song)
		// 1) old and new reports can be merged when they have the same KnownNextGLSN
		// 2) after implementing LSE-wise KnownNextGLSN, history won't be needed any longer
		oldReports, ok := lsr.history[knownHighWatermark]
		if !ok {
			lsr.history[knownHighWatermark] = reports
			goto out
		}
		reports = oldReports
	}

out:
	t.reports = reports
	t.knownHighWatermark = knownHighWatermark
	close(t.done)

	for nextGLSN := range lsr.history {
		if nextGLSN < knownHighWatermark {
			delete(lsr.history, nextGLSN)
		}
	}
}

func (lsr *logStreamReporter) Commit(ctx context.Context, highWatermark, prevHighWatermark types.GLSN, commitResults []CommittedLogStreamStatus) error {
	if !lsr.verifyCommit(prevHighWatermark) {
		return varlog.ErrInternal
	}
	if len(commitResults) == 0 {
		return varlog.ErrInternal
	}

	t := lsrCommitTask{
		highWatermark:     highWatermark,
		prevHighWatermark: prevHighWatermark,
		commitResults:     commitResults,
	}
	tctx, cancel := context.WithTimeout(ctx, lsr.options.CommitCTimeout)
	defer cancel()
	select {
	case lsr.commitC <- t:
		return nil
	case <-tctx.Done():
		return tctx.Err()
	}
}

func (lsr *logStreamReporter) commit(ctx context.Context, t lsrCommitTask) {
	if !lsr.verifyCommit(t.prevHighWatermark) {
		return
	}
	for _, commitResult := range t.commitResults {
		logStreamID := commitResult.LogStreamID
		executor, ok := lsr.lseGetter.GetLogStreamExecutor(logStreamID)
		if !ok {
			panic("no such executor")
		}
		go executor.Commit(ctx, commitResult)
	}
	lsr.knownHighWatermark.Store(t.highWatermark)
}

func (lsr *logStreamReporter) verifyCommit(prevHighWatermark types.GLSN) bool {
	return prevHighWatermark == lsr.knownHighWatermark.Load()
}
