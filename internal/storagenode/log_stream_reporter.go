package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode -package storagenode -destination log_stream_reporter_mock.go . LogStreamReporter

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
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
	knownHighWatermark types.GLSN
	reports            map[types.LogStreamID]UncommittedLogStreamStatus
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
	GetReport(ctx context.Context) (types.GLSN, map[types.LogStreamID]UncommittedLogStreamStatus, error)
	Commit(ctx context.Context, highWatermark, prevHighWatermark types.GLSN, commitResults []CommittedLogStreamStatus) error
}

type logStreamReporter struct {
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter

	history         map[types.GLSN]map[types.LogStreamID]UncommittedLogStreamStatus
	lastReportedHWM types.GLSN

	running     bool
	muRunning   sync.Mutex
	stopped     chan struct{}
	onceStopped sync.Once

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
		history:       make(map[types.GLSN]map[types.LogStreamID]UncommittedLogStreamStatus),
		reportC:       make(chan *lsrReportTask, options.ReportCSize),
		commitC:       make(chan lsrCommitTask, options.CommitCSize),
		runner:        runner.New("logstreamreporter", logger),
		stopped:       make(chan struct{}),
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
func (lsr *logStreamReporter) GetReport(ctx context.Context) (types.GLSN, map[types.LogStreamID]UncommittedLogStreamStatus, error) {
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

	lsr.logger.Debug("contents of report", zap.Int("size", len(t.reports)), zap.Any("KnownHighWatermark", t.knownHighWatermark), zap.Reflect("reports", t.reports))
	return t.knownHighWatermark, t.reports, nil
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

func (lsr *logStreamReporter) saveHistory(knownHighWatermark types.GLSN, report UncommittedLogStreamStatus) {
	if lsr.history[knownHighWatermark] == nil {
		lsr.history[knownHighWatermark] = make(map[types.LogStreamID]UncommittedLogStreamStatus)
	}
	lsr.history[knownHighWatermark][report.LogStreamID] = report
}

func (lsr *logStreamReporter) report(t *lsrReportTask) {
	executors := lsr.lseGetter.GetLogStreamExecutors()
	if len(executors) == 0 {
		close(t.done)
		return
	}

	// hasNewLSE := false
	reports := make(map[types.LogStreamID]UncommittedLogStreamStatus)
	knownHighWatermark := types.MaxGLSN
	for _, executor := range executors {
		status := executor.GetReport()

		// If lastReportedHWM is zero, zero values of knownHighWatermark of
		// LogStreamExecutors are okay, since there is no way to discriminate the
		// LogStreamExecutors are newbies or slow-updaters.
		// If lastReportedHWM is not zero, zero values of knownHighWatermark of
		// LogStreamExecutors are ignored, since they must be newbies.
		if (lsr.lastReportedHWM.Invalid() || !status.KnownHighWatermark.Invalid()) &&
			(knownHighWatermark > status.KnownHighWatermark) {
			knownHighWatermark = status.KnownHighWatermark
		}
		reports[status.LogStreamID] = status

		// NOTE: Special care for the new LSE is not needed. See below comments.
		// hasNewLSE = hasNewLSE || (status.KnownHighWatermark.Invalid() && status.UncommittedLLSNLength > 0)
	}

	// NOTE: Special care for the new LSE is not needed. See below comments.
	// if hasNewLSE {
	//	knownHighWatermark = types.InvalidGLSN
	// }

	for logStreamID, newReport := range reports {
		lsr.saveHistory(newReport.KnownHighWatermark, newReport)

		if newReport.KnownHighWatermark.Invalid() {
			// NOTE: This LSE has invalid global HWM, but don't need special care:
			// 1) Replica of new LogStream
			// 2) New Replica of existing LogStream
			// In both cases, any valid HWM is possible. Valid HWM means that MR has
			// the HWM in its GLS.
			continue
		}

		// Each LSE advances its HWM progress. Thus, some LSEs can't have
		// knownHighWatermark, which is SN's HWM.  So it should delete those statuses of LSE
		// from the report.
		oldReports, ok := lsr.history[knownHighWatermark]
		if !ok {
			delete(reports, logStreamID)
			continue
		}
		oldReport, ok := oldReports[logStreamID]
		if !ok {
			delete(reports, logStreamID)
			continue
		}

		// NOTE: Report calibration (back to the previous status)
		//  If report calibration is needed, remove the comments below.
		oldReport.UncommittedLLSNLength = uint64(newReport.UncommittedLLSNOffset-oldReport.UncommittedLLSNOffset) + newReport.UncommittedLLSNLength

		reports[logStreamID] = oldReport

		// NOTE (jun): reports[logStreamID].KnownHighWatermark is overridden by
		// LogStreamReporterService.  This field is needed only after implementing LS-wise
		// HighWatermark.
	}

	lsr.lastReportedHWM = knownHighWatermark

	t.reports = reports
	t.knownHighWatermark = knownHighWatermark

	close(t.done)

	for hwm := range lsr.history {
		if hwm < knownHighWatermark {
			delete(lsr.history, hwm)
		}
	}
}

func (lsr *logStreamReporter) Commit(ctx context.Context, highWatermark, prevHighWatermark types.GLSN, commitResults []CommittedLogStreamStatus) error {
	if len(commitResults) == 0 {
		lsr.logger.Error("could not try to commit: no commit results")
		return verrors.ErrInternal
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
