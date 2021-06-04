package metadata_repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

const DEFAULT_REPORT_ALL_DUR = time.Second

type ReportCollector interface {
	Run()

	Reset()

	Close()

	Recover([]*varlogpb.StorageNodeDescriptor, []*varlogpb.LogStreamDescriptor, types.GLSN) error

	RegisterStorageNode(*varlogpb.StorageNodeDescriptor) error

	UnregisterStorageNode(types.StorageNodeID) error

	RegisterLogStream(types.StorageNodeID, types.LogStreamID, types.GLSN) error

	UnregisterLogStream(types.StorageNodeID, types.LogStreamID) error

	Commit()

	Seal(types.LogStreamID)

	Unseal(types.LogStreamID, types.GLSN)

	NumExecutors() int

	NumCommitter() int
}

type commitHelper interface {
	commit(context.Context, *snpb.LogStreamCommitResult) error

	getReportedHighWatermark(types.LogStreamID) (types.GLSN, bool)

	lookupNextCommitResults(types.GLSN) (*mrpb.LogStreamCommitResults, error)
}

type logStreamCommitter struct {
	lsID   types.LogStreamID
	helper commitHelper

	commitStatus struct {
		mu                 sync.RWMutex
		status             varlogpb.LogStreamStatus
		beginHighWatermark types.GLSN
	}

	triggerC chan struct{}

	runner *runner.Runner
	cancel context.CancelFunc
	logger *zap.Logger
}

type reportContext struct {
	report   *mrpb.StorageNodeUncommitReport
	reloadAt time.Time
	mu       sync.RWMutex
}

type storageNodeConnector struct {
	sn  *varlogpb.StorageNodeDescriptor
	cli reportcommitter.Client
	mu  sync.RWMutex
}

type reportCollectExecutor struct {
	storageNodeID types.StorageNodeID
	helper        ReportCollectorHelper

	snConnector storageNodeConnector

	reportCtx *reportContext

	committers map[types.LogStreamID]*logStreamCommitter
	cmmu       sync.RWMutex

	rpcTimeout time.Duration

	runner *runner.Runner
	cancel context.CancelFunc
	logger *zap.Logger
}

type reportCollector struct {
	executors map[types.StorageNodeID]*reportCollectExecutor
	mu        sync.RWMutex

	helper ReportCollectorHelper

	rpcTimeout time.Duration
	closed     bool

	logger *zap.Logger
}

func NewReportCollector(helper ReportCollectorHelper, rpcTimeout time.Duration, logger *zap.Logger) *reportCollector {
	return &reportCollector{
		logger:     logger,
		helper:     helper,
		rpcTimeout: rpcTimeout,
		executors:  make(map[types.StorageNodeID]*reportCollectExecutor),
	}
}

func (rc *reportCollector) running() bool {
	return !rc.closed
}

func (rc *reportCollector) Run() {
}

func (rc *reportCollector) reset() {
	if rc.running() {
		for _, e := range rc.executors {
			e.stop()
		}

		rc.executors = make(map[types.StorageNodeID]*reportCollectExecutor)
	}
}

func (rc *reportCollector) Reset() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.reset()
}

func (rc *reportCollector) Close() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.reset()
	rc.closed = true
}

func (rc *reportCollector) Recover(sns []*varlogpb.StorageNodeDescriptor, lss []*varlogpb.LogStreamDescriptor, highWatermark types.GLSN) error {
	rc.Run()

	for _, sn := range sns {
		if sn.Status.Deleted() {
			continue
		}

		err := rc.RegisterStorageNode(sn)
		if err != nil {
			return err
		}
	}

	for _, ls := range lss {
		if ls.Status.Deleted() {
			continue
		}

		for _, r := range ls.Replicas {
			err := rc.RegisterLogStream(r.StorageNodeID, ls.LogStreamID, highWatermark)
			if err != nil {
				return err
			}
		}

		if ls.Status == varlogpb.LogStreamStatusSealed {
			rc.Seal(ls.LogStreamID)
		}
	}

	return nil
}

func (rc *reportCollector) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
	if sn == nil {
		return verrors.ErrInvalid
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	if _, ok := rc.executors[sn.StorageNodeID]; ok {
		return verrors.ErrExist
	}

	logger := rc.logger.Named("executor").With(zap.Uint32("snid", uint32(sn.StorageNodeID)))
	executor := &reportCollectExecutor{
		storageNodeID: sn.StorageNodeID,
		helper:        rc.helper,
		snConnector:   storageNodeConnector{sn: sn},
		reportCtx:     &reportContext{},
		committers:    make(map[types.LogStreamID]*logStreamCommitter),
		rpcTimeout:    rc.rpcTimeout,
		runner:        runner.New("excutor", logger),
		logger:        logger,
	}

	if err := executor.run(); err != nil {
		return err
	}

	rc.executors[sn.StorageNodeID] = executor

	return nil
}

func (rc *reportCollector) UnregisterStorageNode(snID types.StorageNodeID) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor, ok := rc.executors[snID]
	if !ok {
		return verrors.ErrNotExist
	}

	if executor.numCommitter() != 0 {
		return verrors.ErrNotEmpty
	}

	delete(rc.executors, snID)
	executor.stopNoWait()

	return nil
}

func (rc *reportCollector) RegisterLogStream(snID types.StorageNodeID, lsID types.LogStreamID, highWatermark types.GLSN) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor, ok := rc.executors[snID]
	if !ok {
		return verrors.ErrNotExist
	}

	return executor.registerLogStream(lsID, highWatermark)
}

func (rc *reportCollector) UnregisterLogStream(snID types.StorageNodeID, lsID types.LogStreamID) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor, ok := rc.executors[snID]
	if !ok {
		return verrors.ErrNotExist
	}

	return executor.unregisterLogStream(lsID)
}

func (rc *reportCollector) Commit() {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return
	}

	for _, executor := range rc.executors {
		executor.addCommitC()
	}
}

func (rc *reportCollector) Seal(lsID types.LogStreamID) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return
	}

	for _, executor := range rc.executors {
		executor.seal(lsID)
	}
}

func (rc *reportCollector) Unseal(lsID types.LogStreamID, highWatermark types.GLSN) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return
	}

	for _, executor := range rc.executors {
		executor.unseal(lsID, highWatermark)
	}
}

func (rc *reportCollector) NumExecutors() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return len(rc.executors)
}

func (rc *reportCollector) NumCommitter() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	num := 0
	for _, executor := range rc.executors {
		num += executor.numCommitter()
	}

	return num
}

func (rce *reportCollectExecutor) run() error {
	ctx, cancel := rce.runner.WithManagedCancel(context.Background())
	if err := rce.runner.RunC(ctx, rce.runReport); err != nil {
		return err
	}
	rce.cancel = cancel

	return nil
}

func (rce *reportCollectExecutor) stop() {
	rce.cmmu.RLock()
	for _, c := range rce.committers {
		c.stop()
	}
	rce.cmmu.RUnlock()

	rce.runner.Stop()

	rce.closeClient(nil)
}

func (rce *reportCollectExecutor) stopNoWait() {
	rce.cmmu.RLock()
	for _, c := range rce.committers {
		c.stopNoWait()
	}
	rce.cmmu.RUnlock()

	rce.cancel()

	go rce.stop()
}

func (rce *reportCollectExecutor) registerLogStream(lsID types.LogStreamID, highWatermark types.GLSN) error {
	rce.cmmu.Lock()
	defer rce.cmmu.Unlock()

	if _, ok := rce.committers[lsID]; ok {
		return verrors.ErrExist
	}

	c := newLogStreamCommitter(lsID, rce, highWatermark, rce.logger)
	err := c.run()
	if err != nil {
		return err
	}

	rce.committers[lsID] = c
	return nil
}

func (rce *reportCollectExecutor) unregisterLogStream(lsID types.LogStreamID) error {
	rce.cmmu.Lock()
	defer rce.cmmu.Unlock()

	c, ok := rce.committers[lsID]
	if !ok {
		return verrors.ErrNotExist
	}

	delete(rce.committers, lsID)
	c.stopNoWait()

	return nil
}

func (rce *reportCollectExecutor) addCommitC() {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	for _, c := range rce.committers {
		c.addCommitC()
	}
}

func (rce *reportCollectExecutor) numCommitter() int {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	return len(rce.committers)
}

func (rce *reportCollectExecutor) seal(lsID types.LogStreamID) {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	if c, ok := rce.committers[lsID]; ok {
		c.seal()
	}
}

func (rce *reportCollectExecutor) unseal(lsID types.LogStreamID, highWatermark types.GLSN) {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	if c, ok := rce.committers[lsID]; ok {
		c.unseal(highWatermark)
	}
}

func (rce *reportCollectExecutor) runReport(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			err := rce.getReport(ctx)
			if err != nil {
				continue Loop
			}
		}
	}

	rce.closeClient(nil)
}

func (rce *reportCollectExecutor) getReport(ctx context.Context) error {
	cli, err := rce.getClient(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, rce.rpcTimeout)
	defer cancel()
	response, err := cli.GetReport(ctx)
	if err != nil {
		rce.closeClient(cli)
		return err
	}

	report := rce.processReport(response)
	if report.Len() > 0 {
		rce.helper.ProposeReport(rce.storageNodeID, report.UncommitReports)
	}

	return nil
}

func (rce *reportCollectExecutor) processReport(response *snpb.GetReportResponse) *mrpb.StorageNodeUncommitReport {
	report := &mrpb.StorageNodeUncommitReport{
		StorageNodeID:   response.StorageNodeID,
		UncommitReports: response.UncommitReports,
	}

	if report.Len() == 0 {
		return report
	}

	report.Sort()

	prevReport := rce.reportCtx.getReport()
	if prevReport == nil {
		rce.reportCtx.saveReport(report)
		rce.reportCtx.reload()
		return report
	}

	diff := &mrpb.StorageNodeUncommitReport{
		StorageNodeID: report.StorageNodeID,
	}

	i := 0
	j := 0
	for i < report.Len() && j < prevReport.Len() {
		cur := report.UncommitReports[i]
		prev := prevReport.UncommitReports[j]

		if cur.LogStreamID < prev.LogStreamID {
			diff.UncommitReports = append(diff.UncommitReports, cur)
			i++
		} else if prev.LogStreamID < cur.LogStreamID {
			j++
		} else {
			if cur.HighWatermark < prev.HighWatermark {
				fmt.Printf("invalid report prev:%v, cur:%v\n",
					prev.HighWatermark, cur.HighWatermark)
				rce.logger.Panic("invalid report",
					zap.Any("prev", prev.HighWatermark),
					zap.Any("cur", cur.HighWatermark))
			}

			if cur.UncommittedLLSNOffset > prev.UncommittedLLSNOffset ||
				cur.UncommittedLLSNEnd() > prev.UncommittedLLSNEnd() {
				diff.UncommitReports = append(diff.UncommitReports, cur)
			}
			i++
			j++
		}
	}

	if i < report.Len() {
		diff.UncommitReports = append(diff.UncommitReports, report.UncommitReports[i:]...)
	}

	rce.reportCtx.saveReport(report)

	if rce.reportCtx.isExpire() {
		rce.reportCtx.reload()
		return report
	}

	return diff
}

func (rce *reportCollectExecutor) closeClient(cli reportcommitter.Client) {
	rce.snConnector.mu.Lock()
	defer rce.snConnector.mu.Unlock()

	if rce.snConnector.cli != nil &&
		(rce.snConnector.cli == cli || cli == nil) {
		rce.snConnector.cli.Close()
		rce.snConnector.cli = nil
	}
}

func (rce *reportCollectExecutor) getClient(ctx context.Context) (reportcommitter.Client, error) {
	rce.snConnector.mu.RLock()
	cli := rce.snConnector.cli
	rce.snConnector.mu.RUnlock()

	if cli != nil {
		return cli, nil
	}

	rce.snConnector.mu.Lock()
	defer rce.snConnector.mu.Unlock()

	var err error
	if rce.snConnector.cli == nil {
		rce.snConnector.cli, err = rce.helper.GetReporterClient(ctx, rce.snConnector.sn)
	}
	return rce.snConnector.cli, err
}

func (rce *reportCollectExecutor) commit(ctx context.Context, cr *snpb.LogStreamCommitResult) error {
	cli, err := rce.getClient(ctx)
	if err != nil {
		return err
	}

	r := snpb.CommitRequest{
		StorageNodeID: rce.storageNodeID,
		CommitResults: []*snpb.LogStreamCommitResult{cr},
	}

	ctx, cancel := context.WithTimeout(ctx, rce.rpcTimeout)
	defer cancel()

	err = cli.Commit(ctx, &r)
	if err != nil {
		rce.closeClient(cli)
		return err
	}

	return nil
}

func (rce *reportCollectExecutor) getReportedHighWatermark(lsID types.LogStreamID) (types.GLSN, bool) {
	report := rce.reportCtx.getReport()
	if report == nil {
		return types.InvalidGLSN, false
	}

	r := report.LookupReport(lsID)
	if r == nil {
		return types.InvalidGLSN, false
	}

	return r.HighWatermark, true
}

func (rce *reportCollectExecutor) lookupNextCommitResults(glsn types.GLSN) (*mrpb.LogStreamCommitResults, error) {
	return rce.helper.LookupNextCommitResults(glsn)
}

func newLogStreamCommitter(lsID types.LogStreamID, helper commitHelper, highWatermark types.GLSN, logger *zap.Logger) *logStreamCommitter {
	triggerC := make(chan struct{}, 1)

	c := &logStreamCommitter{
		lsID:     lsID,
		helper:   helper,
		triggerC: triggerC,
		runner:   runner.New("lscommitter", logger),
		logger:   logger,
	}

	c.commitStatus.status = varlogpb.LogStreamStatusRunning
	c.commitStatus.beginHighWatermark = highWatermark

	return c
}

func (lc *logStreamCommitter) run() error {
	ctx, cancel := lc.runner.WithManagedCancel(context.Background())
	if err := lc.runner.RunC(ctx, lc.runCommit); err != nil {
		return err
	}
	lc.cancel = cancel

	return nil
}

func (lc *logStreamCommitter) stop() {
	lc.runner.Stop()
}

func (lc *logStreamCommitter) stopNoWait() {
	lc.cancel()
}

func (lc *logStreamCommitter) addCommitC() {
	select {
	case lc.triggerC <- struct{}{}:
	default:
	}
}

func (lc *logStreamCommitter) runCommit(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-lc.triggerC:
			lc.catchup(ctx)
		}
	}

	close(lc.triggerC)
}

func (lc *logStreamCommitter) getCatchupHighWatermark() (types.GLSN, bool) {
	status, beginHighWatermark := lc.getCommitStatus()
	if status.Sealed() {
		return types.InvalidGLSN, false
	}

	highWatermark, ok := lc.helper.getReportedHighWatermark(lc.lsID)
	if !ok {
		return types.InvalidGLSN, false
	}

	if beginHighWatermark > highWatermark {
		return beginHighWatermark, true
	}

	return highWatermark, true
}

func (lc *logStreamCommitter) catchup(ctx context.Context) {
	highWatermark, ok := lc.getCatchupHighWatermark()
	if !ok {
		return
	}

	for ctx.Err() == nil {
		results, err := lc.helper.lookupNextCommitResults(highWatermark)
		if err != nil {
			latestHighWatermark, ok := lc.getCatchupHighWatermark()
			if !ok {
				return
			}

			if latestHighWatermark > highWatermark {
				highWatermark = latestHighWatermark
				continue
			}

			lc.logger.Panic(fmt.Sprintf("lsid:%v latest:%v err:%v", lc.lsID, latestHighWatermark, err.Error()))
		}

		if results == nil {
			return
		}

		cr := results.LookupCommitResult(lc.lsID)
		if cr != nil {
			cr = proto.Clone(cr).(*snpb.LogStreamCommitResult)
			cr.HighWatermark = results.HighWatermark
			cr.PrevHighWatermark = results.PrevHighWatermark

			err := lc.helper.commit(ctx, cr)
			if err != nil {
				return
			}
		}

		highWatermark = results.HighWatermark
	}
}

func (lc *logStreamCommitter) seal() {
	lc.commitStatus.mu.Lock()
	defer lc.commitStatus.mu.Unlock()

	lc.commitStatus.status = varlogpb.LogStreamStatusSealed
}

func (lc *logStreamCommitter) unseal(highWatermark types.GLSN) {
	lc.commitStatus.mu.Lock()
	defer lc.commitStatus.mu.Unlock()

	lc.commitStatus.status = varlogpb.LogStreamStatusRunning
	lc.commitStatus.beginHighWatermark = highWatermark
}

func (lc *logStreamCommitter) getCommitStatus() (varlogpb.LogStreamStatus, types.GLSN) {
	lc.commitStatus.mu.RLock()
	defer lc.commitStatus.mu.RUnlock()

	return lc.commitStatus.status, lc.commitStatus.beginHighWatermark
}

func (rc *reportContext) saveReport(report *mrpb.StorageNodeUncommitReport) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.report = report
}

func (rc *reportContext) getReport() *mrpb.StorageNodeUncommitReport {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.report
}

func (rc *reportContext) reload() {
	rc.reloadAt = time.Now()
}

func (rc *reportContext) isExpire() bool {
	return time.Since(rc.reloadAt) > DEFAULT_REPORT_ALL_DUR
}
