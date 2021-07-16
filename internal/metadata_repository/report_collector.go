package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

const DefaultReportRefreshTime = time.Second
const DefaultCatchupRefreshTime = 3 * time.Millisecond

type ReportCollector interface {
	Run() error

	Reset()

	Close()

	Recover([]*varlogpb.StorageNodeDescriptor, []*varlogpb.LogStreamDescriptor, types.GLSN) error

	RegisterStorageNode(*varlogpb.StorageNodeDescriptor) error

	UnregisterStorageNode(types.StorageNodeID) error

	RegisterLogStream(types.StorageNodeID, types.LogStreamID, types.GLSN, varlogpb.LogStreamStatus) error

	UnregisterLogStream(types.StorageNodeID, types.LogStreamID) error

	Commit()

	Seal(types.LogStreamID)

	Unseal(types.LogStreamID, types.GLSN)

	NumExecutors() int

	NumCommitter() int
}

type commitHelper interface {
	getClient(ctx context.Context) (reportcommitter.Client, error)

	getReportedHighWatermark(types.LogStreamID) (types.GLSN, bool)

	getLastCommitResults() *mrpb.LogStreamCommitResults

	lookupNextCommitResults(types.GLSN) (*mrpb.LogStreamCommitResults, error)

	commit(context.Context, snpb.LogStreamCommitResult) error
}

type logStreamCommitter struct {
	lsID   types.LogStreamID
	helper commitHelper

	commitStatus struct {
		mu                 sync.RWMutex
		status             varlogpb.LogStreamStatus
		beginHighWatermark types.GLSN
	}

	catchupHelper struct {
		cli               reportcommitter.Client
		sentHighWatermark types.GLSN
		sentAt            time.Time
		expectedPos       int
	}

	triggerC chan struct{}

	runner *runner.Runner
	cancel context.CancelFunc
	logger *zap.Logger

	tmStub *telemetryStub
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

	committers []*logStreamCommitter
	cmmu       sync.RWMutex

	rpcTimeout time.Duration

	runner *runner.Runner
	cancel context.CancelFunc
	logger *zap.Logger

	tmStub *telemetryStub
}

type reportCollector struct {
	executors []*reportCollectExecutor
	mu        sync.RWMutex

	helper ReportCollectorHelper

	rpcTimeout time.Duration
	closed     bool

	commitC chan struct{}
	runner  *runner.Runner
	rnmu    sync.RWMutex

	tmStub *telemetryStub
	logger *zap.Logger
}

func NewReportCollector(helper ReportCollectorHelper, rpcTimeout time.Duration, tmStub *telemetryStub, logger *zap.Logger) *reportCollector {
	return &reportCollector{
		logger:     logger,
		helper:     helper,
		rpcTimeout: rpcTimeout,
		tmStub:     tmStub,
		commitC:    make(chan struct{}, 1),
		runner:     runner.New("reportCollector", logger),
	}
}

func (rc *reportCollector) running() bool {
	return !rc.closed
}

func (rc *reportCollector) runPropagateCommit(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rc.commitC:
			rc.commitInternal()
		}
	}
}

func (rc *reportCollector) Run() error {
	rc.rnmu.Lock()
	defer rc.rnmu.Unlock()

	ctx, _ := rc.runner.WithManagedCancel(context.Background())
	if err := rc.runner.RunC(ctx, rc.runPropagateCommit); err != nil {
		return err
	}

	return nil
}

func (rc *reportCollector) Reset() {
	rc.runner.Stop()

	rc.rnmu.Lock()
	rc.runner = runner.New("reportCollector", rc.logger)
	rc.rnmu.Unlock()

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.running() {
		for _, e := range rc.executors {
			e.stop()
		}

		rc.executors = nil
	}
}

func (rc *reportCollector) Close() {
	rc.rnmu.Lock()
	runner := rc.runner
	rc.rnmu.Unlock()

	runner.Stop()

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.running() {
		for _, e := range rc.executors {
			e.stop()
		}

		rc.executors = nil
	}

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

		status := varlogpb.LogStreamStatusRunning
		if ls.Status == varlogpb.LogStreamStatusSealed {
			status = varlogpb.LogStreamStatusSealed
		}

		for _, r := range ls.Replicas {
			err := rc.RegisterLogStream(r.StorageNodeID, ls.LogStreamID, highWatermark, status)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (rc *reportCollector) searchExecutor(id types.StorageNodeID) (int, bool) {
	i := sort.Search(len(rc.executors), func(i int) bool {
		return rc.executors[i].storageNodeID >= id
	})

	if i < len(rc.executors) && rc.executors[i].storageNodeID == id {
		return i, true
	}

	return i, false
}

func (rc *reportCollector) lookupExecutor(id types.StorageNodeID) *reportCollectExecutor {
	i, match := rc.searchExecutor(id)
	if !match {
		return nil
	}

	return rc.executors[i]
}

func (rc *reportCollector) deleteExecutor(snID types.StorageNodeID) error {
	i, match := rc.searchExecutor(snID)
	if !match {
		return errors.New("not exist")
	}

	copy(rc.executors[i:], rc.executors[i+1:])
	rc.executors = rc.executors[:len(rc.executors)-1]

	return nil
}

func (rc *reportCollector) insertExecutor(e *reportCollectExecutor) error {
	i, match := rc.searchExecutor(e.storageNodeID)
	if match {
		return errors.New("already exist")
	}

	rc.executors = append(rc.executors, &reportCollectExecutor{})
	copy(rc.executors[i+1:], rc.executors[i:])

	rc.executors[i] = e

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

	if e := rc.lookupExecutor(sn.StorageNodeID); e != nil {
		return verrors.ErrExist
	}

	logger := rc.logger.Named("executor").With(zap.Uint32("snid", uint32(sn.StorageNodeID)))
	executor := &reportCollectExecutor{
		storageNodeID: sn.StorageNodeID,
		helper:        rc.helper,
		snConnector:   storageNodeConnector{sn: sn},
		reportCtx:     &reportContext{},
		rpcTimeout:    rc.rpcTimeout,
		runner:        runner.New("excutor", logger),
		logger:        logger,
		tmStub:        rc.tmStub,
	}

	if err := executor.run(); err != nil {
		return err
	}

	rc.insertExecutor(executor)
	return nil
}

func (rc *reportCollector) UnregisterStorageNode(snID types.StorageNodeID) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor := rc.lookupExecutor(snID)
	if executor == nil {
		return verrors.ErrNotExist
	}

	if executor.numCommitter() != 0 {
		return verrors.ErrNotEmpty
	}

	rc.deleteExecutor(snID)
	executor.stopNoWait()

	return nil
}

func (rc *reportCollector) RegisterLogStream(snID types.StorageNodeID, lsID types.LogStreamID, highWatermark types.GLSN, status varlogpb.LogStreamStatus) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor := rc.lookupExecutor(snID)
	if executor == nil {
		return verrors.ErrNotExist
	}

	return executor.registerLogStream(lsID, highWatermark, status)
}

func (rc *reportCollector) UnregisterLogStream(snID types.StorageNodeID, lsID types.LogStreamID) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor := rc.lookupExecutor(snID)
	if executor == nil {
		return verrors.ErrNotExist
	}

	return executor.unregisterLogStream(lsID)
}

func (rc *reportCollector) Commit() {
	select {
	case rc.commitC <- struct{}{}:
	default:
	}
}

func (rc *reportCollector) commitInternal() {
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

	rc.logger.Info("Seal", zap.Any("lsid", lsID))
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

func (rce *reportCollectExecutor) searchCommitter(id types.LogStreamID) (int, bool) {
	i := sort.Search(len(rce.committers), func(i int) bool {
		return rce.committers[i].lsID >= id
	})

	if i < len(rce.committers) && rce.committers[i].lsID == id {
		return i, true
	}

	return i, false
}

func (rce *reportCollectExecutor) lookupCommitter(id types.LogStreamID) *logStreamCommitter {
	i, match := rce.searchCommitter(id)
	if !match {
		return nil
	}

	return rce.committers[i]
}

func (rce *reportCollectExecutor) deleteCommitter(id types.LogStreamID) error {
	i, match := rce.searchCommitter(id)
	if !match {
		return errors.New("not exist")
	}

	copy(rce.committers[i:], rce.committers[i+1:])
	rce.committers = rce.committers[:len(rce.committers)-1]

	return nil
}

func (rce *reportCollectExecutor) insertCommitter(c *logStreamCommitter) error {
	i, match := rce.searchCommitter(c.lsID)
	if match {
		return errors.New("already exist")
	}

	rce.committers = append(rce.committers, &logStreamCommitter{})
	copy(rce.committers[i+1:], rce.committers[i:])

	rce.committers[i] = c

	return nil
}

func (rce *reportCollectExecutor) registerLogStream(lsID types.LogStreamID, highWatermark types.GLSN, status varlogpb.LogStreamStatus) error {
	rce.cmmu.Lock()
	defer rce.cmmu.Unlock()

	if c := rce.lookupCommitter(lsID); c != nil {
		return verrors.ErrExist
	}

	c := newLogStreamCommitter(lsID, rce, highWatermark, status, rce.tmStub, rce.logger)
	err := c.run()
	if err != nil {
		return err
	}

	rce.insertCommitter(c)
	return nil
}

func (rce *reportCollectExecutor) unregisterLogStream(lsID types.LogStreamID) error {
	rce.cmmu.Lock()
	defer rce.cmmu.Unlock()

	c := rce.lookupCommitter(lsID)
	if c == nil {
		return verrors.ErrNotExist
	}

	rce.deleteCommitter(lsID)
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

	if c := rce.lookupCommitter(lsID); c != nil {
		c.seal()
	}
}

func (rce *reportCollectExecutor) unseal(lsID types.LogStreamID, highWatermark types.GLSN) {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	if c := rce.lookupCommitter(lsID); c != nil {
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
	getReportStart := time.Now()
	defer func() {
		dur := float64(time.Since(getReportStart).Nanoseconds()) / float64(time.Millisecond)
		rce.tmStub.mb.Records("mr.log_stream_committer.get_report.duration").Record(ctx, dur)
	}()

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
		if err := rce.helper.ProposeReport(rce.storageNodeID, report.UncommitReports); err != nil {
			rce.reportCtx.setExpire()
		}
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
	rce.reportCtx.saveReport(report)
	if prevReport == nil || rce.reportCtx.isExpire() {
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

func (rce *reportCollectExecutor) commit(ctx context.Context, cr snpb.LogStreamCommitResult) error {
	cli, err := rce.getClient(ctx)
	if err != nil {
		return err
	}

	r := snpb.CommitRequest{
		StorageNodeID: rce.storageNodeID,
		CommitResults: []snpb.LogStreamCommitResult{cr},
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

	r, ok := report.LookupReport(lsID)
	if !ok {
		return types.InvalidGLSN, false
	}

	return r.HighWatermark, true
}

func (rce *reportCollectExecutor) getLastCommitResults() *mrpb.LogStreamCommitResults {
	return rce.helper.GetLastCommitResults()
}

func (rce *reportCollectExecutor) lookupNextCommitResults(glsn types.GLSN) (*mrpb.LogStreamCommitResults, error) {
	return rce.helper.LookupNextCommitResults(glsn)
}

func newLogStreamCommitter(lsID types.LogStreamID, helper commitHelper, highWatermark types.GLSN, status varlogpb.LogStreamStatus, tmStub *telemetryStub, logger *zap.Logger) *logStreamCommitter {
	triggerC := make(chan struct{}, 1)

	c := &logStreamCommitter{
		lsID:     lsID,
		helper:   helper,
		triggerC: triggerC,
		runner:   runner.New("lscommitter", logger),
		logger:   logger,
		tmStub:   tmStub,
	}

	c.commitStatus.status = status
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

func (lc *logStreamCommitter) getCatchupHighWatermark(resetCatchupHelper bool) (types.GLSN, bool) {
	if resetCatchupHelper {
		lc.setSentHighWatermark(types.InvalidGLSN)
	}

	status, beginHighWatermark := lc.getCommitStatus()
	if status.Sealed() {
		return types.InvalidGLSN, false
	}

	highWatermark, ok := lc.helper.getReportedHighWatermark(lc.lsID)
	if !ok {
		return types.InvalidGLSN, false
	}

	sent := lc.getSentHighWatermark()
	if sent > highWatermark {
		highWatermark = sent
	}

	if beginHighWatermark > highWatermark {
		return beginHighWatermark, true
	}

	return highWatermark, true
}

func (lc *logStreamCommitter) catchup(ctx context.Context) {
	cli, err := lc.helper.getClient(ctx)
	if err != nil {
		return
	}

	resetCatchupHelper := false
	if lc.getKnownClient() != cli {
		lc.setKnownClient(cli)
		resetCatchupHelper = true
	}

	crs := lc.helper.getLastCommitResults()
	if crs == nil {
		return
	}

	highWatermark, ok := lc.getCatchupHighWatermark(resetCatchupHelper)
	if !ok || highWatermark >= crs.HighWatermark {
		return
	}

	catchupStart := time.Now()
	numCatchups := 0

	defer func() {
		dur := float64(time.Since(catchupStart).Nanoseconds()) / float64(time.Millisecond)
		lc.tmStub.mb.Records("mr.log_stream_committer.catchup.duration").Record(ctx, dur)
		lc.tmStub.mb.Records("mr.log_stream_committer.catchup.counts").Record(ctx, float64(numCatchups))
	}()

	for ctx.Err() == nil {
		if highWatermark != crs.PrevHighWatermark {
			crs, err = lc.helper.lookupNextCommitResults(highWatermark)
			if err != nil {
				latestHighWatermark, ok := lc.getCatchupHighWatermark(false)
				if !ok {
					return
				}

				if latestHighWatermark > highWatermark {
					highWatermark = latestHighWatermark
					continue
				}

				lc.logger.Warn(fmt.Sprintf("lsid:%v latest:%v err:%v", lc.lsID, latestHighWatermark, err.Error()))
				return
			}
		}

		if crs == nil {
			return
		}

		cr, expectedPos, ok := crs.LookupCommitResult(lc.lsID, lc.catchupHelper.expectedPos)
		if ok {
			lc.catchupHelper.expectedPos = expectedPos

			cr.HighWatermark = crs.HighWatermark
			cr.PrevHighWatermark = crs.PrevHighWatermark

			err := lc.helper.commit(ctx, cr)
			if err != nil {
				return
			}
		}

		lc.setSentHighWatermark(crs.HighWatermark)
		highWatermark = crs.HighWatermark

		numCatchups++
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

func (lc *logStreamCommitter) getSentHighWatermark() types.GLSN {
	if time.Since(lc.catchupHelper.sentAt) > DefaultCatchupRefreshTime {
		return types.InvalidGLSN
	}
	return lc.catchupHelper.sentHighWatermark
}

func (lc *logStreamCommitter) setSentHighWatermark(highWatermark types.GLSN) {
	lc.catchupHelper.sentHighWatermark = highWatermark
	lc.catchupHelper.sentAt = time.Now()
}

func (lc *logStreamCommitter) getKnownClient() reportcommitter.Client {
	return lc.catchupHelper.cli
}

func (lc *logStreamCommitter) setKnownClient(cli reportcommitter.Client) {
	lc.catchupHelper.cli = cli
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
	return time.Since(rc.reloadAt) > DefaultReportRefreshTime
}

func (rc *reportContext) setExpire() {
	rc.reloadAt = time.Time{}
}
