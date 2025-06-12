package metarepos

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/reportcommitter"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const DefaultReportRefreshTime = time.Second

const DefaultCatchupRefreshTime = 3 * time.Millisecond

const DefaultSampleReportsRate = 10000

type sampleTracer struct {
	m          sync.Map
	sampleRate uint64
}

type sampleReports struct {
	mu        sync.Mutex
	llsn      types.LLSN
	skip      types.LLSN
	created   map[types.StorageNodeID]time.Time
	committed bool
}

type ReportCollector interface {
	Run() error

	Reset()

	Close()

	Recover([]*varlogpb.StorageNodeDescriptor, []*varlogpb.LogStreamDescriptor, types.Version) error

	RegisterStorageNode(*varlogpb.StorageNodeDescriptor) error

	UnregisterStorageNode(types.StorageNodeID) error

	RegisterLogStream(types.TopicID, types.StorageNodeID, types.LogStreamID, types.Version, varlogpb.LogStreamStatus) error

	UnregisterLogStream(types.StorageNodeID, types.LogStreamID) error

	Commit()

	Seal(types.LogStreamID)

	Unseal(types.LogStreamID, types.Version)

	NumExecutors() int

	NumCommitter() int
}

type commitHelper interface {
	getReportedVersion(types.LogStreamID) (types.Version, bool)

	getLastCommitResults() *mrpb.LogStreamCommitResults

	lookupNextCommitResults(types.Version) (*mrpb.LogStreamCommitResults, error)
}

type logStreamCommitter struct {
	topicID types.TopicID
	lsID    types.LogStreamID
	helper  commitHelper

	commitStatus struct {
		mu           sync.RWMutex
		status       varlogpb.LogStreamStatus
		beginVersion types.Version
	}

	catchupHelper struct {
		sentVersion    types.Version
		sentAt         time.Time
		expectedPos    int
		expectedEndPos int
	}

	logger *zap.Logger

	sampleTracer *sampleTracer
	tmStub       *telemetryStub
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
	knownCli    reportcommitter.Client

	reportCtx *reportContext

	committers []*logStreamCommitter
	cmmu       sync.RWMutex

	rpcTimeout time.Duration

	runner *runner.Runner
	cancel context.CancelFunc

	triggerC chan struct{}

	logger *zap.Logger

	sampleTracer *sampleTracer
	tmStub       *telemetryStub
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

	logger *zap.Logger

	sampleTracer *sampleTracer
	tmStub       *telemetryStub
}

func NewReportCollector(helper ReportCollectorHelper, rpcTimeout time.Duration, tmStub *telemetryStub, logger *zap.Logger) *reportCollector {
	return &reportCollector{
		logger:       logger,
		helper:       helper,
		rpcTimeout:   rpcTimeout,
		tmStub:       tmStub,
		commitC:      make(chan struct{}, 1),
		sampleTracer: &sampleTracer{sampleRate: DefaultSampleReportsRate},
		runner:       runner.New("reportCollector", logger),
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

func (rc *reportCollector) Recover(sns []*varlogpb.StorageNodeDescriptor, lss []*varlogpb.LogStreamDescriptor, ver types.Version) error {
	rc.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.

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
			err := rc.RegisterLogStream(ls.TopicID, r.StorageNodeID, ls.LogStreamID, ver, status)
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

	logger := rc.logger.Named("executor").With(zap.Int32("snid", int32(sn.StorageNodeID)))
	executor := &reportCollectExecutor{
		storageNodeID: sn.StorageNodeID,
		helper:        rc.helper,
		snConnector:   storageNodeConnector{sn: sn},
		reportCtx:     &reportContext{},
		rpcTimeout:    rc.rpcTimeout,
		triggerC:      make(chan struct{}, 1),
		runner:        runner.New("excutor", logger),
		logger:        logger,
		sampleTracer:  rc.sampleTracer,
		tmStub:        rc.tmStub,
	}

	if err := executor.run(); err != nil {
		return err
	}

	rc.insertExecutor(executor) //nolint:errcheck,revive // TODO:: Handle an error returned.
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

	rc.deleteExecutor(snID) //nolint:errcheck,revive // TODO:: Handle an error returned.
	executor.stopNoWait()

	return nil
}

func (rc *reportCollector) RegisterLogStream(topicID types.TopicID, snID types.StorageNodeID, lsID types.LogStreamID, ver types.Version, status varlogpb.LogStreamStatus) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor := rc.lookupExecutor(snID)
	if executor == nil {
		return verrors.ErrNotExist
	}

	return executor.registerLogStream(topicID, lsID, ver, status)
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

func (rc *reportCollector) Unseal(lsID types.LogStreamID, ver types.Version) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return
	}

	for _, executor := range rc.executors {
		executor.unseal(lsID, ver)
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
	if err := rce.runner.RunC(ctx, rce.runCommit); err != nil {
		return err
	}
	rce.cancel = cancel

	return nil
}

func (rce *reportCollectExecutor) stop() {
	rce.runner.Stop()

	rce.closeClient(nil)
}

func (rce *reportCollectExecutor) stopNoWait() {
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

func (rce *reportCollectExecutor) registerLogStream(topicID types.TopicID, lsID types.LogStreamID, ver types.Version, status varlogpb.LogStreamStatus) error {
	rce.cmmu.Lock()
	defer rce.cmmu.Unlock()

	if c := rce.lookupCommitter(lsID); c != nil {
		return verrors.ErrExist
	}

	c := newLogStreamCommitter(topicID, lsID, rce, ver, status, rce.sampleTracer, rce.tmStub, rce.logger)
	rce.insertCommitter(c) //nolint:errcheck,revive // TODO:: Handle an error returned.
	return nil
}

func (rce *reportCollectExecutor) unregisterLogStream(lsID types.LogStreamID) error {
	rce.cmmu.Lock()
	defer rce.cmmu.Unlock()

	c := rce.lookupCommitter(lsID)
	if c == nil {
		return verrors.ErrNotExist
	}

	rce.deleteCommitter(lsID) //nolint:errcheck,revive // TODO:: Handle an error returned.
	return nil
}

func (rce *reportCollectExecutor) addCommitC() {
	select {
	case rce.triggerC <- struct{}{}:
	default:
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

func (rce *reportCollectExecutor) unseal(lsID types.LogStreamID, ver types.Version) {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	if c := rce.lookupCommitter(lsID); c != nil {
		c.unseal(ver)
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

	response, err := cli.GetReport()
	if err != nil {
		rce.closeClient(cli)
		return err
	}

	report := rce.processReport(response)
	defer report.Release()
	if report.Len() > 0 {
		if err := rce.helper.ProposeReport(rce.storageNodeID, report.UncommitReports); err != nil {
			rce.reportCtx.setExpire()
		}
	}

	return nil
}

func (rce *reportCollectExecutor) processReport(response *snpb.GetReportResponse) *mrpb.StorageNodeUncommitReport {
	report := mrpb.NewStorageNodeUncommitReport(response.StorageNodeID)
	report.UncommitReports = response.UncommitReports

	if report.Len() == 0 {
		return report
	}

	report.Sort()

	prevReport, ok := rce.reportCtx.swapReport(report)
	if !ok || rce.reportCtx.isExpire() {
		rce.reportCtx.reload()
		return report
	}

	diff := mrpb.NewStorageNodeUncommitReport(report.StorageNodeID)
	diff.UncommitReports = make([]snpb.LogStreamUncommitReport, 0, len(report.UncommitReports))
	defer report.Release()

	i := 0
	j := 0
	for i < report.Len() && j < prevReport.Len() {
		cur := report.UncommitReports[i]
		prev := prevReport.UncommitReports[j]

		if cur.Invalid() {
			i++
		} else if prev.Invalid() || prev.LogStreamID < cur.LogStreamID {
			j++
		} else if cur.LogStreamID < prev.LogStreamID {
			diff.UncommitReports = append(diff.UncommitReports, cur)
			i++
		} else {
			if cur.Version < prev.Version {
				rce.logger.Error("invalid report",
					zap.Any("prev", prev.Version),
					zap.Any("cur", cur.Version))
			} else if cur.UncommittedLLSNOffset > prev.UncommittedLLSNOffset ||
				cur.UncommittedLLSNEnd() > prev.UncommittedLLSNEnd() {
				diff.UncommitReports = append(diff.UncommitReports, cur)
			}
			i++
			j++
		}
	}

	for ; i < report.Len(); i++ {
		cur := report.UncommitReports[i]
		if !cur.Invalid() {
			diff.UncommitReports = append(diff.UncommitReports, cur)
		}
	}

	for _, r := range diff.UncommitReports {
		rce.sampleTracer.report(r.LogStreamID, rce.storageNodeID, r)
	}

	return diff
}

func (rce *reportCollectExecutor) closeClient(cli reportcommitter.Client) {
	rce.snConnector.mu.Lock()
	defer rce.snConnector.mu.Unlock()

	if rce.snConnector.cli != nil &&
		(rce.snConnector.cli == cli || cli == nil) {
		rce.snConnector.cli.Close() //nolint:errcheck,revive // TODO:: Handle an error returned.
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

func (rce *reportCollectExecutor) getReportedVersion(lsID types.LogStreamID) (types.Version, bool) {
	report, ok := rce.reportCtx.getReport()
	if !ok {
		return types.InvalidVersion, false
	}

	r, ok := report.LookupReport(lsID)
	if !ok {
		return types.InvalidVersion, false
	}

	return r.Version, true
}

func (rce *reportCollectExecutor) getLastCommitResults() *mrpb.LogStreamCommitResults {
	return rce.helper.GetLastCommitResults()
}

func (rce *reportCollectExecutor) lookupNextCommitResults(ver types.Version) (*mrpb.LogStreamCommitResults, error) {
	return rce.helper.LookupNextCommitResults(ver)
}

func newLogStreamCommitter(topicID types.TopicID, lsID types.LogStreamID, helper commitHelper, ver types.Version, status varlogpb.LogStreamStatus, sampleTracer *sampleTracer, tmStub *telemetryStub, logger *zap.Logger) *logStreamCommitter {
	c := &logStreamCommitter{
		topicID:      topicID,
		lsID:         lsID,
		helper:       helper,
		logger:       logger,
		sampleTracer: sampleTracer,
		tmStub:       tmStub,
	}

	c.commitStatus.status = status
	c.commitStatus.beginVersion = ver

	return c
}

func (lc *logStreamCommitter) getCatchupVersion(resetCatchupHelper bool) (types.Version, bool) {
	if resetCatchupHelper {
		lc.setSentVersion(types.InvalidVersion)
	}

	status, beginVer := lc.getCommitStatus()
	if status.Sealed() {
		return types.InvalidVersion, false
	}

	ver, ok := lc.helper.getReportedVersion(lc.lsID)
	if !ok {
		return types.InvalidVersion, false
	}

	sent := lc.getSentVersion()
	if sent > ver {
		ver = sent
	}

	if beginVer > ver {
		if ver.Invalid() {
			// Let newbie logStream know version.
			return beginVer - 1, true
		}
		return beginVer, true
	}

	return ver, true
}

func (lc *logStreamCommitter) commitResult(ctx context.Context, reset bool) snpb.LogStreamCommitResult {
	crs := lc.helper.getLastCommitResults()
	if crs == nil {
		return snpb.InvalidLogStreamCommitResult
	}

	ver, ok := lc.getCatchupVersion(reset)
	if !ok || ver >= crs.Version {
		return snpb.InvalidLogStreamCommitResult
	}

CatchupLoop:
	for ctx.Err() == nil {
		if ver+1 != crs.Version {
			tmp, err := lc.helper.lookupNextCommitResults(ver)
			if err != nil {
				latestVersion, ok := lc.getCatchupVersion(false)
				if !ok {
					return snpb.InvalidLogStreamCommitResult
				}

				if latestVersion > ver {
					ver = latestVersion
					continue CatchupLoop
				}

				return snpb.InvalidLogStreamCommitResult
			}

			if tmp == nil {
				return snpb.InvalidLogStreamCommitResult
			}

			crs = tmp
		}

		cr, expectedPos, ok := crs.LookupCommitResult(lc.topicID, lc.lsID, lc.catchupHelper.expectedPos)
		if ok {
			lc.catchupHelper.expectedPos = expectedPos

			cr.Version = crs.Version
			cr.HighWatermark, lc.catchupHelper.expectedEndPos = crs.LastHighWatermark(lc.topicID, lc.catchupHelper.expectedEndPos)
		} else {
			// Let newbie LogStream know Version.
			cr.Version = crs.Version
			cr.TopicID = lc.topicID
			cr.LogStreamID = lc.lsID
			cr.CommittedGLSNOffset = types.MinGLSN
			cr.CommittedLLSNOffset = types.MinLLSN
			cr.CommittedGLSNLength = 0
		}

		min, max, recordable := lc.sampleTracer.commit(lc.lsID, cr.CommittedLLSNOffset, cr.CommittedLLSNOffset+types.LLSN(cr.CommittedGLSNLength))
		if recordable {
			lc.tmStub.mb.Records("mr.report_commit.delay").Record(ctx, float64(time.Since(min).Nanoseconds())/float64(time.Millisecond))
			lc.tmStub.mb.Records("mr.replicate.delay").Record(ctx, float64(max.Sub(min).Nanoseconds())/float64(time.Millisecond))
		}

		lc.setSentVersion(crs.Version)
		return cr
	}

	return snpb.InvalidLogStreamCommitResult
}

func (lc *logStreamCommitter) seal() {
	lc.commitStatus.mu.Lock()
	defer lc.commitStatus.mu.Unlock()

	lc.commitStatus.status = varlogpb.LogStreamStatusSealed
}

func (lc *logStreamCommitter) unseal(ver types.Version) {
	lc.commitStatus.mu.Lock()
	defer lc.commitStatus.mu.Unlock()

	lc.commitStatus.status = varlogpb.LogStreamStatusRunning
	lc.commitStatus.beginVersion = ver
}

func (lc *logStreamCommitter) getCommitStatus() (varlogpb.LogStreamStatus, types.Version) {
	lc.commitStatus.mu.RLock()
	defer lc.commitStatus.mu.RUnlock()

	return lc.commitStatus.status, lc.commitStatus.beginVersion
}

func (lc *logStreamCommitter) getSentVersion() types.Version {
	if time.Since(lc.catchupHelper.sentAt) > DefaultCatchupRefreshTime {
		return types.InvalidVersion
	}
	return lc.catchupHelper.sentVersion
}

func (lc *logStreamCommitter) setSentVersion(ver types.Version) {
	lc.catchupHelper.sentVersion = ver
	lc.catchupHelper.sentAt = time.Now()
}

func (rc *reportContext) saveReport(report *mrpb.StorageNodeUncommitReport) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.report = mrpb.NewStorageNodeUncommitReport(report.StorageNodeID)
	rc.report.UncommitReports = report.UncommitReports
}

func (rc *reportContext) swapReport(newReport *mrpb.StorageNodeUncommitReport) (old mrpb.StorageNodeUncommitReport, ok bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.report != nil {
		old = *rc.report
		ok = true
		rc.report.Release()
	}

	rc.report = mrpb.NewStorageNodeUncommitReport(newReport.StorageNodeID)
	rc.report.UncommitReports = newReport.UncommitReports

	return old, ok
}

func (rc *reportContext) getReport() (report mrpb.StorageNodeUncommitReport, ok bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.report != nil {
		report = *rc.report
		ok = true
	}

	return report, ok
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

func (st *sampleTracer) report(lsID types.LogStreamID, snID types.StorageNodeID, r snpb.LogStreamUncommitReport) {
	if r.UncommittedLLSNLength == 0 {
		return
	}

	llsn := r.UncommittedLLSNOffset + types.LLSN(st.sampleRate-1)
	llsn = llsn - (llsn % types.LLSN(st.sampleRate))

	for llsn < r.UncommittedLLSNEnd() {
		f, _ := st.m.LoadOrStore(lsID, &sampleReports{llsn: llsn})
		sample := f.(*sampleReports)
		sample.report(snID, llsn)

		llsn += types.LLSN(st.sampleRate)
	}
}

func (st *sampleTracer) commit(lsID types.LogStreamID, begin, end types.LLSN) (min, max time.Time, recordable bool) {
	if f, ok := st.m.Load(lsID); ok {
		sr := f.(*sampleReports)

		sr.mu.Lock()
		defer sr.mu.Unlock()

		if sr.committed {
			return
		}

		if end > sr.llsn {
			sr.committed = true

			min, max = sr.minmax()
			recordable = begin <= sr.llsn && len(sr.created) > 0
		}
	}

	return
}

func (sr *sampleReports) reset(llsn types.LLSN) {
	sr.llsn = llsn
	sr.committed = false
	sr.created = make(map[types.StorageNodeID]time.Time)
}

func (sr *sampleReports) update(snID types.StorageNodeID) {
	if sr.created == nil {
		sr.created = make(map[types.StorageNodeID]time.Time)
	}

	if _, ok := sr.created[snID]; !ok {
		sr.created[snID] = time.Now()
	}
}

func (sr *sampleReports) report(snID types.StorageNodeID, llsn types.LLSN) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.llsn == llsn {
		sr.update(snID)
	} else if sr.llsn < llsn {
		if sr.committed {
			if sr.skip < llsn {
				sr.reset(llsn)
				sr.update(snID)
			}
		} else {
			if sr.skip < llsn {
				sr.skip = llsn
			}
		}
	}
}

func (sr *sampleReports) minmax() (time.Time, time.Time) {
	min := time.Now()
	max := time.Time{}

	for _, r := range sr.created {
		if max.Before(r) {
			max = r
		}

		if min.After(r) {
			min = r
		}
	}

	return min, max
}

func (rce *reportCollectExecutor) makeCommitResults(ctx context.Context, reset bool) []snpb.LogStreamCommitResult {
	rce.cmmu.RLock()
	defer rce.cmmu.RUnlock()

	crs := make([]snpb.LogStreamCommitResult, 0, len(rce.committers))
	for _, lc := range rce.committers {
		cr := lc.commitResult(ctx, reset)
		if !cr.LogStreamID.Invalid() {
			crs = append(crs, cr)
		}
	}

	return crs
}

func (rce *reportCollectExecutor) catchupBatch(ctx context.Context) {
	for {
		cli, err := rce.getClient(ctx)
		if err != nil {
			rce.logger.Debug("getClient", zap.Error(err))
			return
		}

		reset := cli != rce.knownCli
		rce.knownCli = cli

		commits := rce.makeCommitResults(ctx, reset)
		if len(commits) == 0 {
			return
		}

		commitBatchRequest := snpb.CommitBatchRequest{
			StorageNodeID: rce.storageNodeID,
			CommitResults: commits,
		}

		if err := cli.CommitBatch(commitBatchRequest); err != nil {
			rce.logger.Debug("commitBatch", zap.Error(err))
			rce.closeClient(cli)
			return
		}
	}
}

func (rce *reportCollectExecutor) runCommit(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-rce.triggerC:
			rce.catchupBatch(ctx)
		}
	}

	close(rce.triggerC)
}
