package metadata_repository

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

const DEFAULT_REPORT_ALL_DUR = time.Second

type ReportCollectorCallbacks struct {
	report        func(*snpb.LocalLogStreamDescriptor) error
	getClient     func(*varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error)
	lookupNextGLS func(types.GLSN) *snpb.GlobalLogStreamDescriptor
	getOldestGLS  func() *snpb.GlobalLogStreamDescriptor
}

type ReportCollectExecutor struct {
	highWatermark   types.GLSN
	lastReport      *snpb.LocalLogStreamDescriptor
	lastReportAllAt time.Time
	cli             storagenode.LogStreamReporterClient
	sn              *varlogpb.StorageNodeDescriptor
	cb              ReportCollectorCallbacks
	rpcTimeout      time.Duration
	lsmu            sync.RWMutex
	clmu            sync.RWMutex
	resultC         chan *snpb.GlobalLogStreamDescriptor
	logger          *zap.Logger
	cancel          context.CancelFunc
}

type ReportCollector struct {
	executors  map[types.StorageNodeID]*ReportCollectExecutor
	cb         ReportCollectorCallbacks
	rpcTimeout time.Duration
	mu         sync.RWMutex
	logger     *zap.Logger
	cancel     context.CancelFunc
	runner     *runner.Runner
	closed     bool
}

func NewReportCollector(cb ReportCollectorCallbacks, rpcTimeout time.Duration, logger *zap.Logger) *ReportCollector {
	return &ReportCollector{
		logger:     logger,
		cb:         cb,
		rpcTimeout: rpcTimeout,
		executors:  make(map[types.StorageNodeID]*ReportCollectExecutor),
	}
}

func (rc *ReportCollector) running() bool {
	return !rc.closed && rc.runner.State() == runner.Running
}

func (rc *ReportCollector) Run() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if !rc.running() {
		rc.runner = runner.New("rc", rc.logger)
	}
}

func (rc *ReportCollector) reset() {
	if rc.running() {
		rc.executors = make(map[types.StorageNodeID]*ReportCollectExecutor)

		rc.runner.Stop()
	}
}

func (rc *ReportCollector) Reset() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.reset()
}

func (rc *ReportCollector) Close() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.reset()
	rc.closed = true
}

func (rc *ReportCollector) Recover(sns []*varlogpb.StorageNodeDescriptor, highWatermark types.GLSN) error {
	rc.Run()

	for _, sn := range sns {
		err := rc.RegisterStorageNode(sn, highWatermark)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rc *ReportCollector) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor, glsn types.GLSN) error {
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

	executor := &ReportCollectExecutor{
		resultC:       make(chan *snpb.GlobalLogStreamDescriptor, 1),
		highWatermark: glsn,
		sn:            sn,
		cb:            rc.cb,
		rpcTimeout:    rc.rpcTimeout,
		logger:        rc.logger.Named("executor").With(zap.Uint32("snid", uint32(sn.StorageNodeID))),
	}
	ctx, cancel := rc.runner.WithManagedCancel(context.Background())
	executor.cancel = cancel

	if err := rc.runner.RunC(ctx, executor.runCommit); err != nil {
		executor.cancel()
		return err
	}
	if err := rc.runner.RunC(ctx, executor.runReport); err != nil {
		executor.cancel()
		return err
	}

	rc.executors[sn.StorageNodeID] = executor

	return nil
}

func (rc *ReportCollector) UnregisterStorageNode(snID types.StorageNodeID) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if !rc.running() {
		return verrors.ErrStopped
	}

	executor, ok := rc.executors[snID]
	if !ok {
		return verrors.ErrNotExist
	}

	delete(rc.executors, snID)
	executor.cancel()

	return nil
}

func (rc *ReportCollector) Commit(gls *snpb.GlobalLogStreamDescriptor) {
	if gls == nil {
		return
	}

	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if !rc.running() {
		return
	}

	for _, executor := range rc.executors {
		select {
		case executor.resultC <- gls:
		default:
		}
	}
}

func (rce *ReportCollectExecutor) runReport(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			err := rce.getReport()
			if err != nil {
				/*
					rce.logger.Error("getReport fail",
						zap.Uint64("SNID", uint64(rce.sn.StorageNodeID)),
						zap.String("ADDR", rce.sn.Address),
						zap.String("err", err.Error()),
					)
				*/
				continue Loop
			}
		}
	}

	rce.closeClient(nil)
}

func (rce *ReportCollectExecutor) runCommit(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case gls := <-rce.resultC:
			highWatermark := rce.getHighWatermark()

			err := rce.catchup(highWatermark, gls)
			if err != nil {
				/*
					rce.logger.Debug("commit fail",
						zap.Uint64("SNID", uint64(rce.sn.StorageNodeID)),
						zap.String("ADDR", rce.sn.Address),
						zap.String("err", err.Error()),
					)
				*/
				continue Loop
			}

			err = rce.commit(gls)
			if err != nil {
				/*
					rce.logger.Debug("commit fail",
						zap.Uint64("SNID", uint64(rce.sn.StorageNodeID)),
						zap.String("ADDR", rce.sn.Address),
						zap.String("err", err.Error()),
					)
				*/
				continue Loop
			}
		}
	}

	rce.closeClient(nil)
}

func (rce *ReportCollectExecutor) getReport() error {
	cli, err := rce.getClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rce.rpcTimeout)
	defer cancel()
	lls, err := cli.GetReport(ctx)
	if err != nil {
		/*
			rce.logger.Debug("getReport",
				zap.String("err", err.Error()),
			)
		*/
		rce.closeClient(cli)
		return err
	}

	lls = rce.processReport(lls)
	if lls.Len() > 0 {
		rce.cb.report(lls)
	}

	return nil
}

func (rce *ReportCollectExecutor) saveReport(lls *snpb.LocalLogStreamDescriptor) {
	rce.lsmu.Lock()
	rce.highWatermark = lls.HighWatermark
	rce.lastReport = lls
	rce.lsmu.Unlock()
}

func (rce *ReportCollectExecutor) processReport(lls *snpb.LocalLogStreamDescriptor) *snpb.LocalLogStreamDescriptor {
	if lls.HighWatermark.Invalid() {
		lls.HighWatermark = rce.highWatermark
	} else if lls.HighWatermark < rce.highWatermark {
		rce.logger.Panic("invalid GLSN",
			zap.Uint64("REPORT", uint64(lls.HighWatermark)),
			zap.Uint64("CUR", uint64(rce.highWatermark)),
		)
	}

	lls.Sort()

	if rce.lastReport == nil {
		rce.saveReport(lls)
		rce.lastReportAllAt = time.Now()
		return lls
	}

	diff := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: lls.StorageNodeID,
		HighWatermark: lls.HighWatermark,
	}

	merge := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: lls.StorageNodeID,
		HighWatermark: lls.HighWatermark,
	}

	i := 0
	j := 0
	for i < lls.Len() && j < rce.lastReport.Len() {
		cur := lls.Uncommit[i]
		prev := rce.lastReport.Uncommit[j]

		if cur.LogStreamID < prev.LogStreamID {
			diff.Uncommit = append(diff.Uncommit, cur)
			merge.Uncommit = append(merge.Uncommit, cur)
			i++
		} else if prev.LogStreamID < cur.LogStreamID {
			merge.Uncommit = append(merge.Uncommit, prev)
			j++
		} else {
			if cur.UncommittedLLSNOffset > prev.UncommittedLLSNOffset ||
				cur.UncommitedLLSNEnd() > prev.UncommitedLLSNEnd() {
				diff.Uncommit = append(diff.Uncommit, cur)
			}
			merge.Uncommit = append(merge.Uncommit, cur)
			i++
			j++
		}
	}

	if i < lls.Len() {
		diff.Uncommit = append(diff.Uncommit, lls.Uncommit[i:]...)
		merge.Uncommit = append(merge.Uncommit, lls.Uncommit[i:]...)
	} else if j < rce.lastReport.Len() {
		merge.Uncommit = append(merge.Uncommit, rce.lastReport.Uncommit[j:]...)
	}

	rce.saveReport(merge)

	if time.Now().Sub(rce.lastReportAllAt) > DEFAULT_REPORT_ALL_DUR {
		rce.lastReportAllAt = time.Now()
		return lls
	}

	return diff
}

func (rce *ReportCollectExecutor) commit(gls *snpb.GlobalLogStreamDescriptor) error {
	cli, err := rce.getClient()
	if err != nil {
		return err
	}

	rce.lsmu.RLock()
	lastReport := rce.lastReport
	rce.lsmu.RUnlock()

	if lastReport == nil {
		return nil
	}

	r := snpb.GlobalLogStreamDescriptor{
		HighWatermark:     gls.HighWatermark,
		PrevHighWatermark: gls.PrevHighWatermark,
	}

	r.CommitResult = make([]*snpb.GlobalLogStreamDescriptor_LogStreamCommitResult, 0, lastReport.Len())

	for _, u := range lastReport.Uncommit {
		c := getCommitResultFromGLS(gls, u.LogStreamID)
		if c != nil {
			r.CommitResult = append(r.CommitResult, c)
		}
	}

	if len(r.CommitResult) == 0 {
		return nil
	}

	rce.logger.Debug("commit",
		zap.Uint64("hwm", uint64(r.HighWatermark)),
	)

	ctx, cancel := context.WithTimeout(context.Background(), rce.rpcTimeout)
	defer cancel()
	err = cli.Commit(ctx, &r)
	if err != nil {
		/*
			rce.logger.Debug("commit",
				zap.String("err", err.Error()),
			)
		*/
		rce.closeClient(cli)
		return err
	}

	return nil
}

func (rce *ReportCollectExecutor) catchup(highWatermark types.GLSN, gls *snpb.GlobalLogStreamDescriptor) error {
	for highWatermark < gls.HighWatermark {
		prev := rce.cb.lookupNextGLS(highWatermark)
		if prev == nil {
			prev = rce.cb.getOldestGLS()
			if prev == nil || prev.PrevHighWatermark < highWatermark {
				rce.logger.Warn("could not catchup",
					zap.Uint64("known hwm", uint64(highWatermark)),
					zap.Uint64("first prev hwm", uint64(prev.GetPrevHighWatermark())),
				)
				return nil
			}

			rce.logger.Debug("send commit result skipping the sequence",
				zap.Uint64("known hwm", uint64(highWatermark)),
				zap.Uint64("first prev hwm", uint64(prev.GetPrevHighWatermark())),
			)

			if prev.HighWatermark == gls.HighWatermark {
				return nil
			}
		}

		err := rce.commit(prev)
		if err != nil {
			return err
		}

		highWatermark = prev.HighWatermark
	}

	return nil
}

func (rc *ReportCollector) getMinHighWatermark() types.GLSN {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	min := types.MaxGLSN

	for _, e := range rc.executors {
		hwm := e.getHighWatermark()
		if hwm < min {
			min = hwm
		}
	}

	return min
}

func (rce *ReportCollectExecutor) getHighWatermark() types.GLSN {
	rce.lsmu.RLock()
	defer rce.lsmu.RUnlock()

	return rce.highWatermark
}

func (rce *ReportCollectExecutor) closeClient(cli storagenode.LogStreamReporterClient) {
	rce.clmu.Lock()
	defer rce.clmu.Unlock()

	if rce.cli != nil &&
		(rce.cli == cli || cli == nil) {
		rce.cli.Close()
		rce.cli = nil
	}
}

func (rce *ReportCollectExecutor) getClient() (storagenode.LogStreamReporterClient, error) {
	rce.clmu.RLock()
	cli := rce.cli
	rce.clmu.RUnlock()

	if cli != nil {
		return cli, nil
	}

	rce.clmu.Lock()
	defer rce.clmu.Unlock()

	var err error
	if rce.cli == nil {
		rce.cli, err = rce.cb.getClient(rce.sn)
	}
	return rce.cli, err
}
