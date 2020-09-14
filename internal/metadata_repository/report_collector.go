package metadata_repository

import (
	"context"
	"sync"
	"time"

	"github.com/kakao/varlog/internal/storage"
	varlog "github.com/kakao/varlog/pkg/varlog"
	types "github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	"go.uber.org/zap"
)

const RPC_TIMEOUT = 100 * time.Millisecond

type ReportCollectorCallbacks struct {
	report        func(*snpb.LocalLogStreamDescriptor) error
	getClient     func(*varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error)
	lookupNextGLS func(types.GLSN) *snpb.GlobalLogStreamDescriptor
}

type ReportCollectExecutor struct {
	highWatermark types.GLSN
	logStreamIDs  []types.LogStreamID
	cli           storage.LogStreamReporterClient
	sn            *varlogpb.StorageNodeDescriptor
	cb            ReportCollectorCallbacks
	lsmu          sync.RWMutex
	clmu          sync.RWMutex
	resultC       chan *snpb.GlobalLogStreamDescriptor
	logger        *zap.Logger
	cancel        context.CancelFunc
}

type ReportCollector struct {
	executors map[types.StorageNodeID]*ReportCollectExecutor
	cb        ReportCollectorCallbacks
	mu        sync.RWMutex
	logger    *zap.Logger
	runner    runner.Runner
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewReportCollector(cb ReportCollectorCallbacks, logger *zap.Logger) *ReportCollector {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReportCollector{
		logger:    logger,
		cb:        cb,
		ctx:       ctx,
		cancel:    cancel,
		executors: make(map[types.StorageNodeID]*ReportCollectExecutor),
	}
}

func (rc *ReportCollector) Run() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.ctx == nil {
		rc.ctx, rc.cancel = context.WithCancel(context.Background())
	}
}

func (rc *ReportCollector) Close() {
	rc.mu.Lock()
	if rc.cancel != nil {
		rc.cancel()

		rc.ctx = nil
		rc.cancel = nil
	}
	rc.executors = make(map[types.StorageNodeID]*ReportCollectExecutor)
	rc.mu.Unlock()

	rc.runner.CloseWaitDeprecated()
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
		return varlog.ErrInvalid
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.ctx == nil {
		return varlog.ErrInternal
	}

	if _, ok := rc.executors[sn.StorageNodeID]; ok {
		return varlog.ErrExist
	}

	executor := &ReportCollectExecutor{
		resultC:       make(chan *snpb.GlobalLogStreamDescriptor, 1),
		highWatermark: glsn,
		sn:            sn,
		cb:            rc.cb,
		logger:        rc.logger.Named("executor"),
	}
	ctx, cancel := context.WithCancel(rc.ctx)
	executor.cancel = cancel

	rc.executors[sn.StorageNodeID] = executor

	rc.runner.RunDeprecated(ctx, executor.runCommit)
	rc.runner.RunDeprecated(ctx, executor.runReport)

	return nil
}

func (rc *ReportCollector) UnregisterStorageNode(snID types.StorageNodeID) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.ctx == nil {
		return varlog.ErrInternal
	}

	executor, ok := rc.executors[snID]
	if !ok {
		return varlog.ErrNotExist
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

	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
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

	lsIDs := make([]types.LogStreamID, len(lls.Uncommit))
	for i, u := range lls.Uncommit {
		lsIDs[i] = u.LogStreamID
	}

	if lls.HighWatermark.Invalid() {
		lls.HighWatermark = rce.highWatermark
	} else if lls.HighWatermark < rce.highWatermark {
		rce.logger.Panic("invalid GLSN",
			zap.Int32("SNID", int32(rce.sn.StorageNodeID)),
			zap.Uint64("REPORT", uint64(lls.HighWatermark)),
			zap.Uint64("CUR", uint64(rce.highWatermark)),
		)
	}

	rce.lsmu.Lock()
	rce.highWatermark = lls.HighWatermark
	rce.logStreamIDs = lsIDs
	rce.lsmu.Unlock()

	rce.logger.Debug("report",
		zap.Int32("SNID", int32(rce.sn.StorageNodeID)),
		zap.Uint64("HWM", uint64(lls.HighWatermark)),
		zap.Int("UNCOMMIT", len(lls.Uncommit)),
	)
	rce.cb.report(lls)

	return nil
}

func (rce *ReportCollectExecutor) commit(gls *snpb.GlobalLogStreamDescriptor) error {
	cli, err := rce.getClient()
	if err != nil {
		return err
	}

	r := snpb.GlobalLogStreamDescriptor{
		HighWatermark:     gls.HighWatermark,
		PrevHighWatermark: gls.PrevHighWatermark,
	}

	rce.lsmu.RLock()
	lsIDs := rce.logStreamIDs
	rce.lsmu.RUnlock()

	r.CommitResult = make([]*snpb.GlobalLogStreamDescriptor_LogStreamCommitResult, 0, len(lsIDs))

	for _, lsID := range lsIDs {
		c := getCommitResultFromGLS(gls, lsID)
		if c != nil {
			r.CommitResult = append(r.CommitResult, c)
		}
	}

	if len(r.CommitResult) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
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
Loop:
	for highWatermark < gls.HighWatermark {
		prev := rce.cb.lookupNextGLS(highWatermark)
		if prev == nil {
			tmp := rce.getHighWatermark()
			if tmp <= highWatermark {
				rce.logger.Panic("prev gls should not be nil",
					zap.Int32("SNID", int32(rce.sn.StorageNodeID)),
					zap.Uint64("FROM", uint64(highWatermark)),
					zap.Uint64("CUR", uint64(gls.HighWatermark)),
				)
			} else {
				highWatermark = tmp
				continue Loop
			}
		}

		err := rce.commit(prev)
		if err != nil {
			return err
		}

		if prev.HighWatermark <= highWatermark {
			rce.logger.Panic("broken global commit consistency",
				zap.Int32("SNID", int32(rce.sn.StorageNodeID)),
				zap.Uint64("expected", uint64(gls.PrevHighWatermark)),
				zap.Uint64("prev", uint64(prev.HighWatermark)),
				zap.Uint64("cur", uint64(highWatermark)),
			)
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

func (rce *ReportCollectExecutor) closeClient(cli storage.LogStreamReporterClient) {
	rce.clmu.Lock()
	defer rce.clmu.Unlock()

	if rce.cli != nil &&
		(rce.cli == cli || cli == nil) {
		rce.cli.Close()
		rce.cli = nil
	}
}

func (rce *ReportCollectExecutor) getClient() (storage.LogStreamReporterClient, error) {
	rce.clmu.RLock()
	cli := rce.cli
	rce.clmu.RUnlock()

	if cli != nil {
		return cli, nil
	}

	rce.clmu.Lock()
	defer rce.clmu.Unlock()

	var err error

	rce.cli, err = rce.cb.getClient(rce.sn)
	return rce.cli, err
}
