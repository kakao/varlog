package metadata_repository

import (
	"context"
	"sync"

	"github.com/kakao/varlog/internal/storage"
	varlog "github.com/kakao/varlog/pkg/varlog"
	types "github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/runner"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	"go.uber.org/zap"
)

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
	mu            sync.RWMutex
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

func (rc *ReportCollector) Close() {
	rc.cancel()
	rc.runner.CloseWait()
}

func (rc *ReportCollector) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor, glsn types.GLSN) error {
	if sn == nil {
		return varlog.ErrInvalid
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if _, ok := rc.executors[sn.StorageNodeID]; ok {
		return varlog.ErrExist
	}

	cli, err := rc.cb.getClient(sn)
	if err != nil {
		rc.logger.Panic(err.Error())
	}

	executor := &ReportCollectExecutor{
		resultC:       make(chan *snpb.GlobalLogStreamDescriptor, 1),
		highWatermark: glsn,
		sn:            sn,
		cb:            rc.cb,
		cli:           cli,
		logger:        rc.logger.Named("executor"),
	}
	ctx, cancel := context.WithCancel(rc.ctx)
	executor.cancel = cancel

	rc.executors[sn.StorageNodeID] = executor

	rc.runner.Run(ctx, executor.runCommit)
	rc.runner.Run(ctx, executor.runReport)

	return nil
}

func (rc *ReportCollector) UnregisterStorageNode(snID types.StorageNodeID) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

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
				//TODO:: reconnect
				rce.logger.Panic(err.Error())
			}
		}
	}

	//TODO:: close cli
}

func (rce *ReportCollectExecutor) runCommit(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case gls := <-rce.resultC:
			rce.mu.RLock()
			highWatermark := rce.highWatermark
			rce.mu.RUnlock()

			for highWatermark < gls.PrevHighWatermark {
				prev := rce.cb.lookupNextGLS(highWatermark)
				if prev == nil {
					rce.logger.Panic("prev gls should not be nil",
						zap.Int32("SNID", int32(rce.sn.StorageNodeID)),
						zap.Uint64("FROM", uint64(highWatermark)),
						zap.Uint64("CUR", uint64(gls.HighWatermark)),
					)
				}

				err := rce.commit(prev)
				if err != nil {
					//TODO:: reconnect
					rce.logger.Panic(err.Error())
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

			err := rce.commit(gls)
			if err != nil {
				rce.logger.Panic(err.Error())
			}
		}
	}

	//TODO:: close cli
}

func (rce *ReportCollectExecutor) getReport() error {
	lls, err := rce.cli.GetReport(context.TODO())
	if err != nil {
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

	rce.mu.Lock()
	rce.highWatermark = lls.HighWatermark
	rce.logStreamIDs = lsIDs
	rce.mu.Unlock()

	rce.cb.report(lls)

	return nil
}

func (rce *ReportCollectExecutor) commit(gls *snpb.GlobalLogStreamDescriptor) error {
	r := snpb.GlobalLogStreamDescriptor{
		HighWatermark:     gls.HighWatermark,
		PrevHighWatermark: gls.PrevHighWatermark,
	}

	rce.mu.RLock()
	lsIDs := rce.logStreamIDs
	rce.mu.RUnlock()

	r.CommitResult = make([]*snpb.GlobalLogStreamDescriptor_LogStreamCommitResult, len(lsIDs))

	for i, lsID := range lsIDs {
		c := getCommitResultFromGLS(gls, lsID)
		if c == nil {
			c = &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				LogStreamID: lsID,
			}
		}
		r.CommitResult[i] = c
	}

	err := rce.cli.Commit(context.TODO(), &r)
	if err != nil {
		return err
	}

	return nil
}

func (rc *ReportCollector) getMinHighWatermark() types.GLSN {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	min := types.InvalidGLSN

	for _, e := range rc.executors {
		hwm := e.getHighWatermark()
		if min.Invalid() || hwm < min {
			min = hwm
		}
	}

	return min
}

func (rce *ReportCollectExecutor) getHighWatermark() types.GLSN {
	rce.mu.RLock()
	defer rce.mu.RUnlock()

	return rce.highWatermark
}
