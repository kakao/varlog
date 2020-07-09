package metadata_repository

import (
	"context"
	"sync"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/runner"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	"go.uber.org/zap"
)

type ReportCollectorCallbacks struct {
	report     func(*snpb.LocalLogStreamDescriptor)
	getClient  func(*varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error)
	getNextGLS func(types.GLSN) *snpb.GlobalLogStreamDescriptor
}

type ReportCollectExecutor struct {
	nextGLSN     types.GLSN
	logStreamIDs []types.LogStreamID
	cli          storage.LogStreamReporterClient
	sn           *varlogpb.StorageNodeDescriptor
	cb           ReportCollectorCallbacks
	mu           sync.RWMutex
	resultC      chan *snpb.GlobalLogStreamDescriptor
	logger       *zap.Logger
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

func (rc *ReportCollector) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
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
		resultC: make(chan *snpb.GlobalLogStreamDescriptor, 1),
		sn:      sn,
		cb:      rc.cb,
		cli:     cli,
		logger:  rc.logger.Named("executor"),
	}

	rc.executors[sn.StorageNodeID] = executor

	rc.runner.Run(rc.ctx, executor.runCommit)
	rc.runner.Run(rc.ctx, executor.runReport)

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
			nextGLSN := rce.nextGLSN
			rce.mu.RUnlock()

			for nextGLSN < gls.PrevNextGLSN {
				prev := rce.cb.getNextGLS(nextGLSN)
				if prev == nil {
					rce.logger.Panic("prev gls should not be nil")
				}

				err := rce.commit(prev)
				if err != nil {
					//TODO:: reconnect
					rce.logger.Panic(err.Error())
				}

				if prev.NextGLSN <= nextGLSN {
					rce.logger.Panic("broken global commit consistency",
						zap.Uint64("expected", uint64(gls.PrevNextGLSN)),
						zap.Uint64("prev", uint64(prev.NextGLSN)),
						zap.Uint64("cur", uint64(nextGLSN)),
					)
				}
				nextGLSN = prev.NextGLSN
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

	rce.mu.Lock()
	rce.nextGLSN = lls.NextGLSN
	rce.logStreamIDs = lsIDs
	rce.mu.Unlock()

	rce.cb.report(lls)

	return nil
}

func (rce *ReportCollectExecutor) commit(gls *snpb.GlobalLogStreamDescriptor) error {
	r := snpb.GlobalLogStreamDescriptor{
		NextGLSN:     gls.NextGLSN,
		PrevNextGLSN: gls.PrevNextGLSN,
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
