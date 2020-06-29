package metadata_repository

import (
	"sync"

	"github.com/kakao/varlog/internal/storage"
	varlog "github.com/kakao/varlog/pkg/varlog"
	types "github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	"go.uber.org/zap"
)

type ReportCollectorCallbacks struct {
	connect    func(*varlogpb.StorageNodeDescriptor) (storage.LogStreamRepoterClient, error)
	report     func(*snpb.LocalLogStreamDescriptor)
	getNextGLS func(types.GLSN) *snpb.GlobalLogStreamDescriptor
}

type ReportCollectExecutor struct {
	nextGLSN     types.GLSN
	logStreamIDs []types.LogStreamID
	cli          storage.LogStreamRepoterClient
	sn           *varlogpb.StorageNodeDescriptor
	cb           ReportCollectorCallbacks
	mu           sync.RWMutex
	resultC      chan *snpb.GlobalLogStreamDescriptor
	stopC        chan struct{}
	logger       *zap.Logger
}

type ReportCollector struct {
	executors map[types.StorageNodeID]*ReportCollectExecutor
	cb        ReportCollectorCallbacks
	mu        sync.RWMutex
	wg        sync.WaitGroup
	logger    *zap.Logger
}

func NewReportCollector(cb ReportCollectorCallbacks, logger *zap.Logger) *ReportCollector {
	return &ReportCollector{
		logger:    logger,
		cb:        cb,
		executors: make(map[types.StorageNodeID]*ReportCollectExecutor),
	}
}

func (rc *ReportCollector) Close() {
	rc.mu.RLock()

	for _, executor := range rc.executors {
		close(executor.stopC)
	}

	rc.mu.RUnlock()

	rc.wg.Wait()
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

	cli, err := rc.cb.connect(sn)
	if err != nil {
		panic(err)
	}

	executor := &ReportCollectExecutor{
		//TODO::
		resultC: make(chan *snpb.GlobalLogStreamDescriptor, 1024),
		stopC:   make(chan struct{}),
		sn:      sn,
		cb:      rc.cb,
		cli:     cli,
		logger:  rc.logger.Named("executor"),
	}

	rc.executors[sn.StorageNodeID] = executor
	executor.run(&rc.wg)

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

func (rce *ReportCollectExecutor) run(wg *sync.WaitGroup) {
	wg.Add(2)

	go rce.runCommit(wg)
	go rce.runReport(wg)
}

func (rce *ReportCollectExecutor) runReport(wg *sync.WaitGroup) {
	defer wg.Done()

Loop:
	for {
		select {
		case <-rce.stopC:
			break Loop
		default:
			err := rce.getReport()
			if err != nil {
				//TODO:: reconnect
				rce.logger.Panic(err.Error())
			}
		}
	}

	close(rce.resultC)

	//TODO:: close cli
}

func (rce *ReportCollectExecutor) runCommit(wg *sync.WaitGroup) {
	defer wg.Done()

	for gls := range rce.resultC {
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

			nextGLSN = prev.NextGLSN
		}

		err := rce.commit(gls)
		if err != nil {
			rce.logger.Panic(err.Error())
		}
	}

	//TODO:: close cli
}

func (rce *ReportCollectExecutor) getReport() error {
	lls, err := rce.cli.GetReport()
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

	rce.cb.report(&lls)

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

	err := rce.cli.Commit(r)
	if err != nil {
		return err
	}

	return nil
}
