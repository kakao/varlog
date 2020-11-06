package vms

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
)

type LogStreamIDGenerator interface {
	// Generate returns conflict-free LogStreamID. If the returned identifier is duplicated, it
	// means that the varlog cluster consistency is broken.
	Generate() types.LogStreamID

	// Refresh renews LogStreamIDGenerator to update the latest cluster metadata.
	Refresh(ctx context.Context) error
}

// TODO: seqLSIDGen does not consider the restart of VMS.
type seqLSIDGen struct {
	seq types.LogStreamID
	mu  sync.Mutex

	cmView ClusterMetadataView
	snMgr  StorageNodeManager
}

func NewSequentialLogStreamIDGenerator(ctx context.Context, cmView ClusterMetadataView, snMgr StorageNodeManager) (LogStreamIDGenerator, error) {
	gen := &seqLSIDGen{
		cmView: cmView,
		snMgr:  snMgr,
	}
	if err := gen.Refresh(ctx); err != nil {
		return nil, err
	}
	return gen, nil
}

func (gen *seqLSIDGen) Generate() types.LogStreamID {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	gen.seq++
	return gen.seq
}

func (gen *seqLSIDGen) Refresh(ctx context.Context) error {
	maxID, err := gen.getMaxLogStreamID(ctx)
	if err != nil {
		return err
	}

	gen.mu.Lock()
	defer gen.mu.Unlock()
	if gen.seq < maxID {
		gen.seq = maxID
	}
	return nil
}

func (gen *seqLSIDGen) getMaxLogStreamID(ctx context.Context) (types.LogStreamID, error) {
	maxID := types.LogStreamID(0)

	clusmeta, err := gen.cmView.ClusterMetadata(ctx)
	if err != nil {
		return maxID, err
	}

	sndescList := clusmeta.GetStorageNodes()
	snidList := make([]types.StorageNodeID, 0, len(sndescList))
	for _, sndesc := range sndescList {
		snidList = append(snidList, sndesc.GetStorageNodeID())
	}

	maxIDs := make([]types.LogStreamID, len(snidList))
	errs := make([]error, len(snidList))
	runner := runner.New("seq-lsid-gen", zap.NewNop())
	mctx, cancel := runner.WithManagedCancel(ctx)
	defer func() {
		cancel()
		runner.Stop()
	}()

	// TODO (jun): Runner needs graceful stop which waits until tasks are terminated without
	// calling cancel functions.
	var wg sync.WaitGroup
	wg.Add(len(snidList))
	for i, snid := range snidList {
		idx := i
		storageNodeID := snid
		f := func(context.Context) {
			defer wg.Done()
			maxIDs[idx], errs[idx] = getLocalMaxLogStreamID(ctx, storageNodeID, gen.snMgr)
		}
		if err := runner.RunC(mctx, f); err != nil {
			return maxID, err
		}
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return maxID, err
		}
	}

	for _, localMaxID := range maxIDs {
		if localMaxID > maxID {
			maxID = localMaxID
		}
	}
	return maxID, nil
}

func getLocalMaxLogStreamID(ctx context.Context, storageNodeID types.StorageNodeID, snMgr StorageNodeManager) (types.LogStreamID, error) {
	maxID := types.LogStreamID(0)
	snmeta, err := snMgr.GetMetadata(ctx, storageNodeID)
	if err != nil {
		return maxID, err
	}
	lsmetaList := snmeta.GetLogStreams()
	for i := range lsmetaList {
		lsmeta := lsmetaList[i]
		logStreamID := lsmeta.GetLogStreamID()
		if logStreamID > maxID {
			maxID = logStreamID
		}
	}
	return maxID, nil
}
