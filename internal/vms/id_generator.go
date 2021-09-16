package vms

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/types"
)

type LogStreamIDGenerator interface {
	// Generate returns conflict-free LogStreamID. If the returned identifier is duplicated, it
	// means that the varlog cluster consistency is broken.
	Generate() types.LogStreamID

	// Refresh renews LogStreamIDGenerator to update the latest cluster metadata.
	Refresh(ctx context.Context) error
}

type TopicIDGenerator interface {
	// Generate returns conflict-free TopicID. If the returned identifier is duplicated, it
	// means that the varlog cluster consistency is broken.
	Generate() types.TopicID

	// Refresh renews TopicIDGenerator to update the latest cluster metadata.
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

func (gen *seqLSIDGen) getMaxLogStreamID(ctx context.Context) (maxID types.LogStreamID, err error) {
	clusmeta, err := gen.cmView.ClusterMetadata(ctx)
	if err != nil {
		return maxID, err
	}

	sndescList := clusmeta.GetStorageNodes()
	snidList := make([]types.StorageNodeID, 0, len(sndescList))
	for _, sndesc := range sndescList {
		snidList = append(snidList, sndesc.GetStorageNodeID())
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	for _, snid := range snidList {
		storageNodeID := snid
		g.Go(func() error {
			localMaxID, e := getLocalMaxLogStreamID(ctx, storageNodeID, gen.snMgr)
			if e != nil {
				return e
			}
			mu.Lock()
			if maxID < localMaxID {
				maxID = localMaxID
			}
			mu.Unlock()
			return nil
		})
	}
	return maxID, g.Wait()
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

// TODO: seqTopicIDGen does not consider the restart of VMS.
type seqTopicIDGen struct {
	seq types.TopicID
	mu  sync.Mutex

	cmView ClusterMetadataView
	snMgr  StorageNodeManager
}

func NewSequentialTopicIDGenerator(ctx context.Context, cmView ClusterMetadataView, snMgr StorageNodeManager) (TopicIDGenerator, error) {
	gen := &seqTopicIDGen{
		cmView: cmView,
		snMgr:  snMgr,
	}
	if err := gen.Refresh(ctx); err != nil {
		return nil, err
	}
	return gen, nil
}

func (gen *seqTopicIDGen) Generate() types.TopicID {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	gen.seq++
	return gen.seq
}

func (gen *seqTopicIDGen) Refresh(ctx context.Context) error {
	maxID, err := gen.getMaxTopicID(ctx)
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

func (gen *seqTopicIDGen) getMaxTopicID(ctx context.Context) (maxID types.TopicID, err error) {
	clusmeta, err := gen.cmView.ClusterMetadata(ctx)
	if err != nil {
		return maxID, err
	}

	topicDescs := clusmeta.GetTopics()
	for _, topicDesc := range topicDescs {
		if maxID < topicDesc.TopicID {
			maxID = topicDesc.TopicID
		}
	}

	return maxID, nil
}
