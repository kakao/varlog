package metadata_repository

import (
	"context"
	"errors"
	"sync"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type EmptyReporterClient struct {
}

func (rc *EmptyReporterClient) GetReport(ctx context.Context) (*snpb.LocalLogStreamDescriptor, error) {
	return &snpb.LocalLogStreamDescriptor{}, nil
}

func (rc *EmptyReporterClient) Commit(ctx context.Context, gls *snpb.GlobalLogStreamDescriptor) error {
	return nil
}

func (rc *EmptyReporterClient) Close() error {
	return nil
}

type EmptyReporterClientFactory struct {
}

func NewEmptyReporterClientFactory() *EmptyReporterClientFactory {
	return &EmptyReporterClientFactory{}
}

func (rcf *EmptyReporterClientFactory) GetClient(*varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error) {
	return &EmptyReporterClient{}, nil
}

type DummyReporterClient struct {
	storageNodeID      types.StorageNodeID
	knownHighWatermark types.GLSN

	logStreamID           types.LogStreamID
	uncommittedLLSNOffset types.LLSN
	uncommittedLLSNLength uint64

	manual bool
	mu     sync.Mutex
}

type DummyReporterClientFactory struct {
	manual bool
	mu     sync.Mutex
	m      map[types.StorageNodeID]*DummyReporterClient
}

func NewDummyReporterClientFactory(manual bool) *DummyReporterClientFactory {
	a := &DummyReporterClientFactory{
		manual: manual,
		m:      make(map[types.StorageNodeID]*DummyReporterClient),
	}

	return a
}

func (a *DummyReporterClientFactory) GetClient(sn *varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if cli, ok := a.m[sn.StorageNodeID]; ok {
		return cli, nil
	}

	cli := &DummyReporterClient{
		manual:                a.manual,
		storageNodeID:         sn.StorageNodeID,
		logStreamID:           types.LogStreamID(sn.StorageNodeID),
		uncommittedLLSNOffset: types.MinLLSN,
		uncommittedLLSNLength: 0,
	}

	a.m[sn.StorageNodeID] = cli

	return cli, nil
}

func (r *DummyReporterClient) GetReport(ctx context.Context) (*snpb.LocalLogStreamDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.manual {
		r.uncommittedLLSNLength++
	}

	lls := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: r.storageNodeID,
		HighWatermark: r.knownHighWatermark,
		Uncommit: []*snpb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			{
				LogStreamID:           r.logStreamID,
				UncommittedLLSNOffset: r.uncommittedLLSNOffset,
				UncommittedLLSNLength: r.uncommittedLLSNLength,
			},
		},
	}

	return lls, nil
}

func (r *DummyReporterClient) Commit(ctx context.Context, glsn *snpb.GlobalLogStreamDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.knownHighWatermark.Invalid() &&
		glsn.PrevHighWatermark != r.knownHighWatermark {
		return nil
	}

	r.knownHighWatermark = glsn.HighWatermark

	for _, result := range glsn.CommitResult {
		if result.LogStreamID != r.logStreamID {
			return errors.New("invalid log stream ID")
		}

		r.uncommittedLLSNOffset += types.LLSN(result.CommittedGLSNLength)
		r.uncommittedLLSNLength -= result.CommittedGLSNLength
	}

	return nil
}

func (r *DummyReporterClient) Close() error {
	return nil
}

func (a *DummyReporterClientFactory) lookupClient(snID types.StorageNodeID) *DummyReporterClient {
	a.mu.Lock()
	defer a.mu.Unlock()

	if cli, ok := a.m[snID]; ok {
		return cli
	}

	return nil
}

func (r *DummyReporterClient) increaseUncommitted() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.uncommittedLLSNLength++
}

func (r *DummyReporterClient) numUncommitted() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.uncommittedLLSNLength
}
