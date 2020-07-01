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
	storageNodeID types.StorageNodeID
	knownNextGLSN types.GLSN

	logStreamID          types.LogStreamID
	uncommittedLLSNBegin types.LLSN
	uncommittedLLSNEnd   types.LLSN

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
		manual:        a.manual,
		storageNodeID: sn.StorageNodeID,
		logStreamID:   types.LogStreamID(sn.StorageNodeID),
	}

	a.m[sn.StorageNodeID] = cli

	return cli, nil
}

func (r *DummyReporterClient) GetReport(ctx context.Context) (*snpb.LocalLogStreamDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.manual {
		r.uncommittedLLSNEnd++
	}

	lls := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: r.storageNodeID,
		NextGLSN:      r.knownNextGLSN,
		Uncommit: []*snpb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			{
				LogStreamID:          r.logStreamID,
				UncommittedLLSNBegin: r.uncommittedLLSNBegin,
				UncommittedLLSNEnd:   r.uncommittedLLSNEnd,
			},
		},
	}

	return lls, nil
}

func (r *DummyReporterClient) Commit(ctx context.Context, glsn *snpb.GlobalLogStreamDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if glsn.PrevNextGLSN != r.knownNextGLSN {
		return nil
	}

	r.knownNextGLSN = glsn.NextGLSN

	for _, result := range glsn.CommitResult {
		if result.LogStreamID != r.logStreamID {
			return errors.New("invalid log stream ID")
		}

		nrCommitted := uint64(result.CommittedGLSNEnd - result.CommittedGLSNBegin)
		r.uncommittedLLSNBegin += types.LLSN(nrCommitted)
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

	r.uncommittedLLSNEnd++
}

func (r *DummyReporterClient) numUncommitted() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return uint64(r.uncommittedLLSNEnd - r.uncommittedLLSNBegin)
}
