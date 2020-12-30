package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type EmptyReporterClient struct {
}

func (rc *EmptyReporterClient) GetReport(ctx context.Context) (*snpb.GetReportResponse, error) {
	return &snpb.GetReportResponse{}, nil
}

func (rc *EmptyReporterClient) Commit(ctx context.Context, gls *snpb.CommitRequest) error {
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

func (rcf *EmptyReporterClientFactory) GetClient(*varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error) {
	return &EmptyReporterClient{}, nil
}

type DummyReporterClientStatus int32

const DefaultDelay time.Duration = 500 * time.Microsecond

const (
	DUMMY_REPORTERCLIENT_STATUS_RUNNING DummyReporterClientStatus = iota
	DUMMY_REPORTERCLIENT_STATUS_CLOSED
	DUMMY_REPORTERCLIENT_STATUS_CRASH
)

type DummyReporterClient struct {
	storageNodeID types.StorageNodeID

	logStreamIDs          []types.LogStreamID
	knownHighWatermark    []types.GLSN
	uncommittedLLSNOffset []types.LLSN
	uncommittedLLSNLength []uint64

	manual bool
	mu     sync.Mutex

	status  DummyReporterClientStatus
	factory *DummyReporterClientFactory

	ref int
}

func (r *DummyReporterClient) incrRef() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ref += 1
}

func (r *DummyReporterClient) descRef() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ref > 0 {
		r.ref -= 1
	}
}

func (r *DummyReporterClient) getRef() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.ref
}

type DummyReporterClientFactory struct {
	manual       bool
	nrLogStreams int
	m            sync.Map
}

func NewDummyReporterClientFactory(nrLogStreams int, manual bool) *DummyReporterClientFactory {
	a := &DummyReporterClientFactory{
		nrLogStreams: nrLogStreams,
		manual:       manual,
	}

	return a
}

func (a *DummyReporterClientFactory) GetClient(sn *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error) {
	status := DUMMY_REPORTERCLIENT_STATUS_RUNNING

	LSIDs := make([]types.LogStreamID, a.nrLogStreams)
	for i := 0; i < a.nrLogStreams; i++ {
		LSIDs[i] = types.LogStreamID(sn.StorageNodeID) + types.LogStreamID(i)
	}

	knownHighWatermark := make([]types.GLSN, a.nrLogStreams)

	uncommittedLLSNOffset := make([]types.LLSN, a.nrLogStreams)
	for i := 0; i < a.nrLogStreams; i++ {
		uncommittedLLSNOffset[i] = types.MinLLSN
	}

	uncommittedLLSNLength := make([]uint64, a.nrLogStreams)

	cli := &DummyReporterClient{
		manual:                a.manual,
		storageNodeID:         sn.StorageNodeID,
		logStreamIDs:          LSIDs,
		knownHighWatermark:    knownHighWatermark,
		uncommittedLLSNOffset: uncommittedLLSNOffset,
		uncommittedLLSNLength: uncommittedLLSNLength,
		status:                status,
		factory:               a,
	}

	f, _ := a.m.LoadOrStore(sn.StorageNodeID, cli)

	cli = f.(*DummyReporterClient)
	cli.incrRef()

	return cli, nil
}

func (r *DummyReporterClient) GetReport(ctx context.Context) (*snpb.GetReportResponse, error) {
	time.Sleep(DefaultDelay)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == DUMMY_REPORTERCLIENT_STATUS_CRASH {
		return nil, errors.New("crash")
	} else if r.status == DUMMY_REPORTERCLIENT_STATUS_CLOSED {
		return nil, errors.New("closed")
	}

	if !r.manual {
		for i := range r.logStreamIDs {
			r.uncommittedLLSNLength[i]++
		}
	}

	lls := &snpb.GetReportResponse{
		StorageNodeID: r.storageNodeID,
	}

	for i, lsID := range r.logStreamIDs {
		u := &snpb.LogStreamUncommitReport{
			LogStreamID:           lsID,
			HighWatermark:         r.knownHighWatermark[i],
			UncommittedLLSNOffset: r.uncommittedLLSNOffset[i],
			UncommittedLLSNLength: r.uncommittedLLSNLength[i],
		}
		lls.UncommitReports = append(lls.UncommitReports, u)
	}

	return lls, nil
}

func (r *DummyReporterClient) Commit(ctx context.Context, cr *snpb.CommitRequest) error {
	time.Sleep(DefaultDelay)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == DUMMY_REPORTERCLIENT_STATUS_CRASH {
		return errors.New("crash")
	} else if r.status == DUMMY_REPORTERCLIENT_STATUS_CLOSED {
		return errors.New("closed")
	}

	for _, result := range cr.CommitResults {
		idx := int(result.LogStreamID - types.LogStreamID(r.storageNodeID))
		if idx < 0 || idx >= len(r.logStreamIDs) {
			return errors.New("invalid log stream ID")
		}

		if r.knownHighWatermark[idx] != result.PrevHighWatermark {
			continue
		}

		r.uncommittedLLSNOffset[idx] += types.LLSN(result.CommittedGLSNLength)
		r.uncommittedLLSNLength[idx] -= result.CommittedGLSNLength
		r.knownHighWatermark[idx] = result.HighWatermark
	}

	return nil
}

func (r *DummyReporterClient) Close() error {
	r.descRef()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status != DUMMY_REPORTERCLIENT_STATUS_CRASH &&
		r.ref == 0 {
		r.factory.m.Delete(r.storageNodeID)
		r.status = DUMMY_REPORTERCLIENT_STATUS_CLOSED
	}

	return nil
}

func (a *DummyReporterClientFactory) lookupClient(snID types.StorageNodeID) *DummyReporterClient {
	f, ok := a.m.Load(snID)
	if !ok {
		return nil
	}

	return f.(*DummyReporterClient)
}

func (r *DummyReporterClient) increaseUncommitted(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if idx < 0 || idx >= len(r.uncommittedLLSNLength) {
		return
	}

	r.uncommittedLLSNLength[idx]++
}

func (r *DummyReporterClient) numUncommitted(idx int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if idx < 0 || idx >= len(r.uncommittedLLSNLength) {
		return 0
	}

	return r.uncommittedLLSNLength[idx]
}

func (r *DummyReporterClient) getKnownHighWatermark(idx int) types.GLSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.knownHighWatermark[idx]
}

func (a *DummyReporterClientFactory) crashRPC(snID types.StorageNodeID) {
	f, ok := a.m.Load(snID)
	if !ok {
		fmt.Printf("notfound\n")
		return
	}

	cli := f.(*DummyReporterClient)

	cli.mu.Lock()
	defer cli.mu.Unlock()

	cli.status = DUMMY_REPORTERCLIENT_STATUS_CRASH
}

func (a *DummyReporterClientFactory) recoverRPC(snID types.StorageNodeID) {
	f, ok := a.m.Load(snID)
	if !ok {
		return
	}

	old := f.(*DummyReporterClient)

	old.mu.Lock()
	defer old.mu.Unlock()

	cli := &DummyReporterClient{
		manual:                old.manual,
		storageNodeID:         old.storageNodeID,
		logStreamIDs:          old.logStreamIDs,
		knownHighWatermark:    old.knownHighWatermark,
		uncommittedLLSNOffset: old.uncommittedLLSNOffset,
		uncommittedLLSNLength: old.uncommittedLLSNLength,
		status:                DUMMY_REPORTERCLIENT_STATUS_RUNNING,
		factory:               old.factory,
	}

	a.m.Store(snID, cli)
}
