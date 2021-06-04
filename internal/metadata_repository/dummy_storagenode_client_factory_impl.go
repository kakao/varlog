package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter"
	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type EmptyStorageNodeClient struct {
}

func (rc *EmptyStorageNodeClient) GetReport(ctx context.Context) (*snpb.GetReportResponse, error) {
	return &snpb.GetReportResponse{}, nil
}

func (rc *EmptyStorageNodeClient) Commit(ctx context.Context, gls *snpb.CommitRequest) error {
	return nil
}

func (rc *EmptyStorageNodeClient) Close() error {
	return nil
}

func (r *EmptyStorageNodeClient) PeerAddress() string {
	panic("not implemented")
}
func (r *EmptyStorageNodeClient) PeerStorageNodeID() types.StorageNodeID {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) AddLogStream(ctx context.Context, logStreamID types.LogStreamID, path string) error {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) Unseal(ctx context.Context, logStreamID types.LogStreamID, replicas []snpb.Replica) error {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) Sync(ctx context.Context, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	panic("not implemented")
}

func (r *EmptyStorageNodeClient) GetPrevCommitInfo(ctx context.Context, hwm types.GLSN) (*snpb.GetPrevCommitInfoResponse, error) {
	panic("not implemented")
}

type EmptyStorageNodeClientFactory struct {
}

func NewEmptyStorageNodeClientFactory() *EmptyStorageNodeClientFactory {
	return &EmptyStorageNodeClientFactory{}
}

func (rcf *EmptyStorageNodeClientFactory) GetReporterClient(context.Context, *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return &EmptyStorageNodeClient{}, nil
}

func (rcf *EmptyStorageNodeClientFactory) GetManagementClient(context.Context, types.ClusterID, string, *zap.Logger) (snc.StorageNodeManagementClient, error) {
	return &EmptyStorageNodeClient{}, nil
}

type DummyStorageNodeClientStatus int32

const DefaultDelay time.Duration = 500 * time.Microsecond

const (
	DUMMY_STORAGENODE_CLIENT_STATUS_RUNNING DummyStorageNodeClientStatus = iota
	DUMMY_STORAGENODE_CLIENT_STATUS_CLOSED
	DUMMY_STORAGENODE_CLIENT_STATUS_CRASH
)

type DummyStorageNodeClient struct {
	storageNodeID types.StorageNodeID

	logStreamIDs          []types.LogStreamID
	knownHighWatermark    []types.GLSN
	uncommittedLLSNOffset []types.LLSN
	uncommittedLLSNLength []uint64
	commitResultHistory   [][]snpb.LogStreamCommitInfo

	manual bool
	mu     sync.Mutex

	status  DummyStorageNodeClientStatus
	factory *DummyStorageNodeClientFactory

	reportDelay   atomicutil.AtomicDuration
	commitDelay   atomicutil.AtomicDuration
	disableReport atomicutil.AtomicBool

	ref int
}

func (r *DummyStorageNodeClient) incrRef() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ref += 1
}

func (r *DummyStorageNodeClient) descRef() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ref > 0 {
		r.ref -= 1
	}
}

type DummyStorageNodeClientFactory struct {
	manual       bool
	nrLogStreams int
	m            sync.Map
}

func NewDummyStorageNodeClientFactory(nrLogStreams int, manual bool) *DummyStorageNodeClientFactory {
	fac := &DummyStorageNodeClientFactory{
		nrLogStreams: nrLogStreams,
		manual:       manual,
	}

	return fac
}

func (fac *DummyStorageNodeClientFactory) getStorageNodeClient(ctx context.Context, snID types.StorageNodeID) (*DummyStorageNodeClient, error) {
	status := DUMMY_STORAGENODE_CLIENT_STATUS_RUNNING

	LSIDs := make([]types.LogStreamID, fac.nrLogStreams)
	for i := 0; i < fac.nrLogStreams; i++ {
		LSIDs[i] = types.LogStreamID(snID) + types.LogStreamID(i)
	}

	knownHighWatermark := make([]types.GLSN, fac.nrLogStreams)

	uncommittedLLSNOffset := make([]types.LLSN, fac.nrLogStreams)
	for i := 0; i < fac.nrLogStreams; i++ {
		uncommittedLLSNOffset[i] = types.MinLLSN
	}

	uncommittedLLSNLength := make([]uint64, fac.nrLogStreams)
	commitResultHistory := make([][]snpb.LogStreamCommitInfo, fac.nrLogStreams)

	cli := &DummyStorageNodeClient{
		manual:                fac.manual,
		storageNodeID:         snID,
		logStreamIDs:          LSIDs,
		knownHighWatermark:    knownHighWatermark,
		uncommittedLLSNOffset: uncommittedLLSNOffset,
		uncommittedLLSNLength: uncommittedLLSNLength,
		commitResultHistory:   commitResultHistory,
		status:                status,
		factory:               fac,
		reportDelay:           atomicutil.AtomicDuration(DefaultDelay),
		commitDelay:           atomicutil.AtomicDuration(DefaultDelay),
	}

	f, _ := fac.m.LoadOrStore(snID, cli)

	cli = f.(*DummyStorageNodeClient)
	cli.incrRef()

	return cli, nil
}

func (fac *DummyStorageNodeClientFactory) GetReporterClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return fac.getStorageNodeClient(ctx, sn.StorageNodeID)
}

func (fac *DummyStorageNodeClientFactory) GetManagementClient(ctx context.Context, clusterID types.ClusterID, address string, logger *zap.Logger) (snc.StorageNodeManagementClient, error) {
	// cheating for test
	snID, err := strconv.Atoi(address)
	if err != nil {
		return nil, err
	}

	return fac.getStorageNodeClient(ctx, types.StorageNodeID(snID))
}

func (r *DummyStorageNodeClient) DisableReport() {
	r.disableReport.Store(true)
}

func (r *DummyStorageNodeClient) EnableReport() {
	r.disableReport.Store(false)
}

func (r *DummyStorageNodeClient) SetReportDelay(d time.Duration) {
	r.reportDelay.Store(d)
}

func (r *DummyStorageNodeClient) SetCommitDelay(d time.Duration) {
	r.commitDelay.Store(d)
}

func (r *DummyStorageNodeClient) GetReport(ctx context.Context) (*snpb.GetReportResponse, error) {
	if r.disableReport.Load() {
		return &snpb.GetReportResponse{
			StorageNodeID: r.storageNodeID,
		}, nil
	}

	time.Sleep(r.reportDelay.Load())

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == DUMMY_STORAGENODE_CLIENT_STATUS_CRASH {
		return nil, errors.New("crash")
	} else if r.status == DUMMY_STORAGENODE_CLIENT_STATUS_CLOSED {
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

func (r *DummyStorageNodeClient) Commit(ctx context.Context, cr *snpb.CommitRequest) error {
	time.Sleep(r.commitDelay.Load())

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == DUMMY_STORAGENODE_CLIENT_STATUS_CRASH {
		return errors.New("crash")
	} else if r.status == DUMMY_STORAGENODE_CLIENT_STATUS_CLOSED {
		return errors.New("closed")
	}

	for _, result := range cr.CommitResults {

		idx := int(result.LogStreamID - types.LogStreamID(r.storageNodeID))
		if idx < 0 || idx >= len(r.logStreamIDs) {
			return errors.New("invalid log stream ID")
		}

		if r.uncommittedLLSNOffset[idx] != result.CommittedLLSNOffset {
			continue
		}

		if r.knownHighWatermark[idx] >= result.HighWatermark {
			continue
		}

		r.knownHighWatermark[idx] = result.HighWatermark
		r.commitResultHistory[idx] = append(r.commitResultHistory[idx], snpb.LogStreamCommitInfo{
			LogStreamID:         result.LogStreamID,
			CommittedLLSNOffset: r.uncommittedLLSNOffset[idx],
			CommittedGLSNOffset: result.CommittedGLSNOffset,
			CommittedGLSNLength: result.CommittedGLSNLength,
			HighWatermark:       result.HighWatermark,
			PrevHighWatermark:   result.PrevHighWatermark,
		})

		r.uncommittedLLSNOffset[idx] += types.LLSN(result.CommittedGLSNLength)
		r.uncommittedLLSNLength[idx] -= result.CommittedGLSNLength
	}

	return nil
}

func (r *DummyStorageNodeClient) Close() error {
	r.descRef()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status != DUMMY_STORAGENODE_CLIENT_STATUS_CRASH &&
		r.ref == 0 {
		r.factory.m.Delete(r.storageNodeID)
		r.status = DUMMY_STORAGENODE_CLIENT_STATUS_CLOSED
	}

	return nil
}

func (a *DummyStorageNodeClientFactory) lookupClient(snID types.StorageNodeID) *DummyStorageNodeClient {
	f, ok := a.m.Load(snID)
	if !ok {
		return nil
	}

	return f.(*DummyStorageNodeClient)
}

func (a *DummyStorageNodeClientFactory) getClientIDs() []types.StorageNodeID {
	var ids []types.StorageNodeID
	a.m.Range(func(key, _ interface{}) bool {
		ids = append(ids, key.(types.StorageNodeID))
		return true
	})

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}

func (r *DummyStorageNodeClient) increaseUncommitted(idx int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if idx < 0 || idx >= len(r.uncommittedLLSNLength) {
		return
	}

	r.uncommittedLLSNLength[idx]++
}

func (r *DummyStorageNodeClient) numUncommitted(idx int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if idx < 0 || idx >= len(r.uncommittedLLSNLength) {
		return 0
	}

	return r.uncommittedLLSNLength[idx]
}

func (r *DummyStorageNodeClient) getKnownHighWatermark(idx int) types.GLSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.knownHighWatermark[idx]
}

func (a *DummyStorageNodeClientFactory) crashRPC(snID types.StorageNodeID) {
	f, ok := a.m.Load(snID)
	if !ok {
		fmt.Printf("notfound\n")
		return
	}

	cli := f.(*DummyStorageNodeClient)

	cli.mu.Lock()
	defer cli.mu.Unlock()

	cli.status = DUMMY_STORAGENODE_CLIENT_STATUS_CRASH
}

func (r *DummyStorageNodeClient) numLogStreams() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.logStreamIDs)
}

func (r *DummyStorageNodeClient) logStreamID(idx int) types.LogStreamID {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.logStreamIDs[idx]
}

func (a *DummyStorageNodeClientFactory) recoverRPC(snID types.StorageNodeID) {
	f, ok := a.m.Load(snID)
	if !ok {
		return
	}

	old := f.(*DummyStorageNodeClient)

	old.mu.Lock()
	defer old.mu.Unlock()

	cli := &DummyStorageNodeClient{
		manual:                old.manual,
		storageNodeID:         old.storageNodeID,
		logStreamIDs:          old.logStreamIDs,
		knownHighWatermark:    old.knownHighWatermark,
		uncommittedLLSNOffset: old.uncommittedLLSNOffset,
		uncommittedLLSNLength: old.uncommittedLLSNLength,
		status:                DUMMY_STORAGENODE_CLIENT_STATUS_RUNNING,
		factory:               old.factory,
	}

	a.m.Store(snID, cli)
}

func (r *DummyStorageNodeClient) PeerAddress() string {
	return r.storageNodeID.String()
}
func (r *DummyStorageNodeClient) PeerStorageNodeID() types.StorageNodeID {
	return r.storageNodeID
}

func (r *DummyStorageNodeClient) GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	status := varlogpb.StorageNodeStatusRunning
	if r.status != DUMMY_STORAGENODE_CLIENT_STATUS_RUNNING {
		status = varlogpb.StorageNodeStatusDeleted
	}

	var logStreams []varlogpb.LogStreamMetadataDescriptor
	for i, lsID := range r.logStreamIDs {
		logStreams = append(logStreams, varlogpb.LogStreamMetadataDescriptor{
			StorageNodeID: r.storageNodeID,
			LogStreamID:   lsID,
			HighWatermark: r.knownHighWatermark[i],
		})
	}

	meta := &varlogpb.StorageNodeMetadataDescriptor{
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNodeID: r.storageNodeID,
			Address:       r.PeerAddress(),
			Status:        status,
		},
		LogStreams: logStreams,
	}

	return meta, nil
}

func (r *DummyStorageNodeClient) AddLogStream(ctx context.Context, logStreamID types.LogStreamID, path string) error {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Unseal(ctx context.Context, logStreamID types.LogStreamID, replicas []snpb.Replica) error {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Sync(ctx context.Context, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) lookupPrevCommitInfo(idx int, hwm types.GLSN) (snpb.LogStreamCommitInfo, bool) {
	i := sort.Search(len(r.commitResultHistory[idx]), func(i int) bool {
		return r.commitResultHistory[idx][i].PrevHighWatermark >= hwm
	})

	if i < len(r.commitResultHistory[idx]) &&
		r.commitResultHistory[idx][i].PrevHighWatermark == hwm {
		return r.commitResultHistory[idx][i], true
	}

	if i > 0 {
		return r.commitResultHistory[idx][i-1], true
	}

	return snpb.LogStreamCommitInfo{}, false
}

func (r *DummyStorageNodeClient) GetPrevCommitInfo(ctx context.Context, hwm types.GLSN) (*snpb.GetPrevCommitInfoResponse, error) {
	ci := &snpb.GetPrevCommitInfoResponse{
		StorageNodeID: r.storageNodeID,
	}

	ci.CommitInfos = make([]*snpb.LogStreamCommitInfo, len(r.logStreamIDs))

	r.mu.Lock()
	defer r.mu.Unlock()

	for i, lsID := range r.logStreamIDs {
		lsci := &snpb.LogStreamCommitInfo{
			LogStreamID:        lsID,
			HighestWrittenLLSN: r.uncommittedLLSNOffset[i] + types.LLSN(r.uncommittedLLSNLength[i]) - types.MinLLSN,
		}

		if r.knownHighWatermark[i] <= hwm {
			lsci.Status = snpb.GetPrevCommitStatusNotFound
		} else if cr, ok := r.lookupPrevCommitInfo(i, hwm); ok {
			lsci.Status = snpb.GetPrevCommitStatusOK
			lsci.CommittedLLSNOffset = cr.CommittedLLSNOffset
			lsci.CommittedGLSNOffset = cr.CommittedGLSNOffset
			lsci.CommittedGLSNLength = cr.CommittedGLSNLength
			lsci.HighWatermark = cr.HighWatermark
			lsci.PrevHighWatermark = cr.PrevHighWatermark
		} else {
			lsci.Status = snpb.GetPrevCommitStatusNotFound
		}

		ci.CommitInfos[i] = lsci
	}

	return ci, nil
}
