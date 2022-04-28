package metarepos

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/reportcommitter"
	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type EmptyStorageNodeClient struct {
}

func (rc *EmptyStorageNodeClient) GetReport() (*snpb.GetReportResponse, error) {
	return &snpb.GetReportResponse{}, nil
}

func (rc *EmptyStorageNodeClient) Commit(snpb.CommitRequest) error {
	return nil
}

func (rc *EmptyStorageNodeClient) Close() error {
	return nil
}

func (rc *EmptyStorageNodeClient) PeerAddress() string {
	panic("not implemented")
}
func (rc *EmptyStorageNodeClient) PeerStorageNodeID() types.StorageNodeID {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) GetMetadata(context.Context) (*snpb.StorageNodeMetadataDescriptor, error) {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) AddLogStreamReplica(context.Context, types.TopicID, types.LogStreamID, string) error {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) RemoveLogStream(context.Context, types.TopicID, types.LogStreamID) error {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) Seal(context.Context, types.TopicID, types.LogStreamID, types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) Unseal(context.Context, types.TopicID, types.LogStreamID, []varlogpb.LogStreamReplica) error {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) Sync(context.Context, types.TopicID, types.LogStreamID, types.StorageNodeID, string, types.GLSN) (*snpb.SyncStatus, error) {
	panic("not implemented")
}

func (rc *EmptyStorageNodeClient) Trim(context.Context, types.TopicID, types.GLSN) (map[types.LogStreamID]error, error) {
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

const DefaultDelay = 500 * time.Microsecond

const (
	DummyStorageNodeClientStatusRunning DummyStorageNodeClientStatus = iota
	DummyStorageNodeClientStatusClosed
	DummyStorageNodeClientStatusCrash
)

type DummyStorageNodeClient struct {
	storageNodeID types.StorageNodeID

	logStreamIDs          []types.LogStreamID
	knownVersion          []types.Version
	uncommittedLLSNOffset []types.LLSN
	uncommittedLLSNLength []uint64

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

func (fac *DummyStorageNodeClientFactory) getStorageNodeClient(_ context.Context, snID types.StorageNodeID) (*DummyStorageNodeClient, error) {
	status := DummyStorageNodeClientStatusRunning

	LSIDs := make([]types.LogStreamID, fac.nrLogStreams)
	for i := 0; i < fac.nrLogStreams; i++ {
		LSIDs[i] = types.LogStreamID(snID) + types.LogStreamID(i)
	}

	knownVersion := make([]types.Version, fac.nrLogStreams)

	uncommittedLLSNOffset := make([]types.LLSN, fac.nrLogStreams)
	for i := 0; i < fac.nrLogStreams; i++ {
		uncommittedLLSNOffset[i] = types.MinLLSN
	}

	uncommittedLLSNLength := make([]uint64, fac.nrLogStreams)

	cli := &DummyStorageNodeClient{
		manual:                fac.manual,
		storageNodeID:         snID,
		logStreamIDs:          LSIDs,
		knownVersion:          knownVersion,
		uncommittedLLSNOffset: uncommittedLLSNOffset,
		uncommittedLLSNLength: uncommittedLLSNLength,
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

func (fac *DummyStorageNodeClientFactory) GetManagementClient(ctx context.Context, _ types.ClusterID, address string, _ *zap.Logger) (snc.StorageNodeManagementClient, error) {
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

func (r *DummyStorageNodeClient) GetReport() (*snpb.GetReportResponse, error) {
	if r.disableReport.Load() {
		return &snpb.GetReportResponse{
			StorageNodeID: r.storageNodeID,
		}, nil
	}

	time.Sleep(r.reportDelay.Load())

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == DummyStorageNodeClientStatusCrash {
		return nil, errors.New("crash")
	} else if r.status == DummyStorageNodeClientStatusClosed {
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
		u := snpb.LogStreamUncommitReport{
			LogStreamID:           lsID,
			Version:               r.knownVersion[i],
			UncommittedLLSNOffset: r.uncommittedLLSNOffset[i],
			UncommittedLLSNLength: r.uncommittedLLSNLength[i],
		}
		lls.UncommitReports = append(lls.UncommitReports, u)
	}

	return lls, nil
}

func (r *DummyStorageNodeClient) Commit(cr snpb.CommitRequest) error {
	time.Sleep(r.commitDelay.Load())

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status == DummyStorageNodeClientStatusCrash {
		return errors.New("crash")
	} else if r.status == DummyStorageNodeClientStatusClosed {
		return errors.New("closed")
	}

	idx := int(cr.CommitResult.LogStreamID - types.LogStreamID(r.storageNodeID))
	if idx < 0 || idx >= len(r.logStreamIDs) {
		return errors.New("invalid log stream ID")
	}

	if r.uncommittedLLSNOffset[idx] != cr.CommitResult.CommittedLLSNOffset {
		// continue
		return nil
	}

	if r.knownVersion[idx] >= cr.CommitResult.Version {
		//continue
		return nil
	}

	r.knownVersion[idx] = cr.CommitResult.Version

	r.uncommittedLLSNOffset[idx] += types.LLSN(cr.CommitResult.CommittedGLSNLength)
	r.uncommittedLLSNLength[idx] -= cr.CommitResult.CommittedGLSNLength

	return nil
}

func (r *DummyStorageNodeClient) Close() error {
	r.descRef()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.status != DummyStorageNodeClientStatusCrash &&
		r.ref == 0 {
		r.factory.m.Delete(r.storageNodeID)
		r.status = DummyStorageNodeClientStatusClosed
	}

	return nil
}

func (fac *DummyStorageNodeClientFactory) lookupClient(snID types.StorageNodeID) *DummyStorageNodeClient {
	f, ok := fac.m.Load(snID)
	if !ok {
		return nil
	}

	return f.(*DummyStorageNodeClient)
}

func (fac *DummyStorageNodeClientFactory) getClientIDs() []types.StorageNodeID {
	var ids []types.StorageNodeID
	fac.m.Range(func(key, _ interface{}) bool {
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

func (r *DummyStorageNodeClient) getKnownVersion(idx int) types.Version {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.knownVersion[idx]
}

func (fac *DummyStorageNodeClientFactory) crashRPC(snID types.StorageNodeID) {
	f, ok := fac.m.Load(snID)
	if !ok {
		fmt.Printf("notfound\n")
		return
	}

	cli := f.(*DummyStorageNodeClient)

	cli.mu.Lock()
	defer cli.mu.Unlock()

	cli.status = DummyStorageNodeClientStatusCrash
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

func (fac *DummyStorageNodeClientFactory) recoverRPC(snID types.StorageNodeID) {
	f, ok := fac.m.Load(snID)
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
		knownVersion:          old.knownVersion,
		uncommittedLLSNOffset: old.uncommittedLLSNOffset,
		uncommittedLLSNLength: old.uncommittedLLSNLength,
		status:                DummyStorageNodeClientStatusRunning,
		factory:               old.factory,
	}

	fac.m.Store(snID, cli)
}

func (r *DummyStorageNodeClient) PeerAddress() string {
	return r.storageNodeID.String()
}
func (r *DummyStorageNodeClient) PeerStorageNodeID() types.StorageNodeID {
	return r.storageNodeID
}

func (r *DummyStorageNodeClient) GetMetadata(context.Context) (*snpb.StorageNodeMetadataDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	status := varlogpb.StorageNodeStatusRunning
	if r.status != DummyStorageNodeClientStatusRunning {
		status = varlogpb.StorageNodeStatusDeleted
	}

	var logStreams []snpb.LogStreamReplicaMetadataDescriptor
	for i, lsID := range r.logStreamIDs {
		logStreams = append(logStreams, snpb.LogStreamReplicaMetadataDescriptor{
			LogStreamReplica: varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: r.storageNodeID,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					LogStreamID: lsID,
				},
			},
			Version: r.knownVersion[i],
		})
	}

	meta := &snpb.StorageNodeMetadataDescriptor{
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: r.storageNodeID,
				Address:       r.PeerAddress(),
			},
			Status: status,
		},
		LogStreamReplicas: logStreams,
	}

	return meta, nil
}

func (r *DummyStorageNodeClient) AddLogStreamReplica(context.Context, types.TopicID, types.LogStreamID, string) error {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) RemoveLogStream(context.Context, types.TopicID, types.LogStreamID) error {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Seal(context.Context, types.TopicID, types.LogStreamID, types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Unseal(context.Context, types.TopicID, types.LogStreamID, []varlogpb.LogStreamReplica) error {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Sync(context.Context, types.TopicID, types.LogStreamID, types.StorageNodeID, string, types.GLSN) (*snpb.SyncStatus, error) {
	panic("not implemented")
}

func (r *DummyStorageNodeClient) Trim(context.Context, types.TopicID, types.GLSN) (map[types.LogStreamID]error, error) {
	panic("not implemented")
}
