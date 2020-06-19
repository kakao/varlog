package metadata_repository

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/common/log"
	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

type RaftMetadataRepository struct {
	index      int
	isLeader   bool
	storageMap map[types.StorageNodeID]varlog.StorageNodeClient

	// SMR
	smr pb.MetadataRepositoryDescriptor
	mu  sync.RWMutex

	// for ack to session
	sessionNum uint64
	sessionMap sync.Map

	// for raft
	proposeC           chan *pb.RaftEntry
	commitC            chan *pb.RaftEntry
	rnConfChangeC      chan raftpb.ConfChange
	rnProposeC         chan string
	rnCommitC          <-chan *string
	rnErrorC           <-chan error
	rnSnapshotterReady <-chan *snap.Snapshotter

	wg sync.WaitGroup
}

func NewRaftMetadataRepository(index int, peerList []string) *RaftMetadataRepository {
	mr := &RaftMetadataRepository{
		index:    index,
		isLeader: index == 0,
	}

	mr.smr.Metadata = &varlogpb.MetadataDescriptor{}

	mr.proposeC = make(chan *pb.RaftEntry, 4096)
	mr.commitC = make(chan *pb.RaftEntry, 4096)

	mr.rnConfChangeC = make(chan raftpb.ConfChange)
	mr.rnProposeC = make(chan string)
	commitC, errorC, snapshotterReady := newRaftNode(
		int(index)+1, // raftNode is 1-indexed
		peerList,
		false, // not to join an existing cluster
		mr.getSnapshot,
		mr.rnProposeC,
		mr.rnConfChangeC,
	)
	mr.rnCommitC = commitC
	mr.rnErrorC = errorC
	mr.rnSnapshotterReady = snapshotterReady
	return mr
}

func (mr *RaftMetadataRepository) Start() {
	go mr.runReplication()
	go mr.processCommit()
	go mr.processRNCommit()
}

func (mr *RaftMetadataRepository) Close() error {
	close(mr.proposeC)
	close(mr.rnProposeC)
	err := <-mr.rnErrorC

	mr.wg.Wait()
	return err
}

func (mr *RaftMetadataRepository) runReplication() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for e := range mr.proposeC {
		b, err := e.Marshal()
		if err != nil {
			log.Errorf("%v", err)
			continue
		}

		mr.rnProposeC <- string(b)
	}
}

func (mr *RaftMetadataRepository) processCommit() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for e := range mr.commitC {
		if mr.isLeader {
			log.Debugf("%v", e)
		}

		mr.apply(e)
	}
}

func (mr *RaftMetadataRepository) processRNCommit() {
	mr.wg.Add(1)
	defer mr.wg.Done()

	for d := range mr.rnCommitC {
		if d == nil {
			// TODO: handle snapshots
			continue
		}

		e := &pb.RaftEntry{}
		err := e.Unmarshal([]byte(*d))
		if err != nil {
			log.Errorf("%v", err)
			continue
		}

		mr.commitC <- e
	}

	close(mr.commitC)
}

func (mr *RaftMetadataRepository) getSnapshot() ([]byte, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	b, _ := mr.smr.Marshal()
	return b, nil
}

func (mr *RaftMetadataRepository) sendAck(sessionNum uint64, err error) {
	f, ok := mr.sessionMap.Load(sessionNum)
	if !ok {
		return
	}

	c := f.(chan error)
	c <- err
}

func (mr *RaftMetadataRepository) apply(e *pb.RaftEntry) {
	var err error
	f := e.Request.GetValue()
	switch r := f.(type) {
	case *pb.RegisterStorageNodeRequest:
		err = mr.applyRegisterStorageNode(r)
	case *pb.CreateLogStreamRequest:
		err = mr.applyCreateLogStream(r)
	}

	mr.sendAck(e.SessionNum, err)
}

func (mr *RaftMetadataRepository) applyRegisterStorageNode(r *pb.RegisterStorageNodeRequest) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if err := mr.smr.Metadata.InsertStorageNode(r.StorageNode); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (mr *RaftMetadataRepository) applyCreateLogStream(r *pb.CreateLogStreamRequest) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if err := mr.smr.Metadata.InsertLogStream(r.LogStream); err != nil {
		return varlog.ErrAlreadyExists
	}

	return nil
}

func (mr *RaftMetadataRepository) propose(r interface{}) error {
	e := &pb.RaftEntry{}
	e.Request.SetValue(r)
	e.SessionNum = atomic.AddUint64(&mr.sessionNum, 1)

	c := make(chan error, 1)

	mr.sessionMap.Store(e.SessionNum, c)
	mr.proposeC <- e

	return <-c
}

func (mr *RaftMetadataRepository) RegisterStorageNode(sn *varlogpb.StorageNodeDescriptor) error {
	r := &pb.RegisterStorageNodeRequest{
		StorageNode: sn,
	}

	return mr.propose(r)
}

func (mr *RaftMetadataRepository) CreateLogStream(ls *varlogpb.LogStreamDescriptor) error {
	r := &pb.CreateLogStreamRequest{
		LogStream: ls,
	}

	return mr.propose(r)
}

func (mr *RaftMetadataRepository) GetMetadata() (*varlogpb.MetadataDescriptor, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	// TODO:: make it thread-safe
	return mr.smr.Metadata, nil
}
