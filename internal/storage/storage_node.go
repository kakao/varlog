package storage

import (
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"
)

// Management is the interface that wraps methods for managing StorageNode.
type Management interface {
	// GetMetadata returns metadata of StorageNode. The metadata contains
	// configurations and statistics for StorageNode.
	GetMetadata(clusterID types.ClusterID, metadataType pb.MetadataType) (*vpb.StorageNodeMetadataDescriptor, error)

	// AddLogStream adds a new LogStream to StorageNode.
	AddLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, path string) (string, error)

	// RemoveLogStream removes a LogStream from StorageNode.
	RemoveLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error

	// Seal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusSealing or LogStreamStatusSealed.
	Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error)

	// Unseal changes status of LogStreamExecutor corresponding to the
	// LogStreamID to LogStreamStatusRunning.
	Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error
}

type LogStreamExecutorGetter interface {
	GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool)
	GetLogStreamExecutors() []LogStreamExecutor
}

type StorageNode struct {
	ClusterID     types.ClusterID
	StorageNodeID types.StorageNodeID

	lseMtx sync.RWMutex
	lseMap map[types.LogStreamID]LogStreamExecutor
}

func NewStorageNode() (*StorageNode, error) {
	panic("not yet implemented")
}

func (sn *StorageNode) Run() error {
	panic("not yet implemented")
}

func (sn *StorageNode) Close() error {
	panic("not yet implemented")
}

// GetMeGetMetadata implements the Management GetMetadata method.
func (sn *StorageNode) GetMetadata(cid types.ClusterID, metadataType pb.MetadataType) (*vpb.StorageNodeMetadataDescriptor, error) {
	if !sn.verifyClusterID(cid) {
		return nil, varlog.ErrInvalidArgument
	}
	ret := &vpb.StorageNodeMetadataDescriptor{
		ClusterID: sn.ClusterID,
		StorageNode: &vpb.StorageNodeDescriptor{
			StorageNodeID: sn.StorageNodeID,
		},
	}
	return ret, nil
}

// AddLogStream implements the Management AddLogStream method.
func (sn *StorageNode) AddLogStream(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID, path string) (string, error) {
	if !sn.verifyClusterID(cid) || !sn.verifyStorageNodeID(snid) {
		return "", varlog.ErrInvalidArgument
	}
	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()
	_, ok := sn.lseMap[lsid]
	if ok {
		return "", varlog.ErrExist // FIXME: ErrExist or ErrAlreadyExists
	}
	// TODO(jun): Create Storage and add new LSE
	var stg Storage
	var stgPath string
	var options LogStreamExecutorOptions
	lse, err := NewLogStreamExecutor(lsid, stg, &options)
	if err != nil {
		return "", err
	}
	sn.lseMap[lsid] = lse
	return stgPath, nil
}

// RemoveLogStream implements the Management RemoveLogStream method.
func (sn *StorageNode) RemoveLogStream(cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) error {
	if !sn.verifyClusterID(cid) || !sn.verifyStorageNodeID(snid) {
		return varlog.ErrInvalidArgument
	}
	sn.lseMtx.Lock()
	defer sn.lseMtx.Unlock()
	_, ok := sn.lseMap[lsid]
	if !ok {
		return varlog.ErrNotExist
	}
	delete(sn.lseMap, lsid)
	return nil
}

// Seal implements the Management Seal method.
func (sn *StorageNode) Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error) {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return vpb.LogStreamStatusRunning, types.InvalidGLSN, varlog.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return vpb.LogStreamStatusRunning, types.InvalidGLSN, varlog.ErrInvalidArgument
	}
	status, hwm := lse.Seal(lastCommittedGLSN)
	return status, hwm, nil
}

// Unseal implements the Management Unseal method.
func (sn *StorageNode) Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error {
	if !sn.verifyClusterID(clusterID) || !sn.verifyStorageNodeID(storageNodeID) {
		return varlog.ErrInvalidArgument
	}
	lse, ok := sn.GetLogStreamExecutor(logStreamID)
	if !ok {
		return varlog.ErrInvalidArgument
	}
	return lse.Unseal()
}

func (sn *StorageNode) GetLogStreamExecutor(logStreamID types.LogStreamID) (LogStreamExecutor, bool) {
	sn.lseMtx.RLock()
	defer sn.lseMtx.RUnlock()
	lse, ok := sn.lseMap[logStreamID]
	return lse, ok
}

func (sn *StorageNode) GetLogStreamExecutors() []LogStreamExecutor {
	var ret []LogStreamExecutor
	sn.lseMtx.RLock()
	for _, lse := range sn.lseMap {
		ret = append(ret, lse)
	}
	sn.lseMtx.RUnlock()
	return ret
}

func (sn *StorageNode) verifyClusterID(cid types.ClusterID) bool {
	return sn.ClusterID == cid
}

func (sn *StorageNode) verifyStorageNodeID(snid types.StorageNodeID) bool {
	return sn.StorageNodeID == snid
}
