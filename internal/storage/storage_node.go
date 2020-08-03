package storage

import (
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type Management interface {
	AddLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, path string) (string, error)
	RemoveLogStream(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error
	Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error)
	Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error
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
	lse, err := NewLogStreamExecutor(lsid, stg)
	if err != nil {
		return "", err
	}
	sn.lseMap[lsid] = lse
	return stgPath, nil
}

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

func (sn *StorageNode) Seal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error) {
	panic("")
}

func (sn *StorageNode) Unseal(clusterID types.ClusterID, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error {
	panic("")
}

func (sn *StorageNode) verifyClusterID(cid types.ClusterID) bool {
	return sn.ClusterID == cid
}

func (sn *StorageNode) verifyStorageNodeID(snid types.StorageNodeID) bool {
	return sn.StorageNodeID == snid
}
