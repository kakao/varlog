package storage

import (
	"sync"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"
)

type Management interface {
	GetMetadata(clusterID types.ClusterID, metadataType pb.MetadataType) (*vpb.StorageNodeMetadataDescriptor, error)
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
