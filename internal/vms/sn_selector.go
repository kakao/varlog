package vms

import (
	"context"
	"errors"
	"math/rand"

	"github.com/kakao/varlog/proto/varlogpb"
)

var errNotEnoughStorageNodes = errors.New("storagenodeselector: not enough storage nodes")

// StorStorageNodeSelectionPolicy chooses the storage nodes to add a new log stream.
type StorageNodeSelector interface {
	// TODO (jun): Choose storage nodes and their storages!
	SelectStorageNodeAndPath(ctx context.Context, replicationFactor uint) ([]*varlogpb.ReplicaDescriptor, error)
}

// TODO: randomSNSelector does not consider the capacities and load of each SNs.
type randomSNSelector struct {
	cmView ClusterMetadataView
}

func NewRandomSNSelector(cmView ClusterMetadataView) StorageNodeSelector {
	return &randomSNSelector{cmView: cmView}
}

func (sel *randomSNSelector) SelectStorageNodeAndPath(ctx context.Context, replicationFactor uint) ([]*varlogpb.ReplicaDescriptor, error) {
	clusmeta, err := sel.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	snDescList := clusmeta.GetAllStorageNodes()
	if uint(len(snDescList)) < replicationFactor {
		return nil, errNotEnoughStorageNodes
	}
	indices := rand.Perm(len(snDescList))[:replicationFactor]
	ret := make([]*varlogpb.ReplicaDescriptor, 0, replicationFactor)
	for idx := range indices {
		sndesc := snDescList[idx]
		ret = append(ret, &varlogpb.ReplicaDescriptor{
			StorageNodeID: sndesc.GetStorageNodeID(),
			Path:          sndesc.GetStorages()[0].Path,
		})
	}
	return ret, nil
}
