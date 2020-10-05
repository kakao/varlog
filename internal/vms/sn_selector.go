package vms

import (
	"errors"
	"math/rand"

	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

var errNotEnoughStorageNodes = errors.New("storagenodeselector: not enough storage nodes")

// StorStorageNodeSelectionPolicy chooses the storage nodes to add a new log stream.
type StorageNodeSelector interface {
	// TODO (jun): Choose storage nodes and their storages!
	SelectStorageNode(clusMeta *vpb.MetadataDescriptor, replicationFactor uint) ([]*vpb.StorageNodeDescriptor, error)
}

// TODO: randomSNSelector does not consider the capacities and load of each SNs.
type randomSNSelector struct {
}

func NewRandomSNSelector() StorageNodeSelector {
	return &randomSNSelector{}
}

func (sel *randomSNSelector) SelectStorageNode(clusMeta *vpb.MetadataDescriptor, replicationFactor uint) ([]*vpb.StorageNodeDescriptor, error) {
	snDescList := clusMeta.GetAllStorageNodes()
	if uint(len(snDescList)) < replicationFactor {
		return nil, errNotEnoughStorageNodes
	}
	indices := rand.Perm(len(snDescList))[:replicationFactor]
	ret := make([]*vpb.StorageNodeDescriptor, 0, replicationFactor)
	for idx := range indices {
		ret = append(ret, snDescList[idx])
	}
	return ret, nil
}
