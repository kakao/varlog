package vms

import (
	"context"
	"math/rand"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// StorStorageNodeSelectionPolicy chooses the storage nodes to add a new log stream.
type ReplicaSelector interface {
	// TODO (jun): Choose storage nodes and their storages!
	Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error)
}

// TODO: randomReplicaSelector does not consider the capacities and load of each SNs.
type randomReplicaSelector struct {
	r        *rand.Rand
	cmView   ClusterMetadataView
	count    uint
	denylist set.Set // set[types.StorageNodeID]
}

func newRandomReplicaSelector(cmView ClusterMetadataView, count uint, denylist ...types.StorageNodeID) (ReplicaSelector, error) {
	if count == 0 {
		return nil, errors.New("replicaselector: zero replication factor")
	}

	rs := &randomReplicaSelector{
		r:        rand.New(rand.NewSource(time.Now().UnixNano())),
		cmView:   cmView,
		count:    count,
		denylist: set.New(len(denylist)),
	}

	for _, snid := range denylist {
		rs.denylist.Add(snid)
	}
	return rs, nil
}

func (rs *randomReplicaSelector) Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error) {
	clusmeta, err := rs.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	sndescList := clusmeta.GetStorageNodes()
	allowlist := make([]*varlogpb.StorageNodeDescriptor, 0, len(sndescList))
	for _, sndesc := range sndescList {
		if !rs.denylist.Contains(sndesc.GetStorageNodeID()) {
			allowlist = append(allowlist, sndesc)
		}
	}

	if uint(len(allowlist)) < rs.count {
		return nil, errors.New("replicaselector: not enough replicas")
	}
	indices := rs.r.Perm(len(allowlist))[:rs.count]
	ret := make([]*varlogpb.ReplicaDescriptor, 0, rs.count)
	for _, idx := range indices {
		sndesc := allowlist[idx]
		// TODO (jun): choose proper path
		ret = append(ret, &varlogpb.ReplicaDescriptor{
			StorageNodeID: sndesc.GetStorageNodeID(),
			Path:          sndesc.GetStorages()[0].Path,
		})
	}
	return ret, nil
}

type victimSelector struct {
	snMgr       StorageNodeManager
	replicas    []*varlogpb.ReplicaDescriptor
	logStreamID types.LogStreamID
}

func newVictimSelector(snMgr StorageNodeManager, logStreamID types.LogStreamID, replicas []*varlogpb.ReplicaDescriptor) ReplicaSelector {
	clone := make([]*varlogpb.ReplicaDescriptor, len(replicas))
	for i, replica := range replicas {
		clone[i] = proto.Clone(replica).(*varlogpb.ReplicaDescriptor)
	}
	return &victimSelector{
		snMgr:       snMgr,
		replicas:    clone,
		logStreamID: logStreamID,
	}
}

// Select chooses victim replica that is not LogStreamStatusSealed and can be pulled out from the
// log stream.
func (vs *victimSelector) Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error) {
	victims := make([]*varlogpb.ReplicaDescriptor, 0, len(vs.replicas))
	for _, replica := range vs.replicas {
		if snmeta, err := vs.snMgr.GetMetadata(ctx, replica.GetStorageNodeID()); err == nil {
			if lsmeta, ok := snmeta.FindLogStream(vs.logStreamID); ok && lsmeta.GetStatus() == varlogpb.LogStreamStatusSealed {
				continue
			}
		}
		victims = append(victims, replica)
	}
	if len(vs.replicas) <= len(victims) {
		return nil, errors.New("victimselector: no good replica")
	}
	if len(victims) == 0 {
		return nil, errors.New("victimselector: no victim")
	}
	// TODO (jun): need more sophiscate priority rule?
	// TODO (jun): or update repeatedly until all victims are disappeared?
	return victims, nil
}
