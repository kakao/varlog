package varlogadm

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

// ReplicaSelector selects storage nodes and volumes to store data for replicas of a new log stream.
// This method returns a slice of `varlogpb.ReplicaDescriptor` and its length should be equal to the
// replication factor.
type ReplicaSelector interface {
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
	allowlist := make([]struct {
		snd      *varlogpb.StorageNodeDescriptor
		priority int
	}, 0, len(sndescList))
	for _, sndesc := range sndescList {
		storageNodeID := sndesc.GetStorageNodeID()
		if !rs.denylist.Contains(storageNodeID) {
			// TODO (jun): This is very inefficient functions. Make map<storage_node,
			// log_streams> first.
			rds := clusmeta.GetLogStreamsByStorageNodeID(storageNodeID)
			allowlist = append(allowlist, struct {
				snd      *varlogpb.StorageNodeDescriptor
				priority int
			}{
				snd:      sndesc,
				priority: len(rds),
			})
		}
	}

	if uint(len(allowlist)) < rs.count {
		return nil, errors.Errorf("replicaselector: not enough replicas (%d, %d)", len(allowlist), rs.count)
	}

	sort.Slice(allowlist, func(i, j int) bool {
		return allowlist[i].priority < allowlist[j].priority
	})

	ret := make([]*varlogpb.ReplicaDescriptor, 0, rs.count)
	for idx := range allowlist[0:rs.count] {
		// TODO (jun): choose proper path
		ret = append(ret, &varlogpb.ReplicaDescriptor{
			StorageNodeID: allowlist[idx].snd.GetStorageNodeID(),
			Path:          allowlist[idx].snd.Paths[0],
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

// balancedReplicaSelector selects storage nodes and volumes for a new log stream to be balanced in
// terms of the number of replicas as well as the number of primary replica per storage node.
// Note that it does not consider loads of storage nodes.
type balancedReplicaSelector struct {
	rng               *rand.Rand
	cmView            ClusterMetadataView
	replicationFactor int
}

var _ ReplicaSelector = (*balancedReplicaSelector)(nil)

func newBalancedReplicaSelector(cmView ClusterMetadataView, replicationFactor int) (*balancedReplicaSelector, error) {
	if replicationFactor < 1 {
		return nil, errors.Wrap(verrors.ErrInvalid, "replica selector: negative replication factor")
	}
	if cmView == nil {
		return nil, errors.Wrap(verrors.ErrInvalid, "replica selector: invalid cluster metadata view")
	}
	sel := &balancedReplicaSelector{
		rng:               rand.New(rand.NewSource(time.Now().Unix())),
		cmView:            cmView,
		replicationFactor: replicationFactor,
	}
	return sel, nil
}

func (sel *balancedReplicaSelector) Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error) {
	md, err := sel.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "replica selector")
	}

	snds := md.GetStorageNodes()
	stats := make(map[types.StorageNodeID]storageNodeStat, len(snds))

	for _, snd := range snds {
		storageNodeID := snd.StorageNodeID
		st := storageNodeStat{
			storageNodeID: storageNodeID,
			paths:         make(map[string]struct{}, len(snd.Paths)),
			assignedPaths: make(map[string]struct{}, len(snd.Paths)),
		}
		for _, path := range snd.Paths {
			st.paths[path] = struct{}{}
		}
		stats[storageNodeID] = st
	}

	lsds := md.GetLogStreams()
	for _, lsd := range lsds {
		for i, rd := range lsd.Replicas {
			storageNodeID := rd.StorageNodeID
			st, ok := stats[storageNodeID]
			if !ok {
				panic("replica selector: inconsistent cluster metadata")
			}
			st.replicas++
			if i == 0 {
				st.primaryReplicas++
			}
			st.assignedPaths[rd.Path] = struct{}{}
			stats[storageNodeID] = st
		}
	}

	statsList := make([]storageNodeStat, 0, len(stats))
	for _, st := range stats {
		statsList = append(statsList, st)
	}

	sort.Slice(statsList, func(i, j int) bool {
		st1, st2 := statsList[i], statsList[j]
		ut1, ut2 := st1.utilization(), st2.utilization()

		if ut1 != ut2 {
			return ut1 < ut2
		}

		if st1.primaryReplicas != st2.primaryReplicas {
			return st1.primaryReplicas < st2.primaryReplicas
		}
		return st1.replicas < st2.replicas
	})

	statsList = statsList[:sel.replicationFactor]
	sort.Slice(statsList, func(i, j int) bool {
		st1, st2 := statsList[i], statsList[j]
		return st1.primaryReplicas < st2.primaryReplicas
	})

	rds := make([]*varlogpb.ReplicaDescriptor, 0, sel.replicationFactor)
	for _, st := range statsList {
		snd := md.GetStorageNode(st.storageNodeID)
		var path string
		if len(st.paths) == len(st.assignedPaths) {
			path = snd.Paths[sel.rng.Intn(len(snd.Paths))]
		} else {
			for p := range st.paths {
				if _, ok := st.assignedPaths[path]; ok {
					continue
				}
				path = p
				break
			}
		}
		rds = append(rds, &varlogpb.ReplicaDescriptor{
			StorageNodeID: st.storageNodeID,
			Path:          path,
		})
	}

	return rds, nil
}

type storageNodeStat struct {
	storageNodeID   types.StorageNodeID
	replicas        int
	primaryReplicas int
	paths           map[string]struct{}
	assignedPaths   map[string]struct{}
}

func (s storageNodeStat) utilization() float64 {
	return float64(s.replicas) / float64(len(s.paths))
}
