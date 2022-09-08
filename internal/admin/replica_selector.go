package admin

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/admin -package admin -destination replica_selector_mock.go . ReplicaSelector

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

// ReplicaSelector selects storage nodes and volumes to store data for replicas of a new log stream.
// This method returns a slice of `varlogpb.ReplicaDescriptor` and its length should be equal to the
// replication factor.
type ReplicaSelector interface {
	Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error)
}

// balancedReplicaSelector selects storage nodes and volumes for a new log stream to be balanced in
// terms of the number of replicas as well as the number of primary replica per storage node.
// Note that it does not consider loads of storage nodes.
type balancedReplicaSelector struct {
	rng               *rand.Rand
	cmView            mrmanager.ClusterMetadataView
	replicationFactor int
}

var _ ReplicaSelector = (*balancedReplicaSelector)(nil)

func newBalancedReplicaSelector(cmView mrmanager.ClusterMetadataView, replicationFactor int) (*balancedReplicaSelector, error) {
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
