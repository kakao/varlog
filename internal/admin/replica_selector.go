package admin

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/admin -package admin -destination replica_selector_mock.go . ReplicaSelector

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

// ReplicaSelector selects storage nodes and volumes to store data for replicas of a new log stream.
type ReplicaSelector interface {
	// Name returns the name of the replica selector, and it should be unique
	// among all replica selectors.
	Name() string
	// Select returns a slice of `varlogpb.ReplicaDescriptor`; its length
	// should equal the replication factor.
	Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error)
}

const (
	replicaSelectorNameRandom = "random"
	replicaSelectorNameLFU    = "lfu" // least frequently used
)

func newReplicaSelector(selector string, cmview mrmanager.ClusterMetadataView, repfactor int) (ReplicaSelector, error) {
	if repfactor < 1 {
		return nil, errors.Wrap(verrors.ErrInvalid, "replica selector: negative replication factor")
	}

	if cmview == nil {
		return nil, errors.Wrap(verrors.ErrInvalid, "replica selector: invalid cluster metadata view")
	}

	switch strings.ToLower(selector) {
	case replicaSelectorNameRandom:
		return newRandomReplicaSelector(cmview, repfactor)
	case replicaSelectorNameLFU:
		return newLFUSelector(cmview, repfactor)
	default:
		return nil, fmt.Errorf("unknown selector: %s", selector)
	}
}

type randomReplicaSelector struct {
	rng       *rand.Rand
	cmview    mrmanager.ClusterMetadataView
	repfactor int
}

var _ ReplicaSelector = (*randomReplicaSelector)(nil)

func newRandomReplicaSelector(cmview mrmanager.ClusterMetadataView, repfactor int) (*randomReplicaSelector, error) {
	s := &randomReplicaSelector{
		rng:       rand.New(rand.NewSource(time.Now().Unix())),
		cmview:    cmview,
		repfactor: repfactor,
	}
	return s, nil
}

func (s *randomReplicaSelector) Name() string {
	return replicaSelectorNameRandom
}

func (s *randomReplicaSelector) Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error) {
	md, err := s.cmview.ClusterMetadata(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "replica selector")
	}

	if len(md.StorageNodes) < s.repfactor {
		return nil, fmt.Errorf("replica selector: not enough storage nodes: %d < %d", len(md.StorageNodes), s.repfactor)
	}

	ret := make([]*varlogpb.ReplicaDescriptor, s.repfactor)

	snds := md.StorageNodes
	sndIndices := s.rng.Perm(len(snds))[:s.repfactor]
	for idx, sndIdx := range sndIndices {
		snd := snds[sndIdx]
		snid := snd.StorageNodeID
		if snid.Invalid() {
			return nil, errors.New("replica selector: invalid cluster metadata: invalid storage id")
		}

		snpaths := snd.Paths
		if len(snpaths) == 0 {
			return nil, fmt.Errorf("replica selector: invalid cluster metadata: no storage path in storage node %d", snid)
		}
		pathIdx := s.rng.Intn(len(snpaths))
		ret[idx] = &varlogpb.ReplicaDescriptor{
			StorageNodeID:   snid,
			StorageNodePath: snpaths[pathIdx],
		}
	}

	return ret, nil
}

type lfuSelector struct {
	cmview    mrmanager.ClusterMetadataView
	repfactor int
}

type lfuCounter struct {
	snid      types.StorageNodeID
	replicas  uint
	primaries uint
	snpaths   map[string]uint
}

func newLFUSelector(cmview mrmanager.ClusterMetadataView, repfactor int) (*lfuSelector, error) {
	s := &lfuSelector{
		cmview:    cmview,
		repfactor: repfactor,
	}
	return s, nil
}

func (s *lfuSelector) Name() string {
	return replicaSelectorNameLFU
}

func (s *lfuSelector) Select(ctx context.Context) ([]*varlogpb.ReplicaDescriptor, error) {
	md, err := s.cmview.ClusterMetadata(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "replica selector")
	}

	if len(md.StorageNodes) < s.repfactor {
		return nil, fmt.Errorf("replica selector: not enough storage nodes: %d < %d", len(md.StorageNodes), s.repfactor)
	}

	countersMap, err := s.newCounter(md)
	if err != nil {
		return nil, err
	}

	err = s.count(md, countersMap)
	if err != nil {
		return nil, err
	}
	counters := s.sortedCounters(countersMap)
	replicas := s.selectLFU(counters)
	return replicas, nil
}

func (s *lfuSelector) selectLFU(counters []lfuCounter) []*varlogpb.ReplicaDescriptor {
	replicas := make([]*varlogpb.ReplicaDescriptor, s.repfactor)
	for idx, counter := range counters[:s.repfactor] {
		selectedPath := ""
		min := uint(math.MaxUint)
		for snpath, usedCount := range counter.snpaths {
			if usedCount < min {
				min = usedCount
				selectedPath = snpath
			}
		}
		replicas[idx] = &varlogpb.ReplicaDescriptor{
			StorageNodeID:   counter.snid,
			StorageNodePath: selectedPath,
		}
	}
	return replicas
}

func (s *lfuSelector) sortedCounters(countersMap map[types.StorageNodeID]lfuCounter) []lfuCounter {
	counters := maps.Values(countersMap)
	sort.Slice(counters, func(i, j int) bool {
		if counters[i].replicas == counters[j].replicas {
			if counters[i].primaries == counters[j].primaries {
				return counters[i].snid < counters[j].snid
			}
			return counters[i].primaries < counters[j].primaries
		}
		return counters[i].replicas < counters[j].replicas
	})
	return counters
}

func (s *lfuSelector) count(md *varlogpb.MetadataDescriptor, countersMap map[types.StorageNodeID]lfuCounter) error {
	for _, lsd := range md.LogStreams {
		for i, rd := range lsd.Replicas {
			snid := rd.StorageNodeID
			snCounter, ok := countersMap[rd.StorageNodeID]
			if !ok {
				return fmt.Errorf("replica selector: inconsistent cluster metadata: no matched storage node %d", snid)
			}
			snCounter.replicas++
			if i == 0 {
				snCounter.primaries++
			}

			snpath := rd.StorageNodePath
			snpathUsedCount, ok := snCounter.snpaths[snpath]
			if !ok {
				return fmt.Errorf("replica selector: inconsistent cluster metadata: no matched storage path %s", snpath)
			}
			snCounter.snpaths[snpath] = snpathUsedCount + 1
			countersMap[snid] = snCounter
		}
	}
	return nil
}

func (s *lfuSelector) newCounter(md *varlogpb.MetadataDescriptor) (map[types.StorageNodeID]lfuCounter, error) {
	countersMap := make(map[types.StorageNodeID]lfuCounter, len(md.StorageNodes))
	for _, snd := range md.StorageNodes {
		if snd.StorageNodeID.Invalid() {
			return nil, errors.New("replica selector: invalid cluster metadata: invalid storage id")
		}
		if len(snd.Paths) == 0 {
			return nil, fmt.Errorf("replica selector: invalid cluster metadata: no storage path in storage node %d", snd.StorageNodeID)
		}
		cnt := lfuCounter{
			snid:    snd.StorageNodeID,
			snpaths: make(map[string]uint, len(snd.Paths)),
		}
		for _, snpath := range snd.Paths {
			cnt.snpaths[snpath] = 0
		}
		countersMap[snd.StorageNodeID] = cnt
	}
	return countersMap, nil
}
