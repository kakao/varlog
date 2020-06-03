package varlog

import (
	"context"
	"errors"
	"sort"

	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type Replica struct {
	LogStream
	StorageNodeIDs []string
}

type ReplicaSorter []Replica

func (s ReplicaSorter) Len() int           { return len(s) }
func (s ReplicaSorter) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ReplicaSorter) Less(i, j int) bool { return s[i].MinLsn < s[j].MinLsn }

type OpenMode int

type Options struct {
	MetadataRepositoryAddress string
}

// Solar is a log interface with thread-safety. Many goroutines can share the same varlog object.
type Solar interface {
	Read(glsn uint64) ([]byte, error)
	Append(data []byte) (uint64, error)
}

type varlog struct {
	logID           string
	epoch           uint64
	metaReposClient MetadataRepositoryClient
	sqrClient       SequencerClient
	snClientMap     map[string]StorageNodeClient
	replicas        []Replica
}

// Open creates new logs or opens an already created logs.
func Open(logID string, opts Options) (Solar, error) {
	metaReposClient, err := NewMetadataRepositoryClient(opts.MetadataRepositoryAddress)
	if err != nil {
		return nil, err
	}
	varlog := &varlog{
		logID:           logID,
		epoch:           0,
		metaReposClient: metaReposClient,
	}

	newPrj, err := varlog.fetchProjection()
	if err != nil {
		return nil, err
	}

	err = varlog.applyProjection(newPrj)
	if err != nil {
		return nil, err
	}

	varlog.replicas = varlog.createReplicas(newPrj.GetReplicas())

	return varlog, nil
}

func (s *varlog) Read(glsn uint64) ([]byte, error) {
	replica, err := s.getReplica(glsn)
	if err != nil {
		return nil, err
	}
	snID := replica.StorageNodeIDs[len(replica.StorageNodeIDs)-1]
	snClient := s.snClientMap[snID]
	return snClient.Read(context.Background(), s.epoch, glsn)
}

func (s *varlog) Append(data []byte) (uint64, error) {
	glsn, err := s.sqrClient.Next(context.Background())
	if err != nil {
		return 0, err
	}
	replica, err := s.getReplica(glsn)
	if err != nil {
		return 0, err
	}
	for _, snID := range replica.StorageNodeIDs {
		snClient := s.snClientMap[snID]
		if err := snClient.Append(context.Background(), s.epoch, glsn, data); err != nil {
			return 0, err
		}

	}
	return glsn, nil
}

func (s *varlog) Fill(glsn uint64) error {
	panic("not implemented")
}

func (s *varlog) Trim(glsn uint64) error {
	panic("not implemented")
}

func (s *varlog) fetchProjection() (*varlogpb.ProjectionDescriptor, error) {
	ctx := context.Background()
	prj, err := s.metaReposClient.Get(ctx)
	if err != nil {
		return nil, err
	}
	return prj, nil
}

// FIXME: it should be more precise method
func (s *varlog) applyProjection(newPrj *varlogpb.ProjectionDescriptor) error {
	if newPrj == nil {
		return ErrInvalidProjection
	}
	newEpoch := newPrj.GetEpoch()
	if s.epoch > newEpoch {
		return ErrInvalidEpoch
	}

	newSqr := newPrj.GetSequencer()
	newSqrAddr := newSqr.GetAddress()
	if len(newSqrAddr) == 0 {
		return ErrInvalidProjection
	}
	var err error
	newSqrClient := s.sqrClient
	// FIXME: it is assumed that identifier of sequencer equals to its address
	if s.sqrClient == nil || s.sqrClient.Id() != newSqrAddr {
		if s.sqrClient != nil {
			if err := s.sqrClient.Close(); err != nil {
				// TODO: forget it
			}
		}
		newSqrClient, err = NewSequencerClient(newSqrAddr)
		if err != nil {
			return err
		}
	}

	newSnClientMap := make(map[string]StorageNodeClient)
	for _, newSn := range newPrj.GetStorageNodes() {
		newSnID := newSn.GetStorageNodeId()
		newSnAddr := newSn.GetAddress()
		if len(newSnID) == 0 || len(newSnAddr) == 0 {
			return ErrInvalidProjection
		}
		oldSn, ok := s.snClientMap[newSnID]
		var newSnClient StorageNodeClient
		if ok {
			newSnClient = oldSn
		} else {
			newSnClient, err = NewStorageNodeClient(newSn.GetAddress())
			if err != nil {
				return err
			}
		}
		newSnClientMap[newSnID] = newSnClient
	}
	for snID, snClient := range s.snClientMap {
		_, ok := newSnClientMap[snID]
		if ok {
			continue
		}
		if err := snClient.Close(); err != nil {
			// TODO: forget it
		}
	}

	s.epoch = newEpoch
	s.sqrClient = newSqrClient
	s.snClientMap = newSnClientMap
	return nil
}

func (s *varlog) createReplicas(replicas []varlogpb.ReplicaDescriptor) []Replica {
	r := make([]Replica, len(replicas))
	for i, replica := range replicas {
		r[i].MinLsn = replica.GetMinLsn()
		r[i].MaxLsn = replica.GetMaxLsn()
		r[i].StorageNodeIDs = replica.GetStorageNodeIds()
	}
	// FIXME: replicas in projection descriptor should have been sorted
	sort.Sort(ReplicaSorter(r))
	return r
}

func (s *varlog) getReplica(glsn uint64) (Replica, error) {
	lenReplicas := len(s.replicas)
	idx := sort.Search(lenReplicas, func(i int) bool {
		return s.replicas[i].MinLsn <= glsn
	})
	if idx < lenReplicas && s.replicas[idx].MaxLsn > glsn {
		return s.replicas[idx], nil
	}
	return Replica{}, errors.New("no replica")
}
