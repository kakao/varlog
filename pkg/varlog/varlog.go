package varlog

import (
	"context"

	varlogpb "github.com/kakao/varlog/proto/varlog"
)

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
	metadata        *varlogpb.MetadataDescriptor
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

	metadata, err := varlog.fetchMetadata()
	if err != nil {
		return nil, err
	}

	err = varlog.applyMetadata(metadata)
	if err != nil {
		return nil, err
	}

	return varlog, nil
}

func (s *varlog) Read(glsn uint64) ([]byte, error) {
	prj, err := s.getProjection(glsn)
	if err != nil {
		return nil, err
	}

	logStream, err := prj.GetLogStream(glsn)
	if err != nil {
		return nil, err
	}

	replicas := logStream.GetReplicas()

	snID := replicas[len(replicas)-1].GetStorageNodeId()
	snClient := s.snClientMap[snID]

	return snClient.Read(context.Background(), s.epoch, glsn)
}

func (s *varlog) Append(data []byte) (uint64, error) {
	glsn, err := s.sqrClient.Next(context.Background())
	if err != nil {
		return 0, err
	}
	prj, err := s.getProjection(glsn)
	if err != nil {
		return 0, err
	}

	logStream, err := prj.GetLogStream(glsn)
	if err != nil {
		return 0, err
	}

	replicas := logStream.GetReplicas()

	for _, replica := range replicas {
		snID := replica.GetStorageNodeId()
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

func (s *varlog) fetchMetadata() (*varlogpb.MetadataDescriptor, error) {
	ctx := context.Background()
	meta, err := s.metaReposClient.GetMetadata(ctx)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// FIXME: it should be more precise method
func (s *varlog) applyMetadata(metadata *varlogpb.MetadataDescriptor) error {
	if metadata == nil {
		return ErrInvalidProjection
	}
	newEpoch := metadata.GetLastEpoch()
	if s.epoch > newEpoch {
		return ErrInvalidEpoch
	}

	newSqr := metadata.GetSequencer()
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
	for _, newSn := range metadata.GetStorageNodes() {
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
	s.metadata = metadata
	return nil
}

func (s *varlog) getProjection(glsn uint64) (*varlogpb.ProjectionDescriptor, error) {
	return s.metadata.GetProjection(glsn), nil
}
