package varlog

import (
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type OpenMode int

type Options struct {
	MetadataRepositoryAddress string
}

type LogStreamSelectionPolicy int

const (
	RanomSelection LogStreamSelectionPolicy = iota
	CustomSelection
)

type AppendOption struct {
	LSSPolicy LogStreamSelectionPolicy
}

// Varlog is a log interface with thread-safety. Many goroutines can share the same varlog object.
type Varlog interface {
	Append(data []byte, opts AppendOption) (uint64, error)
	Read(glsn uint64, logStreamID string) ([]byte, error)
	Subscribe(glsn uint64) (<-chan []byte, error)
	Trim(glsn uint64) error
	Close() error
}

type varlog struct {
	logID string

	logStreams     []LogStreamID
	storageNodes   []StorageNodeID
	replicationMap map[LogStreamID][]StorageNodeID
	storageMap     map[StorageNodeID]StorageNodeClient

	metaReposClient MetadataRepositoryClient
	metadata        *varlogpb.MetadataDescriptor
}

// Open creates new logs or opens an already created logs.
func Open(logID string, opts Options) (Varlog, error) {
	metaReposClient, err := NewMetadataRepositoryClient(opts.MetadataRepositoryAddress)
	if err != nil {
		return nil, err
	}
	varlog := &varlog{
		logID:           logID,
		metaReposClient: metaReposClient,
	}
	return varlog, nil
}

func (s *varlog) Append(data []byte, opts AppendOption) (uint64, error) {
	panic("not yet implemented")
}

func (s *varlog) Read(glsn uint64, logStreamID string) ([]byte, error) {
	panic("not yet implemented")
}

func (v *varlog) Subscribe(glsn uint64) (<-chan []byte, error) {
	panic("not implemented")
}

func (s *varlog) Trim(glsn uint64) error {
	panic("not implemented")
}

func (s *varlog) Close() error {
	panic("not implemented")
}
