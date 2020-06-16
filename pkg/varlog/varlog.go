package varlog

import (
	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
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
	Append(data []byte, opts AppendOption) (types.GLSN, error)
	AppendTo(logStreamID types.LogStreamID, data []byte, opts AppendOption) (types.GLSN, error)
	Read(logStreamID types.LogStreamID, glsn types.GLSN) ([]byte, error)
	Subscribe(glsn types.GLSN) (<-chan []byte, error)
	Trim(glsn types.GLSN) error
	Close() error
}

type varlog struct {
	logID string

	logStreams     []types.LogStreamID
	storageNodes   []types.StorageNodeID
	replicationMap map[types.LogStreamID][]types.StorageNodeID
	storageMap     map[types.StorageNodeID]StorageNodeClient

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

func (s *varlog) Append(data []byte, opts AppendOption) (types.GLSN, error) {
	panic("not yet implemented")
}

func (s *varlog) AppendTo(logStreamID types.LogStreamID, data []byte, opts AppendOption) (types.GLSN, error) {
	panic("not yet implemented")
}

func (s *varlog) Read(logStreamID types.LogStreamID, glsn types.GLSN) ([]byte, error) {
	panic("not yet implemented")
}

func (v *varlog) Subscribe(glsn types.GLSN) (<-chan []byte, error) {
	panic("not implemented")
}

func (s *varlog) Trim(glsn types.GLSN) error {
	panic("not implemented")
}

func (s *varlog) Close() error {
	panic("not implemented")
}
