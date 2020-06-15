package storage

import (
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
)

// LogStreamReporterClient contains the functionality of bi-directional communication about local
// log stream and global log stream.
type LogStreamRepoterClient interface {
	GetReport() (pb.LocalLogStreamDescriptor, error)
	Commit(pb.GlobalLogStreamDescriptor) error
}
