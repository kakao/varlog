package storage

import (
	pb "github.com/kakao/varlog/proto/storage_node"
)

// LogStreamReporterClient contains the functionality of bi-directional communication about local
// log stream and global log stream.
type LogStreamRepoterClient interface {
	GetReport() (pb.LocalLogStreamDescriptor, error)
	Commit(pb.GlobalLogStreamDescriptor) error
}
