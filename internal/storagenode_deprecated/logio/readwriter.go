package logio

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// ReadWriter represents methods to read or write logs in a log stream.
type ReadWriter interface {
	// Append writes a log to the log stream.
	Append(ctx context.Context, data [][]byte, backups ...varlogpb.Replica) ([]snpb.AppendResult, error)

	// Read reads a log with the given glsn.
	Read(ctx context.Context, glsn types.GLSN) (varlogpb.LogEntry, error)

	// Subscribe scans logs from the inclusive begin to the exclusive end.
	Subscribe(ctx context.Context, begin, end types.GLSN) (SubscribeEnv, error)

	// Subscribe scans logs from the inclusive begin to the exclusive end.
	SubscribeTo(ctx context.Context, begin, end types.LLSN) (SubscribeEnv, error)

	// Trim removes logs until glsn.
	Trim(ctx context.Context, glsn types.GLSN) error

	// LogStreamMetadata returns metadata of log stream.
	// FIXME (jun): It is similar to MetadataProvider.
	LogStreamMetadata() (varlogpb.LogStreamDescriptor, error)
}

type SubscribeEnv interface {
	ScanResultC() <-chan storage.ScanResult
	Stop()
	Err() error
}

// Getter is the interface that wraps basic methods to access ReadWriter.
//
// ReadWriter returns a ReadWriter corresponded with the argument topicID and the argument
// logStreamID.
//
// ForEachReadWriters iterates all of ReadWriters and calls the argument f respectively.
type Getter interface {
	ReadWriter(topicID types.TopicID, logStreamID types.LogStreamID) (ReadWriter, bool)
	ForEachReadWriters(f func(ReadWriter))
}
