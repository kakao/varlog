package logio

import (
	"context"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type ReadWriter interface {
	Append(ctx context.Context, data []byte, backups ...snpb.Replica) (types.GLSN, error)
	Read(ctx context.Context, glsn types.GLSN) (types.LogEntry, error)
	Subscribe(ctx context.Context, begin, end types.GLSN) (SubscribeEnv, error)
	Trim(ctx context.Context, glsn types.GLSN) error
}

type SubscribeEnv interface {
	ScanResultC() <-chan storage.ScanResult
	Stop()
	Err() error
}

type Getter interface {
	ReadWriter(logStreamID types.LogStreamID) (ReadWriter, bool)
	ReadWriters() []ReadWriter
}
