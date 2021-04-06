package logio

import (
	"context"

	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
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
