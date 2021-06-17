package replication

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/replication -package replication -destination replication_mock.go . Replicator,Getter

import (
	"context"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type SyncTaskStatus struct {
	Replica snpb.Replica
	State   snpb.SyncState
	Span    snpb.SyncRange
	Curr    types.LLSN
	Err     error
	Cancel  context.CancelFunc
	Mu      sync.RWMutex
}

type Replicator interface {
	Replicate(ctx context.Context, llsn types.LLSN, data []byte) error
	SyncInit(ctx context.Context, srcRnage snpb.SyncRange) (snpb.SyncRange, error)
	SyncReplicate(ctx context.Context, payload snpb.SyncPayload) error
	Sync(ctx context.Context, replica snpb.Replica) (*snpb.SyncStatus, error)
}

type Getter interface {
	Replicator(logStreamID types.LogStreamID) (Replicator, bool)
	Replicators() []Replicator
}
