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
	First   snpb.SyncPosition
	Last    snpb.SyncPosition
	Current snpb.SyncPosition
	Err     error
	Cancel  context.CancelFunc
	Mu      sync.RWMutex
}

type Replicator interface {
	Replicate(ctx context.Context, llsn types.LLSN, data []byte) error
	SyncReplicate(ctx context.Context, first, last snpb.SyncPosition, payload snpb.SyncPayload) error
	Sync(ctx context.Context, replica snpb.Replica, lastGLSN types.GLSN) (*SyncTaskStatus, error)
}

type Getter interface {
	Replicator(logStreamID types.LogStreamID) (Replicator, bool)
	Replicators() []Replicator
}
