package replication

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/replication -package replication -destination replication_mock.go . Replicator,Getter

import (
	"context"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type SyncTaskStatus struct {
	Replica varlogpb.Replica
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
	Sync(ctx context.Context, replica varlogpb.Replica) (*snpb.SyncStatus, error)
}

// Getter is an interface that gets Replicator.
//
// Replicator returns a Replicator corresponded with the argument topicID and the argument
// logStreamID.
type Getter interface {
	Replicator(topicID types.TopicID, logStreamID types.LogStreamID) (Replicator, bool)
}
