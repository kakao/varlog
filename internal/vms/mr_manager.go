package vms

import (
	"context"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepositoryManager interface {
	GetClusterMetadata(ctx context.Context) (*vpb.MetadataDescriptor, error)

	RegisterStorageNode(ctx context.Context, storageNodeMeta *vpb.StorageNodeDescriptor) error

	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error

	RegisterLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error

	UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) error

	UpdateLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error

	Seal(ctx context.Context, logStreamID types.LogStreamID) (lastCommittedGLSN types.GLSN, err error)

	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	/*
		AddPeer()
		RemovePeer()
	*/
}
