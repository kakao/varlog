package vms

import (
	"context"
	"io"
	"math/rand"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	vpb "github.com/kakao/varlog/proto/varlog"
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
	io.Closer
}

var _ MetadataRepositoryManager = (*mrManager)(nil)

type mrManager struct {
	cs []varlog.MetadataRepositoryClient
}

func NewMRManager(mrAddrs []string) (MetadataRepositoryManager, error) {
	mrm := &mrManager{
		cs: make([]varlog.MetadataRepositoryClient, 0, len(mrAddrs)),
	}

	for _, addr := range mrAddrs {
		mrc, err := varlog.NewMetadataRepositoryClient(addr)
		if err != nil {
			// TODO: skip or error?
			return nil, err
		}
		mrm.cs = append(mrm.cs, mrc)
	}
	return mrm, nil
}

func (mrm mrManager) c() varlog.MetadataRepositoryClient {
	i := rand.Intn(len(mrm.cs))
	return mrm.cs[i]
}

func (mrm *mrManager) Close() error {
	// TODO: guard multiple calls?
	var err error
	for _, c := range mrm.cs {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (mrm *mrManager) GetClusterMetadata(ctx context.Context) (*vpb.MetadataDescriptor, error) {
	return mrm.c().GetMetadata(ctx)
}

func (mrm *mrManager) RegisterStorageNode(ctx context.Context, storageNodeMeta *vpb.StorageNodeDescriptor) error {
	return mrm.c().RegisterStorageNode(ctx, storageNodeMeta)
}

func (mrm *mrManager) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	panic("not implemented")
}

func (mrm *mrManager) RegisterLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error {
	return mrm.c().RegisterLogStream(ctx, logStreamDesc)
}

func (mrm *mrManager) UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (mrm *mrManager) UpdateLogStream(ctx context.Context, logStreamDesc *vpb.LogStreamDescriptor) error {
	panic("not implemented")
}

func (mrm *mrManager) Seal(ctx context.Context, logStreamID types.LogStreamID) (lastCommittedGLSN types.GLSN, err error) {
	panic("not implemented")
}

func (mrm *mrManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}
