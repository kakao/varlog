package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
)

type Management interface {
	AddPeer(context.Context, types.ClusterID, types.NodeID, string) error
	RemovePeer(context.Context, types.ClusterID, types.NodeID) error
	GetClusterInfo(context.Context, types.ClusterID) (*mrpb.ClusterInfo, error)
}
