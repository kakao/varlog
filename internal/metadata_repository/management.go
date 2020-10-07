package metadata_repository

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
)

type Management interface {
	AddPeer(context.Context, types.ClusterID, types.NodeID, string) error
	RemovePeer(context.Context, types.ClusterID, types.NodeID) error
	GetClusterInfo(context.Context, types.ClusterID) (*pb.ClusterInfo, error)
}
