package metadata_repository

import (
	"context"

	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/metadata_repository"
)

type Management interface {
	AddPeer(context.Context, types.ClusterID, types.NodeID, string) error
	RemovePeer(context.Context, types.ClusterID, types.NodeID) error
	GetClusterInfo(context.Context, types.ClusterID) (*pb.ClusterInfo, error)
}
