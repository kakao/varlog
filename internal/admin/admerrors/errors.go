package admerrors

import (
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
)

var (
	ErrNoSuchStorageNode         = status.Error(codes.NotFound, "no such storage node")
	ErrNoSuchTopic               = status.Error(codes.NotFound, "no such topic")
	ErrNoSuchLogStream           = status.Error(codes.NotFound, "no such log stream")
	ErrClusterMetadataNotFetched = status.Error(codes.Unavailable, "cluster metadata not fetched")
	ErrNotIdleReplicas           = status.Error(codes.FailedPrecondition, "not idle replicas")
)
