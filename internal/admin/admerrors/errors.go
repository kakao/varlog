package admerrors

import (
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
)

var (
	ErrNoSuchStorageNode = status.Error(codes.NotFound, "no such storage node")
	ErrNoSuchTopic       = status.Error(codes.NotFound, "no such topic")
	// ErrNoSuchLogStream indicates that the log stream does not exist.
	ErrNoSuchLogStream = status.Error(codes.NotFound, "no such log stream")
	// ErrClusterMetadataNotFetched indicates that the cluster metadata
	// cannot be fetched from the metadata repository.
	// It is usually resolved by retrying with a backoff.
	ErrClusterMetadataNotFetched     = status.Error(codes.Unavailable, "cluster metadata not fetched")
	ErrNotIdleReplicas               = status.Error(codes.FailedPrecondition, "not idle replicas")
	ErrMetadataRepositoryUnavailable = status.Error(codes.Unavailable, "metadata repository unavailable")
)
