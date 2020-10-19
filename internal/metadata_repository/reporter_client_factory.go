package metadata_repository

import (
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetClient(*varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error)
}
