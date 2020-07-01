package metadata_repository

import (
	"github.com/kakao/varlog/internal/storage"
	varlogpb "github.com/kakao/varlog/proto/varlog"
)

type ReporterClientFactory interface {
	GetClient(*varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error)
}
