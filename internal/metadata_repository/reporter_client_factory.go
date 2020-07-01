package metadata_repository

import (
	"github.daumkakao.com/varlog/varlog/internal/storage"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type ReporterClientFactory interface {
	GetClient(*varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error)
}
