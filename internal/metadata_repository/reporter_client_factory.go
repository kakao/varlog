package metadata_repository

import (
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetClient(*varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error)
}
