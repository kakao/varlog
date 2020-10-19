package metadata_repository

import (
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/proto/varlogpb"
)

type ReporterClientFactory interface {
	GetClient(*varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error)
}
