package metadata_repository

import (
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(sn *varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error) {
	return storage.NewLogStreamReporterClient(sn.Address)
}
