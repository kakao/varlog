package metadata_repository

import (
	"github.daumkakao.com/varlog/varlog/internal/storage"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(sn *varlogpb.StorageNodeDescriptor) (storage.LogStreamReporterClient, error) {
	return storage.NewLogStreamReporterClient(sn.Address)
}
