package metadata_repository

import (
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type reporterClientFactory struct {
}

func NewReporterClientFactory() *reporterClientFactory {
	return &reporterClientFactory{}
}

func (rcf *reporterClientFactory) GetClient(sn *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error) {
	return storagenode.NewLogStreamReporterClient(sn.Address)
}
