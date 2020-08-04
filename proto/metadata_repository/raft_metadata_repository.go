package metadata_repository

import (
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

func (l *MetadataRepositoryDescriptor_LocalLogStreamReplicas) Deleted() bool {
	return l.Status == varlogpb.LogStreamStatusDeleted
}
