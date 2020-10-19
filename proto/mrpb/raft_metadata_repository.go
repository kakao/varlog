package mrpb

import (
	"sort"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func (s *MetadataRepositoryDescriptor) LookupGlobalLogStreamByPrev(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	i := sort.Search(len(s.LogStream.GlobalLogStreams), func(i int) bool {
		return s.LogStream.GlobalLogStreams[i].PrevHighWatermark >= glsn
	})

	if i < len(s.LogStream.GlobalLogStreams) && s.LogStream.GlobalLogStreams[i].PrevHighWatermark == glsn {
		return s.LogStream.GlobalLogStreams[i]
	}

	return nil
}

func (s *MetadataRepositoryDescriptor) LookupGlobalLogStream(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	i := sort.Search(len(s.LogStream.GlobalLogStreams), func(i int) bool {
		return s.LogStream.GlobalLogStreams[i].HighWatermark >= glsn
	})

	if i < len(s.LogStream.GlobalLogStreams) && s.LogStream.GlobalLogStreams[i].HighWatermark == glsn {
		return s.LogStream.GlobalLogStreams[i]
	}

	return nil
}

func (s *MetadataRepositoryDescriptor) GetLastGlobalLogStream() *snpb.GlobalLogStreamDescriptor {
	n := len(s.LogStream.GlobalLogStreams)
	if n == 0 {
		return nil
	}

	return s.LogStream.GlobalLogStreams[n-1]
}

func (s *MetadataRepositoryDescriptor) GetFirstGlobalLogStream() *snpb.GlobalLogStreamDescriptor {
	n := len(s.LogStream.GlobalLogStreams)
	if n == 0 {
		return nil
	}

	return s.LogStream.GlobalLogStreams[0]
}

func (l *MetadataRepositoryDescriptor_LocalLogStreamReplicas) Deleted() bool {
	return l.Status == varlogpb.LogStreamStatusDeleted
}

func (r *MetadataRepositoryDescriptor_LocalLogStreamReplica) UncommittedLLSNEnd() types.LLSN {
	// return exclusive end
	if r == nil {
		return types.InvalidLLSN
	}

	return r.UncommittedLLSNOffset + types.LLSN(r.UncommittedLLSNLength)
}

func (r *MetadataRepositoryDescriptor_LocalLogStreamReplica) Seal(end types.LLSN) types.LLSN {
	if r == nil {
		return types.InvalidLLSN
	}

	if end < r.UncommittedLLSNOffset {
		return types.InvalidLLSN
	}

	if end > r.UncommittedLLSNEnd() {
		return types.InvalidLLSN
	}

	r.UncommittedLLSNLength = uint64(end - r.UncommittedLLSNOffset)

	return r.UncommittedLLSNEnd()
}
