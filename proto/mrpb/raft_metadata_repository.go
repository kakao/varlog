package mrpb

import (
	"sort"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func (s *MetadataRepositoryDescriptor) LookupCommitResultsByPrev(glsn types.GLSN) *LogStreamCommitResults {
	i := sort.Search(len(s.LogStream.CommitHistory), func(i int) bool {
		return s.LogStream.CommitHistory[i].PrevHighWatermark >= glsn
	})

	if i < len(s.LogStream.CommitHistory) && s.LogStream.CommitHistory[i].PrevHighWatermark == glsn {
		return s.LogStream.CommitHistory[i]
	}

	return nil
}

func (s *MetadataRepositoryDescriptor) LookupCommitResults(glsn types.GLSN) *LogStreamCommitResults {
	i := sort.Search(len(s.LogStream.CommitHistory), func(i int) bool {
		return s.LogStream.CommitHistory[i].HighWatermark >= glsn
	})

	if i < len(s.LogStream.CommitHistory) && s.LogStream.CommitHistory[i].HighWatermark == glsn {
		return s.LogStream.CommitHistory[i]
	}

	return nil
}

func (s *MetadataRepositoryDescriptor) GetLastCommitResults() *LogStreamCommitResults {
	n := len(s.LogStream.CommitHistory)
	if n == 0 {
		return nil
	}

	return s.LogStream.CommitHistory[n-1]
}

func (s *MetadataRepositoryDescriptor) GetFirstCommitResults() *LogStreamCommitResults {
	n := len(s.LogStream.CommitHistory)
	if n == 0 {
		return nil
	}

	return s.LogStream.CommitHistory[0]
}

func (crs *LogStreamCommitResults) LookupCommitResult(lsID types.LogStreamID) (snpb.LogStreamCommitResult, bool) {
	if crs == nil {
		return snpb.InvalidLogStreamCommitResult, false
	}

	i := sort.Search(len(crs.CommitResults), func(i int) bool {
		return crs.CommitResults[i].LogStreamID >= lsID
	})

	if i < len(crs.CommitResults) && crs.CommitResults[i].LogStreamID == lsID {
		return crs.CommitResults[i], true
	}

	return snpb.InvalidLogStreamCommitResult, false
}

func (l *LogStreamUncommitReports) Deleted() bool {
	return l.Status == varlogpb.LogStreamStatusDeleted
}

func (l *StorageNodeUncommitReport) Len() int {
	return len(l.UncommitReports)
}

func (l *StorageNodeUncommitReport) Swap(i, j int) {
	l.UncommitReports[i], l.UncommitReports[j] = l.UncommitReports[j], l.UncommitReports[i]
}

func (l *StorageNodeUncommitReport) Less(i, j int) bool {
	return l.UncommitReports[i].LogStreamID < l.UncommitReports[j].LogStreamID
}

func (l *StorageNodeUncommitReport) Sort() {
	sort.Sort(l)
}

func (l *StorageNodeUncommitReport) LookupReport(lsID types.LogStreamID) (snpb.LogStreamUncommitReport, bool) {
	if l == nil {
		return snpb.InvalidLogStreamUncommitReport, false
	}

	i := sort.Search(l.Len(), func(i int) bool { return l.UncommitReports[i].LogStreamID >= lsID })
	if i < l.Len() && l.UncommitReports[i].LogStreamID == lsID {
		return l.UncommitReports[i], true
	}
	return snpb.InvalidLogStreamUncommitReport, false
}
