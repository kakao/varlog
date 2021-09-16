package mrpb

import (
	"sort"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func (s *MetadataRepositoryDescriptor) LookupCommitResults(ver types.Version) *LogStreamCommitResults {
	i := sort.Search(len(s.LogStream.CommitHistory), func(i int) bool {
		return s.LogStream.CommitHistory[i].Version >= ver
	})

	if i < len(s.LogStream.CommitHistory) && s.LogStream.CommitHistory[i].Version == ver {
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

func (crs *LogStreamCommitResults) LookupCommitResult(topicID types.TopicID, lsID types.LogStreamID, hintPos int) (snpb.LogStreamCommitResult, int, bool) {
	if crs == nil {
		return snpb.InvalidLogStreamCommitResult, -1, false
	}

	cr, ok := crs.getCommitResultByIdx(hintPos)
	if ok && cr.LogStreamID == lsID {
		return cr, hintPos, true
	}

	i := sort.Search(len(crs.CommitResults), func(i int) bool {
		if crs.CommitResults[i].TopicID == topicID {
			return crs.CommitResults[i].LogStreamID >= lsID
		}

		return crs.CommitResults[i].TopicID >= topicID
	})

	if i < len(crs.CommitResults) && crs.CommitResults[i].LogStreamID == lsID {
		return crs.CommitResults[i], i, true
	}

	return snpb.InvalidLogStreamCommitResult, -1, false
}

func (crs *LogStreamCommitResults) getCommitResultByIdx(idx int) (snpb.LogStreamCommitResult, bool) {
	if crs == nil || idx < 0 || len(crs.CommitResults) <= idx {
		return snpb.InvalidLogStreamCommitResult, false
	}

	return crs.CommitResults[idx], true
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

// return HighWatermark of the topic
// TODO:: lookup last logStream of the topic
func (crs *LogStreamCommitResults) LastHighWatermark(topicID types.TopicID, hintPos int) (types.GLSN, int) {
	if crs == nil {
		return types.InvalidGLSN, -1
	}

	n := len(crs.GetCommitResults())
	if n == 0 {
		return types.InvalidGLSN, -1
	}

	cr, ok := crs.getCommitResultByIdx(hintPos)
	if ok && cr.TopicID == topicID {
		nxt, ok := crs.getCommitResultByIdx(hintPos + 1)
		if !ok || nxt.TopicID != topicID {
			return cr.GetHighWatermark(), hintPos
		}
	}

	i := sort.Search(len(crs.CommitResults), func(i int) bool {
		return crs.CommitResults[i].TopicID >= topicID+1
	})

	if i > 0 {
		return crs.GetCommitResults()[i-1].GetHighWatermark(), i - 1
	}

	return types.InvalidGLSN, -1
}
