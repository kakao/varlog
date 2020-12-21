package snpb

import (
	"sort"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func (l *LocalLogStreamDescriptor) Len() int {
	return len(l.Uncommit)
}

func (l *LocalLogStreamDescriptor) Swap(i, j int) {
	l.Uncommit[i], l.Uncommit[j] = l.Uncommit[j], l.Uncommit[i]
}

func (l *LocalLogStreamDescriptor) Less(i, j int) bool {
	return l.Uncommit[i].LogStreamID < l.Uncommit[j].LogStreamID
}

func (l *LocalLogStreamDescriptor) Sort() {
	sort.Sort(l)
}

func (l *LocalLogStreamDescriptor) LookupReport(lsID types.LogStreamID) *LocalLogStreamDescriptor_LogStreamUncommitReport {
	i := sort.Search(l.Len(), func(i int) bool { return l.Uncommit[i].LogStreamID >= lsID })
	if i < l.Len() && l.Uncommit[i].LogStreamID == lsID {
		return l.Uncommit[i]
	}
	return nil
}

func (u *LocalLogStreamDescriptor_LogStreamUncommitReport) UncommitedLLSNEnd() types.LLSN {
	return u.UncommittedLLSNOffset + types.LLSN(u.UncommittedLLSNLength)
}
