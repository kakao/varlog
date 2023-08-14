package mrpb

import (
	"sync"
	"time"

	"github.com/kakao/varlog/pkg/types"
)

var reportsPool = sync.Pool{
	New: func() any {
		return &Reports{}
	},
}

func NewReports(nodeID types.NodeID, ts time.Time) *Reports {
	rs := reportsPool.Get().(*Reports)
	rs.NodeID = nodeID
	rs.CreatedTime = ts
	return rs
}

func (rs *Reports) Release() {
	if rs != nil {
		rq := (ReportQueue)(rs.Reports)
		rq.Release()
		*rs = Reports{}
		reportsPool.Put(rs)
	}
}

const (
	reportQueueSize = 1024
)

type ReportQueue []*Report

var reportQueuePool = sync.Pool{
	New: func() any {
		q := make(ReportQueue, 0, reportQueueSize)
		return &q
	},
}

func NewReportQueue() ReportQueue {
	rq := reportQueuePool.Get().(*ReportQueue)
	return *rq
}

func (rq *ReportQueue) Release() {
	if rq != nil {
		*rq = (*rq)[0:0:reportQueueSize]
		reportQueuePool.Put(rq)
	}
}

var raftEntryPool = sync.Pool{
	New: func() any {
		return &RaftEntry{}
	},
}

func NewRaftEntry() *RaftEntry {
	return raftEntryPool.Get().(*RaftEntry)
}

func (re *RaftEntry) Release() {
	if re != nil {
		re.Request.Report.Release()
		re.Reset()
		raftEntryPool.Put(re)
	}
}
