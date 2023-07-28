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
		*rs = Reports{}
		reportsPool.Put(rs)
	}
}
