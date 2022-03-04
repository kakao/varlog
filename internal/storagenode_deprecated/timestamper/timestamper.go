package timestamper

import (
	"sync/atomic"
	"time"
)

type Timestamper interface {
	Created() time.Time
	LastUpdated() time.Time
	Touch()
}
type timestamper struct {
	created time.Time
	updated atomic.Value
}

func New() Timestamper {
	now := time.Now()
	ts := &timestamper{
		created: now,
	}
	ts.updated.Store(now)
	return ts
}

func (ts *timestamper) Created() time.Time {
	return ts.created
}

func (ts *timestamper) LastUpdated() time.Time {
	return ts.updated.Load().(time.Time)
}

func (ts *timestamper) Touch() {
	ts.updated.Store(time.Now())
}
