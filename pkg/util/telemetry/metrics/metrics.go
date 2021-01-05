package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Metadata struct {
	Name        string
	Description string
	Attributes  map[string]string
}

type Counter struct {
	count int64
}

func (c *Counter) Add(delta int64) {
	atomic.AddInt64(&c.count, delta)
}

func (c *Counter) Count() int64 {
	return atomic.LoadInt64(&c.count)
}

type Guage struct {
	value uint64
}

func (g *Guage) Update(value float64) {
	atomic.StoreUint64(&g.value, math.Float64bits(value))
}

func (g *Guage) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.value))
}

type Recorder struct {
	values     []uint64
	timestamps []time.Time
	mu         sync.RWMutex
}

func (r *Recorder) Record(value float64) {
	r.mu.Lock()
	r.values = append(r.values, math.Float64bits(value))
	r.timestamps = append(r.timestamps, time.Now())
	r.mu.Unlock()
}
