package sequencer

import (
	"sync/atomic"
)

type Sequencer interface {
	Next() uint64
	Peek() uint64
}

type sequencer struct {
	glsn uint64
}

func NewSequencer() Sequencer {
	return &sequencer{
		glsn: 0,
	}
}

func (s *sequencer) Next() uint64 {
	return s.next(1)
}

func (s *sequencer) Peek() uint64 {
	return atomic.LoadUint64(&s.glsn)
}

func (s *sequencer) next(delta uint64) uint64 {
	return atomic.AddUint64(&s.glsn, delta) - delta
}
