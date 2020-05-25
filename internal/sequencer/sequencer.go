package sequencer

import (
	"sync/atomic"
)

type Sequencer struct {
	glsn uint64
}

func NewSequencer() *Sequencer {
	return &Sequencer{
		glsn: 0,
	}
}

func (s *Sequencer) Next() uint64 {
	return s.next(1)
}

func (s *Sequencer) Peek() uint64 {
	return atomic.LoadUint64(&s.glsn)
}

func (s *Sequencer) next(delta uint64) uint64 {
	return atomic.AddUint64(&s.glsn, delta) - delta
}
