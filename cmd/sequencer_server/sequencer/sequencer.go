package sequencer

import "sync/atomic"

type sequencer struct {
	glsn uint64
}

func newSequencer() *sequencer {
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

func (s *sequencer) next(count uint64) uint64 {
	return atomic.AddUint64(&s.glsn, count) - count
}
