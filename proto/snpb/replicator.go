package snpb

import (
	"fmt"

	"github.com/kakao/varlog/pkg/types"
)

func (m *ReplicateRequest) ResetReuse() {
	llsnSlice := m.LLSN[:0]
	for i := range m.Data {
		m.Data[i] = m.Data[i][:0]
	}
	dataSlice := m.Data[:0]
	m.Reset()
	m.LLSN = llsnSlice
	m.Data = dataSlice
}

func InvalidSyncPosition() SyncPosition {
	return SyncPosition{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
}

func (sp SyncPosition) Invalid() bool {
	return sp.LLSN.Invalid() || sp.GLSN.Invalid()
}

func (sp SyncPosition) LessThan(other SyncPosition) bool {
	return sp.LLSN < other.LLSN && sp.GLSN < other.GLSN
}

func InvalidSyncRange() SyncRange {
	return SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}
}

func (sr SyncRange) Invalid() bool {
	return sr.FirstLLSN.Invalid() || sr.LastLLSN.Invalid() || sr.FirstLLSN > sr.LastLLSN
}

func (sr SyncRange) Validate() error {
	if sr.FirstLLSN > sr.LastLLSN {
		return fmt.Errorf("invalid sync range: first %d, last %d", sr.FirstLLSN, sr.LastLLSN)
	}
	if sr.FirstLLSN.Invalid() && !sr.LastLLSN.Invalid() {
		return fmt.Errorf("invalid sync range: only first invalid: first %d, last %d", sr.FirstLLSN, sr.LastLLSN)
	}
	return nil
}
