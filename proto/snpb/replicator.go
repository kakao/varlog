package snpb

import (
	"fmt"

	"github.com/kakao/varlog/pkg/types"
)

// InvalidSyncPosition returns a SyncPosition with both LLSN and GLSN set to
// types.InvalidGLSN.
func InvalidSyncPosition() SyncPosition {
	return SyncPosition{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
}

// Invalid checks if the SyncPosition is invalid. A SyncPosition is considered
// invalid if either LLSN or GLSN is invalid.
func (sp SyncPosition) Invalid() bool {
	return sp.LLSN.Invalid() || sp.GLSN.Invalid()
}

// LessThan checks if the current SyncPosition "sp" is less than another
// SyncPosition "other". It returns true if both LLSN and GLSN of the current
// SyncPosition are less than those of the other SyncPosition.
func (sp SyncPosition) LessThan(other SyncPosition) bool {
	return sp.LLSN < other.LLSN && sp.GLSN < other.GLSN
}

// InvalidSyncRange returns a SyncRange with both FirstLLSN and LastLLSN set to
// types.InvalidLLSN.
func InvalidSyncRange() SyncRange {
	return SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}
}

// Invalid determines if the SyncRange is invalid. A SyncRange is considered
// invalid if either FirstLLSN or LastLLSN is invalid, or if FirstLLSN is
// greater than LastLLSN.
func (sr SyncRange) Invalid() bool {
	return sr.FirstLLSN.Invalid() || sr.LastLLSN.Invalid() || sr.FirstLLSN > sr.LastLLSN
}

// Validate checks the validity of the SyncRange. It returns an error if
// FirstLLSN is greater than LastLLSN, or if FirstLLSN is invalid while
// LastLLSN is valid. If both FirstLLSN and LastLLSN are invalid, it returns
// nil, indicating that the entire log range is considered trimmed.
func (sr SyncRange) Validate() error {
	if sr.FirstLLSN > sr.LastLLSN {
		return fmt.Errorf("invalid sync range: first %d, last %d", sr.FirstLLSN, sr.LastLLSN)
	}
	if sr.FirstLLSN.Invalid() && !sr.LastLLSN.Invalid() {
		return fmt.Errorf("invalid sync range: only first invalid: first %d, last %d", sr.FirstLLSN, sr.LastLLSN)
	}
	return nil
}
