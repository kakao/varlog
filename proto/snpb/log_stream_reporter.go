package snpb

import (
	"github.com/kakao/varlog/pkg/types"
)

// InvalidLogStreamUncommitReport is empty report. Do **NOT** modify this.
var InvalidLogStreamUncommitReport = LogStreamUncommitReport{}

var InvalidLogStreamCommitResult = LogStreamCommitResult{}

// Invalid returns whether the LogStreamUncommitReport is acceptable.
// LogStreamUncommitReport with invalid logStream or invalid uncommittedLLSNOffset
// is not acceptable. MetadataRepository ignores these reports.
func (m *LogStreamUncommitReport) Invalid() bool {
	return m.GetLogStreamID().Invalid() || m.GetUncommittedLLSNOffset().Invalid()
}

// UncommittedLLSNEnd returns the exclusive end of uncommitted LLSN spans. If
// the LogStreamUncommitReport is nil, it returns types.InvalidLLSN. Otherwise,
// it calculates the end by adding the UncommittedLLSNOffset and the
// UncommittedLLSNLength.
//
// Example: If UncommittedLLSNOffset is 10 and UncommittedLLSNLength is 5, the
// method will return 15 (10 + 5).
func (m *LogStreamUncommitReport) UncommittedLLSNEnd() types.LLSN {
	if m == nil {
		return types.InvalidLLSN
	}

	return m.UncommittedLLSNOffset + types.LLSN(m.UncommittedLLSNLength)
}

// Seal finalizes the uncommitted LLSN spans in the LogStreamUncommitReport up
// to the specified end LLSN. The 'end' parameter specifies the LLSN up to
// which the uncommitted spans should be sealed. The method returns the new end
// LLSN if the operation is successful, or types.InvalidLLSN if the provided
// end LLSN is invalid.
//
// This method adjusts the internal state to reflect that the spans up to 'end'
// are now sealed. If the LogStreamUncommitReport is nil, or if the provided
// end LLSN is outside the valid range (less than the starting offset or beyond
// the current uncommitted end), the method returns types.InvalidLLSN and makes
// no changes.
//
// For example, given a report with a starting offset of 10 and an uncommitted
// length of 5 (covering spans 10 to 15), calling Seal(12) will mark spans up
// to 12 as sealed, set the uncommitted length to 2 (12 - 10), and return 12.
func (m *LogStreamUncommitReport) Seal(end types.LLSN) types.LLSN {
	if m == nil {
		return types.InvalidLLSN
	}

	if end < m.UncommittedLLSNOffset {
		return types.InvalidLLSN
	}

	if end > m.UncommittedLLSNEnd() {
		return types.InvalidLLSN
	}

	m.UncommittedLLSNLength = uint64(end - m.UncommittedLLSNOffset)

	return m.UncommittedLLSNEnd()
}
