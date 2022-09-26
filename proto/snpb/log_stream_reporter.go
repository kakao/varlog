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

func (m *LogStreamUncommitReport) UncommittedLLSNEnd() types.LLSN {
	if m == nil {
		return types.InvalidLLSN
	}

	return m.UncommittedLLSNOffset + types.LLSN(m.UncommittedLLSNLength)
}

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
