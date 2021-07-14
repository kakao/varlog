package snpb

import (
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

// InvalidLogStreamUncommitReport is empty report. Do **NOT** modify this.
var InvalidLogStreamUncommitReport = LogStreamUncommitReport{}
var InvalidLogStreamCommitResult = LogStreamCommitResult{}

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
