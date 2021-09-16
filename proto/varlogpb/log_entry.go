package varlogpb

import "github.daumkakao.com/varlog/varlog/pkg/types"

var invalidLogEntry = LogEntry{
	GLSN: types.InvalidGLSN,
	LLSN: types.InvalidLLSN,
	Data: nil,
}

func InvalidLogEntry() LogEntry {
	return invalidLogEntry
}

func (le LogEntry) Invalid() bool {
	return le.GLSN.Invalid() && le.LLSN.Invalid() && len(le.Data) == 0
}
