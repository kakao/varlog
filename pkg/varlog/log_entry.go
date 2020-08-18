package varlog

import types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"

type LogEntry struct {
	GLSN types.GLSN
	LLSN types.LLSN
	Data []byte
}

var InvalidLogEntry = LogEntry{
	GLSN: types.InvalidGLSN,
	LLSN: types.InvalidLLSN,
	Data: nil,
}
