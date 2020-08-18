package varlog

import types "github.com/kakao/varlog/pkg/varlog/types"

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
