package types

type LogEntry struct {
	GLSN GLSN
	LLSN LLSN
	Data []byte
}

var InvalidLogEntry = LogEntry{
	GLSN: InvalidGLSN,
	LLSN: InvalidLLSN,
	Data: nil,
}

func (le LogEntry) Invalid() bool {
	return le.GLSN.Invalid() && le.LLSN.Invalid() && len(le.Data) == 0
}
