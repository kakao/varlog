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
