package varlogpb

func InvalidLogEntryMeta() LogEntryMeta {
	return LogEntryMeta{}
}

func InvalidLogEntry() LogEntry {
	return LogEntry{}
}

func (le LogEntry) Invalid() bool {
	return le.GLSN.Invalid() && le.LLSN.Invalid() && len(le.Data) == 0
}
