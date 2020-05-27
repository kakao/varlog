package solar

type LogStream struct {
	MinLsn uint64
	MaxLsn uint64
}

/*
func (ls *LogStream) Clone() *LogStream {
	return &LogStream{
		minLsn: ls.minLsn,
		maxLsn: ls.maxLsn,
	}
}
*/
