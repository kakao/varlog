package snpb

import "github.daumkakao.com/varlog/varlog/pkg/types"

func InvalidSyncPosition() SyncPosition {
	return SyncPosition{LLSN: types.InvalidLLSN, GLSN: types.InvalidGLSN}
}

func (sp SyncPosition) Invalid() bool {
	return sp.LLSN.Invalid() || sp.GLSN.Invalid()
}

func (sp SyncPosition) LessThan(other SyncPosition) bool {
	return sp.LLSN < other.LLSN && sp.GLSN < other.GLSN
}

func InvalidSyncRange() SyncRange {
	return SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN}
}

func (sr SyncRange) Invalid() bool {
	return sr.FirstLLSN.Invalid() || sr.LastLLSN.Invalid() || sr.FirstLLSN > sr.LastLLSN
}
