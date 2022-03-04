package storage

import "github.com/kakao/varlog/pkg/types"

type CommitContext struct {
	Version            types.Version
	HighWatermark      types.GLSN
	CommittedGLSNBegin types.GLSN
	CommittedGLSNEnd   types.GLSN
	CommittedLLSNBegin types.LLSN
}

func (cc CommitContext) Empty() bool {
	return cc.CommittedGLSNEnd-cc.CommittedGLSNBegin == 0
}

func (cc CommitContext) Equal(other CommitContext) bool {
	return cc.Version == other.Version &&
		cc.CommittedGLSNBegin == other.CommittedGLSNBegin &&
		cc.CommittedGLSNEnd == other.CommittedGLSNEnd &&
		cc.CommittedLLSNBegin == other.CommittedLLSNBegin
}
