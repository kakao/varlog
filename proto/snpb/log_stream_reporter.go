package snpb

import (
	"github.com/kakao/varlog/pkg/types"
)

func (u *LogStreamUncommitReport) UncommittedLLSNEnd() types.LLSN {
	if u == nil {
		return types.InvalidLLSN
	}

	return u.UncommittedLLSNOffset + types.LLSN(u.UncommittedLLSNLength)
}

func (r *LogStreamUncommitReport) Seal(end types.LLSN) types.LLSN {
	if r == nil {
		return types.InvalidLLSN
	}

	if end < r.UncommittedLLSNOffset {
		return types.InvalidLLSN
	}

	if end > r.UncommittedLLSNEnd() {
		return types.InvalidLLSN
	}

	r.UncommittedLLSNLength = uint64(end - r.UncommittedLLSNOffset)

	return r.UncommittedLLSNEnd()
}
