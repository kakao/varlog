package storagenode

import (
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

func errTrimmed(glsn, lwm types.GLSN) error {
	return verrors.NewErrorf(verrors.ErrTrimmed, codes.OutOfRange, "glsn=%v lwm=%v", glsn, lwm)
}

func errUndecidable(glsn, hwm types.GLSN) error {
	return verrors.NewErrorf(verrors.ErrUndecidable, codes.Unavailable, "glsn=%v hwm=%v", glsn, hwm)
}
