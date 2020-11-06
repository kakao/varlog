package storagenode

import (
	"google.golang.org/grpc/codes"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

func errTrimmed(glsn, lwm types.GLSN) error {
	return verrors.NewErrorf(verrors.ErrTrimmed, codes.OutOfRange, "glsn=%v lwm=%v", glsn, lwm)
}

func errUndecidable(glsn, hwm types.GLSN) error {
	return verrors.NewErrorf(verrors.ErrUndecidable, codes.Unavailable, "glsn=%v hwm=%v", glsn, hwm)
}
