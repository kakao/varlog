package storagenode

import (
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"google.golang.org/grpc/codes"
)

func errTrimmed(glsn, lwm types.GLSN) error {
	return varlog.NewErrorf(varlog.ErrTrimmed, codes.OutOfRange, "glsn=%v lwm=%v", glsn, lwm)
}

func errUndecidable(glsn, hwm types.GLSN) error {
	return varlog.NewErrorf(varlog.ErrUndecidable, codes.Unavailable, "glsn=%v hwm=%v", glsn, hwm)
}
