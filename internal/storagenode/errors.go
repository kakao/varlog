package storagenode

import (
	"fmt"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

func errTrimmed(glsn, lwm types.GLSN) error {
	return fmt.Errorf("trimmed (glsn=%d lwm=%d): %w", glsn, lwm, verrors.ErrTrimmed)
	// return verrors.NewErrorf(verrors.ErrTrimmed, codes.OutOfRange, "glsn=%v lwm=%v", glsn, lwm)
}

func errUndecidable(glsn, hwm types.GLSN) error {
	return fmt.Errorf("undecidable (glsn=%d hwm=%d): %w", glsn, hwm, verrors.ErrUndecidable)
	// return verrors.NewErrorf(verrors.ErrUndecidable, codes.Unavailable, "glsn=%v hwm=%v", glsn, hwm)
}
