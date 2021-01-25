package storagenode

import (
	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
)

func errTrimmed(glsn, lwm types.GLSN) error {
	return errors.Wrapf(verrors.ErrTrimmed, "trimmed (glsn = %d, lwm = %d)", glsn, lwm)
}

func errUndecidable(glsn, hwm types.GLSN) error {
	return errors.Wrapf(verrors.ErrUndecidable, "undecidable (glsn = %d, hwm = %d)", glsn, hwm)
}
