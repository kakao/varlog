package varlog

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrUnwrittenLogEntry = errors.New("unwritten log entry")
	ErrWrittenLogEntry   = errors.New("already written log entry")
	ErrTrimmedLogEntry   = errors.New("already trimmed log entry")
	ErrInvalidProjection = errors.New("invalid projection")

	ErrAlreadyExists = status.New(codes.AlreadyExists, "varlogserver: already exists").Err()

	errStringToError = map[string]error{
		ErrorDesc(ErrAlreadyExists): ErrAlreadyExists,
	}
)

type VarlogError struct {
	code codes.Code
	desc string
}

// Code returns grpc/codes.Code.
func (e VarlogError) Code() codes.Code {
	return e.code
}

func (e VarlogError) Error() string {
	return e.desc
}

func Error(err error) error {
	if err == nil {
		return nil
	}
	verr, ok := errStringToError[ErrorDesc(err)]
	if !ok { // not gRPC error
		return err
	}
	ev, ok := status.FromError(verr)
	var desc string
	if ok {
		desc = ev.Message()
	} else {
		desc = verr.Error()
	}
	return VarlogError{code: ev.Code(), desc: desc}
}

func ErrorDesc(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

func toErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	err = Error(err)
	if _, ok := err.(VarlogError); ok {
		return err
	}
	if ev, ok := status.FromError(err); ok {
		code := ev.Code()
		switch code {
		case codes.DeadlineExceeded, codes.Canceled:
			if ctx.Err() != nil {
				err = ctx.Err()
			}
		}
	}
	return err
}

func IsTransientErr(err error) bool {
	if _, ok := err.(VarlogError); ok {
		return false
	}

	if ev, ok := status.FromError(err); ok && ev.Code() != codes.Unavailable {
		return false
	}

	return true
}

func IsAlreadyExistsErr(err error) bool {
	if _, ok := err.(VarlogError); !ok {
		return false
	}

	return err.Error() == Error(ErrAlreadyExists).Error()
}
