package varlog

import (
	"context"
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"google.golang.org/grpc/codes"
)

// TODO (jun, pharrell): ErrAlreadyExists, ErrExist, ErrVMSStorageNode...
// - Use basic error? (ErrAlreadyExists, ErrExist, ...)
// - Use specific error? (ErrLogStreamAlreadyExists, ...)
var (
	ErrNoEntry        = errors.New("storage: no entry")
	ErrCorruptStorage = errors.New("storage: corrupt")
)

var (
	ErrTrimmed          = errors.New("logstream: trimmed")
	ErrUndecidable      = errors.New("logstream: undecidable")
	ErrCorruptLogStream = errors.New("logstream: corrupt")
	ErrSealed           = errors.New("logstream: sealed")
	ErrUnordered        = errors.New("logstream: unordered scanner")
)

var (
	ErrLogStreamAlreadyExists   = errors.New("logstream already exists")
	ErrStorageNodeNotExist      = errors.New("storagenode does not exist")
	ErrStorageNodeAlreadyExists = errors.New("storagenode already exists")
)

var (
	ErrInvalid       = errors.New("invalid argument")
	ErrExist         = errors.New("already exists")
	ErrIgnore        = errors.New("ignore")
	ErrInprogress    = errors.New("inprogress")
	ErrNeedRetry     = errors.New("need retry")
	ErrStopped       = errors.New("stopped")
	ErrNotMember     = errors.New("not member")
	ErrNotAccessible = errors.New("not accessible")

	ErrInvalidArgument = status.New(codes.InvalidArgument, "invalid argument").Err()
	ErrAlreadyExists   = status.New(codes.AlreadyExists, "varlogserver: already exists").Err()
	ErrNotExist        = status.New(codes.NotFound, "not exist").Err()

	ErrInternal = status.New(codes.Internal, "internal error").Err()
)

var ErrorRegistry map[string]error = make(map[string]error)

func init() {
	initErrorRegistry(
		// storage
		ErrNoEntry, ErrCorruptStorage,

		// logstream
		ErrTrimmed, ErrUndecidable, ErrCorruptLogStream, ErrSealed, ErrUnordered,

		ErrInvalidArgument, ErrAlreadyExists, ErrNotExist,

		ErrLogStreamAlreadyExists, ErrStorageNodeNotExist, ErrStorageNodeAlreadyExists,
	)
}

func initErrorRegistry(errs ...error) {
	for _, err := range errs {
		desc := ErrorDesc(err)
		if _, ok := ErrorRegistry[desc]; ok {
			panic(fmt.Sprintf("desc in ErrorRegistry should be unique: desc=%v, err=%v", desc, err))
		}
		ErrorRegistry[desc] = err
	}
}

// Error is wrapper of any errors containing grpc/codes.Code, it can be converted to grpc
// statusError easily. Usually, sentinel error can be wrapped by ValogError with grpc/codes.Code
// and extra descriptions.
type Error struct {
	code  codes.Code
	desc  string
	cause error
}

// NewSimpleErrorf returns an error having a cause and desc made by format and args. It infers a
// code by the given cause.
func NewSimpleErrorf(cause error, format string, args ...interface{}) error {
	if ve, ok := cause.(*Error); ok {
		return NewErrorf(cause, ve.code, format, args...)
	}

	switch cause {
	case nil:
		return nil
	case context.Canceled:
		return NewErrorf(cause, codes.Canceled, format, args...)
	case context.DeadlineExceeded:
		return NewErrorf(cause, codes.DeadlineExceeded, format, args...)
	}

	if IsTransientErr(cause) {
		return NewErrorf(cause, codes.Unavailable, format, args...)
	}
	return NewErrorf(cause, codes.Unknown, format, args...)
}

// NewError returns an error having a cause, a code, and a desc.
func NewError(cause error, code codes.Code, desc string) error {
	return NewErrorf(cause, code, desc)
}

// NewErrorf returns an error having a cause, a code, and a desc made by format and args.
func NewErrorf(cause error, code codes.Code, format string, args ...interface{}) error {
	if ve, ok := cause.(*Error); ok && ve.code != code {
		panic(fmt.Errorf("unexpected code: %v != %v", ve.code, code))
	}
	return &Error{
		code:  code,
		desc:  fmt.Sprintf(format, args...),
		cause: cause,
	}
}

// Code returns grpc/codes.Code.
func (e Error) Code() codes.Code {
	return e.code
}

// Error returns error string.
func (e Error) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("desc=%v code=%v)", e.desc, e.code)
	}
	return fmt.Sprintf("%v: desc=%v code=%v", e.cause.Error(), e.desc, e.code)
}

// Unwrap returns cause.
func (e Error) Unwrap() error {
	return e.cause
}

// Is checks equality agains target.
func (e Error) Is(target error) bool {
	if target == nil {
		return e == target
	}
	if te, ok := target.(*Error); ok && e.desc == te.desc && e.code == te.code {
		return true
	}
	return e.cause != nil && errors.Is(e.cause, target)
}

// Status returns grpc/status.Status. It stores cause into grpc/status.Status.Details.
func (e Error) Status() (*status.Status, error) {
	st := status.New(e.code, e.desc)
	var err error = e.cause

	var details []proto.Message
	for err != nil {
		detail := &vpb.ErrorDetail{}
		if ve, ok := err.(*Error); ok {
			detail.Desc = ve.desc
			detail.IsVarlogError = true
		} else {
			detail.Desc = err.Error()
		}
		details = append(details, detail)
		err = errors.Unwrap(err)
	}
	return st.WithDetails(details...)
}

// ToStatusError converts an errors into grpc/status.statusError. It usually is used before sending
// responses via gRPC.
// If the err is either context.DeadlineExceeded or context.Canceled, it returns
// grpc/status.statusError having a code of either codes.DeadlineExceeded or codes.Canceled. If
// the err is type of Error, it is converted by Error.Status. Otherwise, it is converted by
// grpc/status.FromError.
func ToStatusError(err error) error {
	if err == nil {
		return nil
	}
	return ToStatus(err).Err()
}

// ToStatus converts err to grpc/status.Status. If err is context error, code of status is
// code.DeadlineExceeded or code.Canceled. If err is non-nil error, it returns proper
// grpc/status.Status.
func ToStatus(err error) *status.Status {
	switch err {
	case nil:
		return nil
	case context.DeadlineExceeded:
		return status.New(codes.DeadlineExceeded, err.Error())
	case context.Canceled:
		return status.New(codes.Canceled, err.Error())
	}
	if ve, ok := err.(*Error); ok {
		if s, e := ve.Status(); e == nil {
			return s
		}
	}
	st, _ := status.FromError(err)
	return st
}

// FromStatusError converts an grpc/status.statusError into an error. There are below cases:
// - If maybeStatusErr is nil, it returns nil.
// - If maybeStatusErr is not type of grpc/status.statusError, it returns maybeStatusErr again.
// - In case of that maybeStatusErr is type of grpc/status.statusError:
//   - its code is either grpc/codes.DeadlineExceeded or grpc/codes.Canceled, and given ctx has an
//     error, it returns the error of ctx.
//   - ErrorRegistry has the error string of maybeStatusErr, it returns registered error.
//   - Otherwise, it assumes that maybeStatusErr is type of Error. If length of details is not zero,
//     it tries to convert details into wrapped errors, and set it as cause.
func FromStatusError(ctx context.Context, maybeStatusErr error) error {
	if maybeStatusErr == nil {
		return nil
	}
	if status, ok := status.FromError(maybeStatusErr); ok {
		code := status.Code()
		if code == codes.DeadlineExceeded || code == codes.Canceled {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// NB: ctx.Err() is nil, but status.code is either DeadlineExceeded or
			// Canceled.
			// If server's context is either canceled or timed out, but client's context
			// is neighter not canceld nor timed out, given ctx can be nil and
			// maybeStatusErr's code can be either codes.DeadlineExceeded or
			// codes.Canceled.
			switch code {
			case codes.DeadlineExceeded:
				return context.DeadlineExceeded
			case codes.Canceled:
				return context.Canceled
			}
		}
		// remove protobuf-related fields
		return fromStatus(status)
	}
	return maybeStatusErr
}

func fromStatus(status *status.Status) error {
	if status.Code() == codes.OK {
		return nil
	}
	code := status.Code()
	details := status.Details()
	if err, ok := ErrorRegistry[status.Message()]; ok {
		return err
	}
	var cause error
	for i := len(details) - 1; i >= 0; i-- {
		detail, ok := details[i].(*vpb.ErrorDetail)
		if !ok {
			continue
		}
		if err, ok := ErrorRegistry[detail.Desc]; ok {
			cause = err
			continue
		}
		// regardless of vpb.ErrorDetail.IsVarlogError, create varlog.Error
		cause = NewErrorf(cause, code, detail.Desc)
	}
	return NewErrorf(cause, code, status.Message())
}

// ErrorDesc returns desc if the err is type of Error, otherwise, result of error.Error() method.
func ErrorDesc(err error) string {
	if ve, ok := err.(*Error); ok {
		return ve.desc
	}
	return err.Error()
}

// ToErr converts statusError whose code is either codes.DeadlineExceeded or codes.Canceled to
// one of the context error (e.g., context.DeadlineExceeded or context.Canceled).
//
// If error is context.DeadlineExceeded (errors.Is), it returns context.DeadlineExceeded.
// If error is context.Canceled (errors.Is), it returns context.Canceled.
// If statusError.Code is codes.DeadlineExceeded, it returns context.DeadlineExceeded.
// If statusError.Code is codes.Canceled, it returns context.Canceled.
//
// TODO (jun): change function name: ToErr -> ToContextErr
// TODO (jun): If given context has no error, what should ToErr return?
func ToErr(ctx context.Context, err error) error {
	return FromStatusError(ctx, err)
}

// IsTransientErr checks if err is temporarily error.
func IsTransientErr(err error) bool {
	if err == nil {
		return false
	}
	// TODO (jun): add more errors
	if ve, ok := err.(Error); ok && ve.code == codes.Unavailable {
		return true
	}
	if ve, ok := err.(*Error); ok && ve.code == codes.Unavailable {
		return true
	}
	if se, ok := err.(interface{ GRPCStatus() *status.Status }); ok && se.GRPCStatus().Code() == codes.Unavailable {
		return true
	}
	return errors.Is(err, ErrUndecidable)
}
