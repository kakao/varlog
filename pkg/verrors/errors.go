package verrors

import (
	"context"
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/proto/varlogpb"
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
	ErrNotEmpty      = errors.New("not empty")

	ErrInvalidArgument = errors.New("status - invalid argument")
	ErrAlreadyExists   = errors.New("status - varlogserver: already exists")
	ErrNotExist        = errors.New("status - not exist")
	ErrInternal        = errors.New("status - internal error")
)

type transientError struct {
	err error
}

func (e *transientError) Error() string { return e.err.Error() }

func (e *transientError) Unwrap() error { return e.err }

func (e *transientError) Is(target error) bool {
	if target == nil {
		return e == target
	}
	if _, ok := target.(*transientError); ok {
		return true
	}
	return e.err != nil && errors.Is(e.err, target)
}

var errorRegistry map[string]error = make(map[string]error)

func init() {
	initErrorRegistry(
		// storage
		ErrNoEntry, ErrCorruptStorage,

		// logstream
		ErrTrimmed, ErrUndecidable, ErrCorruptLogStream, ErrSealed, ErrUnordered,

		ErrInvalidArgument, ErrAlreadyExists, ErrNotExist,

		ErrLogStreamAlreadyExists, ErrStorageNodeNotExist, ErrStorageNodeAlreadyExists,

		// metadata repository
		ErrNotMember,
	)
}

func initErrorRegistry(errs ...error) {
	for _, err := range errs {
		if _, ok := status.FromError(err); ok {
			panic(fmt.Sprintf("bad error: %+v", err))
		}
		key := errorRegistryKey(err)
		if _, ok := errorRegistry[key]; ok {
			panic(fmt.Sprintf("not unique in errorRegistry: key=%s, err=%+v", key, err))
		}
		errorRegistry[key] = err
	}
}

// ToStatusError converts an errors into grpc/status.statusError. It usually is used before sending
// responses via gRPC.
// If the err is either context.DeadlineExceeded or context.Canceled, it returns
// grpc/status.statusError having a code of either codes.DeadlineExceeded or codes.Canceled.
func ToStatusError(err error) error {
	if errors.Is(err, context.Canceled) {
		return ToStatusErrorWithCode(err, codes.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ToStatusErrorWithCode(err, codes.DeadlineExceeded)
	}
	return ToStatusErrorWithCode(err, codes.Unknown)
}

func ToStatusErrorWithCode(err error, code codes.Code) error {
	if err == nil || code == codes.OK {
		return nil
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	st := status.New(code, err.Error())
	var details []proto.Message
	for err != nil {
		detail := &varlogpb.ErrorDetail{
			ErrorString: err.Error(),
		}
		details = append(details, detail)
		err = errors.Unwrap(err)
	}
	st, err = st.WithDetails(details...)
	if err != nil {
		// NOTE (jun): Losing details may be dangerous since the details can have a sentinel
		// error that should be handled specially.
		panic(err)
	}
	return st.Err()
}

// FromStatusError converts an grpc/status.statusError into an error. There are below cases:
// - If maybeStatusErr is nil, it returns nil.
// - If maybeStatusErr is not type of grpc/status.statusError, it returns maybeStatusErr again.
// - In case of that maybeStatusErr is type of grpc/status.statusError:
//   - try to parse Details field, and return an error if it is parsed
//   - if the error is not parsed, and its code is either grpc/codes.DeadlineExceeded or
//     grpc/codes.Canceled, FromStatusError returns context error
//   - if the code is code.Unavailable, then wrap the error by transientError and return it
func FromStatusError(maybeStatusErr error) (err error) {
	if maybeStatusErr == nil {
		return nil
	}

	st, ok := status.FromError(maybeStatusErr)
	if !ok {
		return maybeStatusErr
	}

	if st.Code() == codes.OK {
		return nil
	}

	err = parseDetails(st)
	if err == nil { // no details
		switch st.Code() {
		case codes.Canceled:
			return context.Canceled
		case codes.DeadlineExceeded:
			return context.DeadlineExceeded
		}

		err, ok = registeredError(st.Message())
		if !ok {
			err = st.Err()
		}
	}

	if st.Code() == codes.Unavailable {
		err = wrapTransientError(err)
	}
	return err
}

func parseDetails(st *status.Status) (err error) {
	details := st.Details()
	for i := len(details) - 1; i >= 0; i-- {
		detail, ok := details[i].(*varlogpb.ErrorDetail)
		if !ok {
			continue
		}
		errDesc := detail.GetErrorString()
		if err == nil {
			if regErr, ok := registeredError(errDesc); ok {
				err = regErr
			} else {
				err = errors.New(errDesc)
			}
		} else {
			err = fmt.Errorf("%s: %w", errDesc, err)
		}
	}
	return err
}

func wrapTransientError(err error) error {
	return &transientError{err: err}
}

func registeredError(errstr string) (err error, ok bool) {
	err, ok = errorRegistry[errstr]
	return err, ok
}

// errorRegistryKey returns desc if the err is type of Error, otherwise, result of error.Error() method.
func errorRegistryKey(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

func ToErr(ctx context.Context, err error) error {
	return FromStatusError(err)
}

// IsTransient checks if err is temporarily error.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		return true
	}
	return errors.Is(err, &transientError{}) || errors.Is(err, ErrUndecidable)
}
