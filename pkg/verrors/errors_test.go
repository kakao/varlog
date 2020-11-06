package verrors

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVarlogErrorIs(t *testing.T) {
	err1 := NewError(ErrNoEntry, codes.Unknown, "desc1")
	err2 := NewError(ErrInternal, codes.Unknown, "desc1")
	if !err1.(*Error).Is(err2) {
		t.Error("unexpected Is result")
	}
	if !errors.Is(err1, err2) {
		t.Error("unexpected Is result")
	}

	if !errors.Is(err1, ErrNoEntry) {
		t.Error("unexpected Is result")
	}

	err3 := NewError(err1, codes.Unknown, "outer-most")
	if !errors.Is(err3, ErrNoEntry) {
		t.Error("unexpected Is result")
	}
	if !errors.Is(err3, err1) {
		t.Error("unexpected Is result")
	}
}

func TestToStatus(t *testing.T) {
	if ToStatus(nil) != nil {
		t.Error("should be nil")
	}

	if st := ToStatus(context.Canceled); st.Code() != codes.Canceled {
		t.Errorf("actual = %v expected = %v", st.Code(), codes.Canceled)
	}

	if st := ToStatus(context.DeadlineExceeded); st.Code() != codes.DeadlineExceeded {
		t.Errorf("actual = %v expected = %v", st.Code(), codes.Canceled)
	}

	if st := ToStatus(NewError(ErrNeedRetry, codes.Unavailable, ErrNeedRetry.Error())); st.Code() != codes.Unavailable {
		t.Errorf("actual = %v expected = %v", st.Code(), codes.Unavailable)
	}
}

func TestFromStatusError(t *testing.T) {
	if FromStatusError(context.TODO(), nil) != nil {
		t.Error("should be nil")
	}

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	<-ctx.Done()
	if err := FromStatusError(ctx, status.New(codes.Canceled, "canceled").Err()); err != context.Canceled {
		t.Errorf("actual = %v expected =%v", err, context.Canceled)
	}

	ctx, cancel = context.WithTimeout(context.TODO(), time.Duration(0))
	cancel()
	<-ctx.Done()
	if err := FromStatusError(ctx, status.New(codes.DeadlineExceeded, "timeout").Err()); err != context.DeadlineExceeded {
		t.Errorf("actual = %v expected =%v", err, context.DeadlineExceeded)
	}

	varlogErr := NewErrorf(ErrTrimmed, codes.Unknown, "logstream: trimmed (glsn = 10)")
	statusErr := ToStatusError(varlogErr)
	decodedErr := FromStatusError(context.TODO(), statusErr)
	if !errors.Is(decodedErr, ErrTrimmed) {
		t.Errorf("unexpected Is result, %v %v", varlogErr, decodedErr)
	}

	statusErr = ToStatusError(ErrTrimmed)
	decodedErr = FromStatusError(context.TODO(), statusErr)
	if !errors.Is(decodedErr, ErrTrimmed) {
		t.Errorf("unexpected Is result, %v %v", varlogErr, decodedErr)
	}

	statusErr = ToStatusError(errors.New(ErrTrimmed.Error()))
	decodedErr = FromStatusError(context.TODO(), statusErr)
	if !errors.Is(decodedErr, ErrTrimmed) {
		t.Errorf("unexpected Is result, %v %v", varlogErr, decodedErr)
	}
}
