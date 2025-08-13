package verrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gogo/status"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
)

func TestStatusError(t *testing.T) {
	Convey("Given chained errors not registered", t, func() {
		sentinelErr := errors.New("sentinel error")
		registeredSentinelErr := ErrNoEntry
		So(errorRegistry, ShouldContainKey, registeredSentinelErr.Error())

		err1 := fmt.Errorf("outermost: %w", fmt.Errorf("outer: %w", sentinelErr))
		err2 := fmt.Errorf("outermost: %w", fmt.Errorf("outer: %w", sentinelErr))
		err3 := fmt.Errorf("outermost: %w", fmt.Errorf("outer: %w", sentinelErr))
		errs := []error{err1, err2, err3}

		Convey("When the error is converted by uinsg ToStatusError", func() {
			var stErrs []error
			for _, err := range errs {
				stErr := ToStatusErrorWithCode(err, codes.Internal)
				_, ok := status.FromError(stErr)
				So(ok, ShouldBeTrue)
				stErrs = append(stErrs, stErr)
			}

			Convey("Then the errors is reverted by using FromStatusError", func() {
				for _, stErr := range stErrs {
					revErr := FromStatusError(stErr)
					_, ok := status.FromError(revErr)
					So(ok, ShouldBeFalse)

					maybeSentinelError := revErr
					for {
						e := errors.Unwrap(maybeSentinelError)
						if e == nil {
							break
						}
						maybeSentinelError = e
					}

					So(maybeSentinelError.Error(), ShouldEqual, sentinelErr.Error())
					So(errors.Is(maybeSentinelError, sentinelErr), ShouldBeFalse)
				}
			})
		})
	})

	Convey("Given chained errors registered", t, func() {
		sentinelErr := ErrNoEntry
		So(errorRegistry, ShouldContainKey, sentinelErr.Error())

		err1 := fmt.Errorf("outermost: %w", fmt.Errorf("outer: %w", sentinelErr))
		err2 := fmt.Errorf("outermost: %w", fmt.Errorf("outer: %w", sentinelErr))
		err3 := fmt.Errorf("outermost: %w", fmt.Errorf("outer: %w", sentinelErr))
		errs := []error{err1, err2, err3}

		Convey("When the error is converted by uinsg ToStatusError", func() {
			var stErrs []error
			for _, err := range errs {
				stErr := ToStatusErrorWithCode(err, codes.Internal)
				_, ok := status.FromError(stErr)
				So(ok, ShouldBeTrue)
				stErrs = append(stErrs, stErr)
			}

			Convey("Then the errors is reverted by using FromStatusError", func() {
				for _, stErr := range stErrs {
					revErr := FromStatusError(stErr)
					_, ok := status.FromError(revErr)
					So(ok, ShouldBeFalse)

					maybeSentinelError := revErr
					for {
						e := errors.Unwrap(maybeSentinelError)
						if e == nil {
							break
						}
						maybeSentinelError = e
					}

					So(maybeSentinelError.Error(), ShouldEqual, sentinelErr.Error())
					So(errors.Is(maybeSentinelError, sentinelErr), ShouldBeTrue)
				}
			})
		})
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
