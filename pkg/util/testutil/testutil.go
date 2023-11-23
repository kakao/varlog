package testutil

import (
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/vtesting"
)

func CompareWait(cmp func() bool, timeout time.Duration) bool {
	after := time.NewTimer(timeout)
	defer after.Stop()

	for {
		select {
		case <-after.C:
			return false
		default:
			if cmp() {
				return true
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func CompareWaitN(factor int64, cmp func() bool) bool {
	if factor < 1 {
		factor = 1
	}

	return CompareWait(cmp, vtesting.TimeoutUnitTimesFactor(factor))
}

func CompareWait100(cmp func() bool) bool {
	return CompareWaitN(100, cmp)
}

func CompareWait10(cmp func() bool) bool {
	return CompareWaitN(10, cmp)
}

func CompareWait1(cmp func() bool) bool {
	return CompareWaitN(1, cmp)
}

func CompareWaitErrorWithRetryInterval(cmp func() (bool, error), timeout time.Duration, retryInterval time.Duration) error {
	after := time.NewTimer(timeout)
	defer after.Stop()

	numTries := 0
	for {
		select {
		case <-after.C:
			return errors.Errorf("compare wait timeout (%s,tries=%d)", timeout.String(), numTries)
		default:
			numTries++
			ok, err := cmp()
			if err != nil {
				return err
			}

			if ok {
				return nil
			}
			time.Sleep(retryInterval)
		}
	}
}

func CompareWaitError(cmp func() (bool, error), timeout time.Duration) error {
	return CompareWaitErrorWithRetryInterval(cmp, timeout, time.Millisecond)
}

func CompareWaitErrorWithRetryIntervalN(factor int64, retryInterval time.Duration, cmp func() (bool, error)) error {
	if factor < 1 {
		factor = 1
	}

	return CompareWaitErrorWithRetryInterval(cmp, vtesting.TimeoutUnitTimesFactor(factor), retryInterval)
}

func CompareWaitErrorN(factor int64, cmp func() (bool, error)) error {
	if factor < 1 {
		factor = 1
	}

	return CompareWaitError(cmp, vtesting.TimeoutUnitTimesFactor(factor))
}

func GetFunctionName(i interface{}) string {
	a := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	s := strings.Split(a, "/")
	return s[len(s)-1]
}
