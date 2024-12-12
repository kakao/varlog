package testutil

import (
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/kakao/varlog/internal/vtesting"
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

func GetFunctionName(i interface{}) string {
	a := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	s := strings.Split(a, "/")
	return s[len(s)-1]
}
