package testutil

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.daumkakao.com/varlog/varlog/vtesting"
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

func CompareWaitError(cmp func() (bool, error), timeout time.Duration) error {
	after := time.NewTimer(timeout)
	defer after.Stop()

	for {
		select {
		case <-after.C:
			return errors.New("timeout")
		default:
			ok, err := cmp()
			if err != nil {
				return err
			}

			if ok {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func CompareWaitErrorN(factor int64, cmp func() (bool, error)) error {
	if factor < 1 {
		factor = 1
	}

	return CompareWaitError(cmp, vtesting.TimeoutUnitTimesFactor(factor))
}

func GC() {
	var ms runtime.MemStats
	var gc runtime.MemStats

	runtime.ReadMemStats(&ms)
	runtime.GC()
	runtime.ReadMemStats(&gc)

	fmt.Printf("\nGC Stat:: %f -> %f mb, Sys: %f mb\n",
		float32(ms.Alloc)/float32(1024*1024),
		float32(gc.Alloc)/float32(1024*1024),
		float32(gc.Sys)/float32(1024*1024))
}

func GetFunctionName(i interface{}) string {
	a := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	s := strings.Split(a, "/")
	return s[len(s)-1]
}
