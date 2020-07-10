package testutil

import "time"

func CompareWait(cmp func() bool, timeout time.Duration) bool {
	after := time.After(timeout)
	for {
		select {
		case <-after:
			return false
		default:
			if cmp() {
				return true
			}
		}
	}
}
