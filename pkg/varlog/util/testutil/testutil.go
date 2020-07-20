package testutil

import (
	"fmt"
	"net"
	"time"
)

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
			time.Sleep(time.Millisecond)
		}
	}
}

func GetLocalAddress(lis net.Listener) string {
	addr := lis.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	address := fmt.Sprintf("localhost:%d", tcpAddr.Port)
	return address
}
