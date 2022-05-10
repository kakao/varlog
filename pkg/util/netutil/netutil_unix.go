//go:build linux || freebsd || darwin

package netutil

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func ControlRawNetworkConnection(network, address string, c syscall.RawConn) error {
	var ret error
	err := c.Control(func(fd uintptr) {
		setSockOpt := func(opt int) error {
			return unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, opt, 1)
		}
		if err := setSockOpt(unix.SO_REUSEPORT); err != nil {
			ret = err
		}
		if err := setSockOpt(unix.SO_REUSEADDR); err != nil {
			ret = err
		}
	})
	if ret != nil {
		return ret
	}
	if err != nil {
		return err
	}
	return nil
}
