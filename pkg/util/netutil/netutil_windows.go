// +build windows

package netutil

import (
    "syscall"
)

func ControlRawNetworkConnection(network, address string, c syscall.RawConn) error {
	var ret error
	err := c.Control(func(fd uintptr) {
		setSockOpt := func(opt int) error {
			return syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, opt, 1)
		}
		//if err := setSockOpt(syscall.SO_REUSEPORT); err != nil {	// Windows not support
		//	ret = err
		//}
		if err := setSockOpt(syscall.SO_REUSEADDR); err != nil {
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
