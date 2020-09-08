// Package netutil provides helper functions for network.
//
package netutil

import (
	"context"
	"errors"
	"net"
	"strconv"
	"syscall"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"golang.org/x/sys/unix"
)

var (
	errNotSupportedNetwork = errors.New("not supported network")
	errNotLocalAdddress    = errors.New("not local address")
)

type StoppableListener struct {
	*net.TCPListener
	ctx context.Context
}

func NewStoppableListener(ctx context.Context, addr string) (*StoppableListener, error) {
	ln, err := Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &StoppableListener{ln.(*net.TCPListener), ctx}, nil
}

func (ln StoppableListener) Accept() (c net.Conn, err error) {
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.ctx.Done():
		return nil, varlog.ErrStopped
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}

func Listen(network, address string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
				unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
		},
	}
	return lc.Listen(context.Background(), network, address)
}

// GetListenerAddr returns ip address of given net.Listener. If the net.Listener is not TCP
// listener, GetListenerAddr returns errNotSupportedNetwork.
func GetListenerAddr(lis net.Listener) (string, error) {
	addr, err := getTCPListenerAddr(lis)
	if err != nil {
		return "", err
	}
	return addr.String(), nil
}

// GetListenerLocalAddr is  returns local ip address of given net.Listener. If the net.Listener is
// not TCP listener, GetListenerLocalAddr returns errNotSupportedNetwork. If the net.Listener does
// not bind loopback, GetListenerLocalAddr returns errNotLocalAdddress.
func GetListenerLocalAddr(lis net.Listener) (string, error) {
	addr, err := getTCPListenerAddr(lis)
	if err != nil {
		return "", err
	}
	if addr.IP.IsUnspecified() || addr.IP.IsLoopback() {
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(addr.Port)), nil
	}
	return "", errNotLocalAdddress
}

func getTCPListenerAddr(lis net.Listener) (*net.TCPAddr, error) {
	addr := lis.Addr()
	if addr.Network() != "tcp" {
		return nil, errNotSupportedNetwork
	}
	return addr.(*net.TCPAddr), nil
}
