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

	"github.com/kakao/varlog/pkg/verrors"

	"golang.org/x/sys/unix"
)

var (
	errNotSupportedNetwork     = errors.New("not supported network")
	errNotLocalAdddress        = errors.New("not local address")
	errNotGlobalUnicastAddress = errors.New("not global unicast address")
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
		return nil, verrors.ErrStopped
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
		},
	}
	return lc.Listen(context.Background(), network, address)
}

// GetListenerAddrs returns ip address of given net.Listener. If the net.Listener is not TCP
// listener, GetListenerAddrs returns errNotSupportedNetwork.
func GetListenerAddrs(addr net.Addr) ([]string, error) {
	tcpAddr, err := getTCPListenerAddr(addr)
	if err != nil {
		return nil, err
	}

	var ips []net.IP
	if tcpAddr.IP.IsUnspecified() {
		ips = getIPs()
	} else if tcpAddr.IP.IsLoopback() {
		ips = append(ips, tcpAddr.IP)
	} else if ip, err := getIP(tcpAddr); err != nil {
		ips = append(ips, ip)
	}

	port := strconv.Itoa(tcpAddr.Port)
	ret := make([]string, len(ips))
	for i, ip := range ips {
		ret[i] = net.JoinHostPort(ip.String(), port)
	}
	return ret, nil
}

func getIP(addr net.Addr) (net.IP, error) {
	ip, _, _ := net.ParseCIDR(addr.String())
	if !ip.IsGlobalUnicast() {
		return nil, errNotGlobalUnicastAddress
	}
	return ip, nil
}

func getIPs() []net.IP {
	var ret []net.IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ret
	}
	for _, addr := range addrs {
		if ip, err := getIP(addr); err == nil {
			ret = append(ret, ip)
		}
	}
	return ret
}

func getTCPListenerAddr(addr net.Addr) (*net.TCPAddr, error) {
	if addr.Network() != "tcp" {
		return nil, errNotSupportedNetwork
	}
	return addr.(*net.TCPAddr), nil
}

func UnicastIPs() ([]net.IP, error) {
	ips, err := IPs()
	if err != nil {
		return nil, err
	}

	ret := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		if ip.IsGlobalUnicast() && !ip.IsLoopback() {
			ret = append(ret, ip)
		}
	}
	return ret, nil
}

func IPs() ([]net.IP, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		if len(addrs) == 0 {
			continue
		}

		for _, addr := range addrs {
			addr := addr.(*net.IPNet)
			ip := addr.IP.To4()
			if ip != nil {
				ips = append(ips, ip)
			}
		}
	}
	return ips, nil
}
