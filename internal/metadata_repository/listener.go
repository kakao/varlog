package metadata_repository

import (
	"errors"
	"net"
	"time"

	reuseport "github.com/libp2p/go-reuseport"
	"go.uber.org/zap"
)

// stoppableListener sets TCP keep-alive timeouts on accepted
// connections and waits on stopc message
type stoppableListener struct {
	*net.TCPListener
	stopc  <-chan struct{}
	logger *zap.Logger
}

func newStoppableListener(addr string, stopc <-chan struct{}, logger *zap.Logger) (*stoppableListener, error) {
	logger.Info("Listen", zap.String("addr", addr))
	//ln, err := net.Listen("tcp", addr)
	ln, err := reuseport.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc, logger}, nil
}

func (ln stoppableListener) Accept() (c net.Conn, err error) {
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
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
