package metadata_repository

import (
	"context"
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
	ctx    context.Context
	logger *zap.Logger
}

func newStoppableListener(ctx context.Context, addr string, logger *zap.Logger) (*stoppableListener, error) {
	logger.Info("Listen", zap.String("addr", addr))
	//ln, err := net.Listen("tcp", addr)
	ln, err := reuseport.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), ctx, logger}, nil
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
	case <-ln.ctx.Done():
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
