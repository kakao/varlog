package pprof

import (
	"context"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/pkg/errors"
)

type Server interface {
	Run(net.Listener) error
	Close(context.Context) error
}

type server struct {
	config
	http.Server
}

var _ Server = (*server)(nil)

func New(opts ...Option) Server {
	cfg := newConfig(opts)
	s := &server{config: cfg}
	return s
}

func (s *server) Run(ls net.Listener) error {
	return errors.WithStack(s.Server.Serve(ls))
}

func (s *server) Close(ctx context.Context) error {
	return s.Server.Shutdown(ctx)
}
