package pprof

import (
	"context"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/pkg/errors"
)

type Server struct {
	config
	httpServer http.Server
}

func New(opts ...Option) *Server {
	cfg := newConfig(opts)
	s := &Server{config: cfg}
	return s
}

func (s *Server) Run(ls net.Listener) error {
	return errors.WithStack(s.httpServer.Serve(ls))
}

func (s *Server) Close(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
