package storagenode

import (
	"context"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/pkg/errors"
)

const (
	DefaultPProfReadHeaderTimeout = 5 * time.Second
	DefaultPProfWriteTimeout      = 11 * time.Second
	DefaultPProfIdleTimeout       = 120 * time.Second
)

type PProfServerConfig struct {
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

func defaultPProfServerConfig() PProfServerConfig {
	return PProfServerConfig{
		ReadHeaderTimeout: DefaultPProfReadHeaderTimeout,
		WriteTimeout:      DefaultPProfWriteTimeout,
		IdleTimeout:       DefaultPProfIdleTimeout,
	}
}

type pprofServer struct {
	s http.Server
}

func newPprofServer(cfg PProfServerConfig) *pprofServer {
	ps := &pprofServer{}
	ps.s.ReadHeaderTimeout = cfg.ReadHeaderTimeout
	ps.s.WriteTimeout = cfg.WriteTimeout
	ps.s.IdleTimeout = cfg.IdleTimeout
	return ps
}

func (ps *pprofServer) run(ls net.Listener) error {
	return errors.WithStack(ps.s.Serve(ls))
}

func (ps *pprofServer) close(ctx context.Context) error {
	return ps.s.Shutdown(ctx)
}
