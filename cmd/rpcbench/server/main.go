package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/util/telemetry"
	"github.com/kakao/varlog/proto/rpcbenchpb"
)

type server struct {
	addr string
	lis  net.Listener
	gs   *grpc.Server
	seq  uint64

	tm telemetry.Telemetry
}

var _ rpcbenchpb.RPCBenchServer = (*server)(nil)

func newServer(listenAddr string, telemetryEndpoint string) (*server, error) {
	s := &server{
		addr: listenAddr,
		gs:   grpc.NewServer(),
	}

	exporterType := "otel"
	if len(telemetryEndpoint) == 0 {
		exporterType = "nop"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tm, err := telemetry.New(
		ctx,
		"rpcbench",
		"rpcbench",
		telemetry.WithExporterType(exporterType),
		telemetry.WithEndpoint(telemetryEndpoint),
	)
	if err != nil {
		log.Fatalf("telemetry initialization: %+v", err)
	}
	s.tm = tm

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	s.lis = lis
	rpcbenchpb.RegisterRPCBenchServer(s.gs, s)
	log.Printf("server is initialized: listen_addr=%s, telemetry=type=%s,endpoint=%s", s.addr, exporterType, telemetryEndpoint)
	return s, nil
}

func (s *server) Run() error {
	log.Printf("server is running")
	return errors.WithStack(s.gs.Serve(s.lis))
}

func (s *server) Stop() {
	s.gs.GracefulStop()
	_ = s.tm.Close(context.Background())
	log.Printf("server is stopped")
}

func (s *server) Call(ctx context.Context, req *rpcbenchpb.Request) (*rpcbenchpb.Response, error) {
	seq := atomic.AddUint64(&s.seq, 1) - 1
	return &rpcbenchpb.Response{Seq: seq}, nil
}

const (
	defaultListenAddr        = "0.0.0.0:9997"
	defaultTelemetryEndpoint = "localhost:4317"
)

var (
	listenAddr        string
	telemetryEndpoint string
)

func init() {
	flag.StringVar(&listenAddr, "l", defaultListenAddr, "listen address")
	flag.StringVar(&telemetryEndpoint, "t", defaultTelemetryEndpoint, "telemetry endpoint")
	flag.Parse()

}

func main() {
	svr, err := newServer(listenAddr, telemetryEndpoint)
	if err != nil {
		log.Fatalf("%+v", err)
		os.Exit(1)
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sigC:
			svr.Stop()
		}
	}()

	var grp errgroup.Group
	grp.Go(svr.Run)
	if err := grp.Wait(); err != nil {
		log.Fatalf("%+v", err)
	}
}
