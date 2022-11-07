//go:build rpc_e2e
// +build rpc_e2e

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/rpc"
)

const (
	defaultListenAddr = "0.0.0.0:9997"
)

var (
	listenAddr string
)

func init() {
	flag.StringVar(&listenAddr, "l", defaultListenAddr, "listen address")
	flag.Parse()
}

func initServer() (*rpc.TestServer, error) {
	ts, err := rpc.NewTestServer(grpc.ReadBufferSize(128 * 1024))
	if err != nil {
		return nil, err
	}
	if err := ts.InitListener(listenAddr, false); err != nil {
		return nil, err
	}
	return ts, nil
}

func main() {
	ts, err := initServer()
	if err != nil {
		log.Printf("%+v", err)
		os.Exit(1)
	}

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sigC:
			if err := ts.Close(); err != nil {
				log.Printf("%+v", err)
			}
		}
	}()

	log.Println("running rpc_test_server")
	if err := ts.Run(); err != nil {
		log.Printf("%+v", err)
		os.Exit(1)
	}
}
