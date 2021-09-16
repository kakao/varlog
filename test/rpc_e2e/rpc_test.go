//go:build rpc_e2e
// +build rpc_e2e

package rpc_e2e

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/rpc/testpb"
)

const (
	defaultServerAddr = "127.0.0.1:9997"
	defaultRPCTimeout = 5 * time.Second
	defaultRPCPerConn = 1
)

var (
	serverAddr string
	rpcTimeout time.Duration
	rpcPerConn int
)

func init() {
	flag.StringVar(&serverAddr, "server-addr", defaultServerAddr, "server address")
	flag.DurationVar(&rpcTimeout, "rpc-timeout", defaultRPCTimeout, "rpc timeout")
	flag.IntVar(&rpcPerConn, "rpc-per-conn", defaultRPCPerConn, "number of rpc per connection")
}

func TestRPC(t *testing.T) {
	defer goleak.VerifyNone(t)

	if rpcPerConn < defaultRPCPerConn {
		rpcPerConn = defaultRPCPerConn
	}

	connCtx, connCancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer connCancel()
	rpcConn, err := rpc.NewConn(connCtx, serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcConn.Close()

	for i := 0; i < rpcPerConn; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		defer cancel()

		now := time.Now()
		req := &testpb.TestRequest{Msg: now.String()}
		log.Printf("Request: %s", req.String())
		tc := testpb.NewTestClient(rpcConn.Conn)
		rsp, err := tc.Call(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("Response: %s, tick=%s", rsp.String(), time.Since(now).String())
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}
