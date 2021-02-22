package rpc

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/rpc/testpb"
	"github.com/kakao/varlog/pkg/util/netutil"
)

const (
	fakeWorkTime    = 10 * time.Millisecond
	testConnTimeout = 10 * time.Microsecond
	testCallTimeout = fakeWorkTime / 3
)

type TestServer struct {
	ListenAddr string
	Listener   net.Listener
	Server     *grpc.Server
	G          *errgroup.Group
}

func NewTestServer(opts ...grpc.ServerOption) (*TestServer, error) {
	ts := &TestServer{
		Server: grpc.NewServer(opts...),
	}
	return ts, nil
}

func (ts *TestServer) InitListener(listenAddr string, localTest bool) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return errors.WithStack(err)
	}

	addrs, err := netutil.GetListenerAddrs(lis.Addr())
	if err != nil {
		return multierr.Append(err, lis.Close())
	}
	if localTest {
		for _, addr := range addrs {
			if strings.Contains(addr, "127.0.0.1") {
				listenAddr = addr
				break
			}
		}
		if listenAddr == "" {
			return multierr.Append(lis.Close(), errors.New("no listen addr"))
		}
	}
	ts.Listener = lis
	ts.ListenAddr = listenAddr
	return nil
}

func (ts *TestServer) Run() error {
	testpb.RegisterTestServer(ts.Server, ts)
	return errors.WithStack(ts.Server.Serve(ts.Listener))
}

func (ts *TestServer) Close() error {
	ts.Server.GracefulStop()
	return nil
}

func (ts *TestServer) Call(ctx context.Context, req *testpb.TestRequest) (*testpb.TestResponse, error) {
	start := time.Now()
	time.Sleep(fakeWorkTime)
	var sb strings.Builder
	fmt.Fprintf(&sb, "req=%s expected=%s actual=%s",
		req.GetMsg(),
		fakeWorkTime,
		time.Since(start),
	)
	rsp := &testpb.TestResponse{Msg: sb.String()}
	log.Printf("response=%s", rsp.String())
	return rsp, nil
}
