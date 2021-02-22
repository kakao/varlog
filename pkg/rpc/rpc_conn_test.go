package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/pkg/rpc/testpb"
)

const (
	testListenAddr = "127.0.0.1:0"
)

func setup(t *testing.T, waitRun chan struct{}) *TestServer {
	ts, err := NewTestServer()
	if err != nil {
		t.Fatal(err)
	}

	if err := ts.InitListener(testListenAddr, true); err != nil {
		t.Fatal(err)
	}

	g, _ := errgroup.WithContext(context.TODO())
	ts.G = g
	g.Go(func() error {
		if waitRun != nil {
			<-waitRun
		}
		return ts.Run()
	})
	return ts
}

func cleanup(t *testing.T, ts *TestServer) {
	if err := ts.Close(); err != nil {
		t.Logf("%+v", err)
		t.Fatal(err)
	}
	check(t, ts)
	ts.G.Wait()
	check(t, ts)
}

func check(t *testing.T, ts *TestServer) {
	rpcConn, err := NewConn(context.TODO(), ts.ListenAddr)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := rpcConn.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	rsp, err := testpb.NewTestClient(rpcConn.Conn).Call(context.TODO(), &testpb.TestRequest{Msg: "check"})
	if err == nil || rsp != nil {
		t.Error("should be error")
	}
}

func TestNewBlockingConn(t *testing.T) {
	defer goleak.VerifyNone(t)

	ts := setup(t, nil)
	defer cleanup(t, ts)

	tick := time.Now()
	rpcConn, err := NewBlockingConn(context.TODO(), ts.ListenAddr)
	t.Logf("NewBlockingConn: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	defer rpcConn.Close()

	tc := testpb.NewTestClient(rpcConn.Conn)
	tick = time.Now()
	rsp, err := tc.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rsp.String())
}

func TestNewBlockingConnWithConnTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	ts := setup(t, nil)
	defer cleanup(t, ts)

	tick := time.Now()
	ctx, cancel := context.WithTimeout(context.TODO(), testConnTimeout)
	defer cancel()
	rpcConn, err := NewBlockingConn(ctx, ts.ListenAddr)
	t.Logf("NewBlockingConn: %s\n", time.Since(tick))
	if err == nil || rpcConn != nil {
		t.Fatal("should be error")
	}
}

func TestNewBlockingConnWithCallTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

	ts := setup(t, nil)
	defer cleanup(t, ts)

	tick := time.Now()
	rpcConn, err := NewBlockingConn(context.TODO(), ts.ListenAddr)
	t.Logf("NewBlockingConn: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	defer rpcConn.Close()

	tc := testpb.NewTestClient(rpcConn.Conn)
	ctx, cancel := context.WithTimeout(context.TODO(), testCallTimeout)
	defer cancel()
	tick = time.Now()
	rsp, err := tc.Call(ctx, &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err == nil {
		t.Fatal("should be error")
	}
	t.Log(rsp.String())
}

func TestNewConn(t *testing.T) {
	defer goleak.VerifyNone(t)

	ts := setup(t, nil)
	defer cleanup(t, ts)

	tick := time.Now()
	rpcConn, err := NewConn(context.TODO(), ts.ListenAddr)
	t.Logf("NewConn: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	defer rpcConn.Close()

	tc := testpb.NewTestClient(rpcConn.Conn)
	tick = time.Now()
	rsp, err := tc.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rsp.String())
}

func TestNewConnDeferredListen(t *testing.T) {
	defer goleak.VerifyNone(t)

	// try to connect not listening server
	tick := time.Now()
	rpcConn, err := NewConn(context.TODO(), "127.0.0.1:80")
	t.Logf("NewConn: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	defer rpcConn.Close()

	tc := testpb.NewTestClient(rpcConn.Conn)
	tick = time.Now()
	rsp, err := tc.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err == nil || rsp != nil {
		t.Fatal(err)
	}
	t.Log(rsp.String())
}

func TestNewConnDeferredServe(t *testing.T) {
	defer goleak.VerifyNone(t)

	waitc := make(chan struct{})
	ts := setup(t, waitc)
	defer cleanup(t, ts)

	// try to connect before calling grpc/Server.Serve
	go func() {
		time.Sleep(1 * time.Second)
		close(waitc)
	}()
	tick := time.Now()
	rpcConn, err := NewConn(context.TODO(), ts.ListenAddr)
	t.Logf("NewConn: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	defer rpcConn.Close()

	tc := testpb.NewTestClient(rpcConn.Conn)
	tick = time.Now()
	rsp, err := tc.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rsp.String())
}

func TestSharedRPCConn(t *testing.T) {
	defer goleak.VerifyNone(t)

	ts := setup(t, nil)
	defer cleanup(t, ts)

	tick := time.Now()
	rpcConn, err := NewConn(context.TODO(), ts.ListenAddr)
	t.Logf("NewConn: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}

	tc1 := testpb.NewTestClient(rpcConn.Conn)
	tick = time.Now()
	rsp1, err := tc1.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rsp1.String())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			tc := testpb.NewTestClient(rpcConn.Conn)
			_, err := tc.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
			if err != nil {
				break
			}
		}
	}()

	if err := rpcConn.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	tc2 := testpb.NewTestClient(rpcConn.Conn)
	tick = time.Now()
	rsp2, err := tc2.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
	t.Logf("Call: %s\n", time.Since(tick))
	if err == nil || rsp2 != nil {
		t.Fatal("should be error")
	}
	t.Log(rsp2.String())
}

func TestManyConns(t *testing.T) {
	const n = 100
	defer goleak.VerifyNone(t)

	ts := setup(t, nil)
	defer cleanup(t, ts)

	for i := 0; i < n; i++ {
		func() {
			tick := time.Now()
			rpcConn, err := NewConn(context.TODO(), ts.ListenAddr)
			t.Logf("NewConn: %s\n", time.Since(tick))
			if err != nil {
				t.Fatal(err)
			}
			defer rpcConn.Close()

			tc := testpb.NewTestClient(rpcConn.Conn)
			tick = time.Now()
			rsp, err := tc.Call(context.TODO(), &testpb.TestRequest{Msg: "foo"})
			t.Logf("Call: %s\n", time.Since(tick))
			if err != nil {
				t.Fatal(err)
			}
			t.Log(rsp.String())
		}()
	}
}
