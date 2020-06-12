package test

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/varlog"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	etcdcli "go.etcd.io/etcd/clientv3"
)

func Init() {
	os.RemoveAll("./default.etcd")
}

func startProcess(args ...string) (p *os.Process, err error) {
	for i := 0; i < 5; i++ {
		if args[0], err = exec.LookPath(args[0]); err == nil {
			var procAttr os.ProcAttr
			procAttr.Files = []*os.File{os.Stdin, os.Stdout, os.Stderr}

			log.Printf("start process %v\n", args)
			p, err := os.StartProcess(args[0], args, &procAttr)
			if err == nil {
				return p, nil
			}
		}

		time.Sleep(time.Second)
	}
	return nil, err
}

func connectCheck(host, port string, timeout time.Duration) error {
	st := time.Now()

	for {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
		if conn != nil {
			defer conn.Close()
			log.Printf("connection check OK(dur:%v)", time.Now().Sub(st))
			return nil
		}

		if time.Now().Sub(st) > timeout {
			return fmt.Errorf("conn fail. %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func makeDummyProjection(epoch uint64) *varlogpb.ProjectionDescriptor {
	prj := &varlogpb.ProjectionDescriptor{}
	prj.Epoch = epoch
	prj.MinLsn = 0
	prj.MaxLsn = math.MaxUint64

	return prj
}

func testProposeByClientDirect(t *testing.T) {
	metaRepos := metadata_repository.NewEtcdMetadataRepository()
	if metaRepos == nil {
		t.Fatal()
	}

	metaRepos.Clear()

	dur := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			metaRepos := metadata_repository.NewEtcdMetadataRepository()

			for epoch := uint64(0); epoch < uint64(100); epoch++ {
				projection := makeDummyProjection(epoch + 1)
				err := metaRepos.Propose(epoch, projection)
				if err != nil {
					t.Errorf("propose error: %v", err)
					return
				}

				recv, err := metaRepos.Get(epoch + 1)
				if err != nil {
					t.Errorf("get error: %v", err)
					return
				}

				if recv == nil {
					t.Errorf("get projection[%d] should success", epoch+1)
					return
				}

				if recv.Epoch != epoch+1 {
					t.Errorf("expected projection[%d] actual[%d]", epoch+1, recv.Epoch)
					return
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("dur %v\n", time.Now().Sub(dur))
}

func testProposeUsingProxy(t *testing.T) {
	/* make repository */
	metaRepos := metadata_repository.NewEtcdProxyMetadataRepository()
	if metaRepos == nil {
		t.Fatal()
	}
	metaRepos.Clear()

	/* make server */
	lis, server, err := createServer()
	if err != nil {
		t.Fatal(err)
	}

	addr := lis.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	address := fmt.Sprintf("localhost:%d", tcpAddr.Port)

	service := metadata_repository.NewMetadataRepositoryService(metaRepos)
	service.Register(server)

	go startServer(lis, server)
	defer server.GracefulStop()

	dur := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			metaRepoClient, err := varlog.NewMetadataRepositoryClient(address)
			if err != nil {
				t.Errorf("uninitialied client: %v", err)
			}
			defer metaRepoClient.Close()

			for epoch := uint64(0); epoch < uint64(100); epoch++ {
				projection := makeDummyProjection(epoch + 1)
				err = metaRepoClient.Propose(context.Background(), epoch, projection)
				if err != nil {
					t.Errorf("propose error: %v", err)
					return
				}

				recvProjection, err := metaRepoClient.GetProjection(context.Background(), epoch+1)
				if err != nil {
					t.Errorf("get error: %v", err)
					return
				}

				if recvProjection.Epoch != epoch+1 {
					t.Errorf("expected projection[%d] actual[%d]", epoch+1, recvProjection.Epoch)
					return
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("dur %v\n", time.Now().Sub(dur))
}

func TestEtcdMetadataRepositoryPropose(t *testing.T) {
	etcd := fmt.Sprintf("./etcd/%s/etcd", runtime.GOOS)
	p, err := startProcess(etcd, "--force-new-cluster=true")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := p.Kill()
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = connectCheck("localhost", "2379", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	//testProposeByClientDirect(t)
	testProposeUsingProxy(t)
}

func TestEtcdValue(t *testing.T) {
	etcd := fmt.Sprintf("./etcd/%s/etcd", runtime.GOOS)
	p, err := startProcess(etcd, "--force-new-cluster=true")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := p.Kill()
		if err != nil {
			t.Fatal(err)
		}
	}()

	cli, err := etcdcli.New(etcdcli.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})

	// init data
	val := make([]byte, 64*1024)
	for i := 0; i < 64*1024; i++ {
		val[i] = byte('a')
	}
	v1 := string(val[:256])
	v2 := string(val[:16*1024])
	v3 := string(val[:64*1024])

	// warm up
	for i := 0; i < 1000; i++ {
		cli.Put(context.TODO(), "test", "0")
	}

	st := time.Now()
	for i := 0; i < 1000; i++ {
		cli.Put(context.TODO(), "test", v1)
	}

	t1 := time.Now()
	for i := 0; i < 1000; i++ {
		cli.Put(context.TODO(), "test", v2)
	}

	t2 := time.Now()
	for i := 0; i < 1000; i++ {
		cli.Put(context.TODO(), "test", v3)
	}
	t3 := time.Now()

	t.Logf("%d : %v", len(v1), t1.Sub(st)/1000)
	t.Logf("%d : %v", len(v2), t2.Sub(t1)/1000)
	t.Logf("%d : %v", len(v3), t3.Sub(t2)/1000)
}
