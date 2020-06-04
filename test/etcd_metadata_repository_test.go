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

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
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
	projection := &varlogpb.ProjectionDescriptor{}
	projection.Sequencer = varlogpb.SequencerDescriptor{Address: "addr"}
	projection.StorageNodes = []varlogpb.StorageNodeDescriptor{
		{
			StorageNodeId: "addr",
			Address:       "addr",
		},
	}
	projection.Replicas = []varlogpb.ReplicaDescriptor{
		{
			MinLsn:         0,
			MaxLsn:         math.MaxUint64,
			StorageNodeIds: []string{"addr"},
		},
	}

	projection.Epoch = epoch

	return projection
}

func testProposeByClientDirect(t *testing.T) {
	metaRepos := metadata_repository.NewEtcdMetadataRepository()
	if metaRepos == nil {
		t.Fatal()
	}

	metaRepos.Clear()

	dur := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			metaRepos := metadata_repository.NewEtcdMetadataRepository()

			for epoch := uint64(0); epoch < uint64(50); epoch++ {
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

	address := lis.Addr().String()

	service := metadata_repository.NewMetadataRepositoryService(metaRepos)
	service.Register(server)

	go startServer(lis, server)
	defer func() {
		server.GracefulStop()
		t.Log("server closed\n")
	}()

	dur := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			metaRepoClient, err := varlog.NewMetadataRepositoryClient(address)
			if err != nil {
				t.Errorf("uninitialied client: %v", err)
			}

			metaRepoClient.Close()

			return

			for epoch := uint64(0); epoch < uint64(50); epoch++ {
				projection := makeDummyProjection(epoch + 1)
				err = metaRepoClient.Propose(context.Background(), epoch, projection)
				if err != nil {
					t.Errorf("propose error: %v", err)
					return
				}

				recvProjection, err := metaRepoClient.Get(context.Background(), epoch+1)
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
