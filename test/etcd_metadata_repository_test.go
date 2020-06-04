package test

import (
	"context"
	"fmt"
	"log"
	"math"
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

func startProcess(args ...string) (p *os.Process, err error) {
	if args[0], err = exec.LookPath(args[0]); err == nil {
		var procAttr os.ProcAttr
		procAttr.Files = []*os.File{os.Stdin, os.Stdout, os.Stderr}

		log.Printf("start process %s\n", args[0])
		p, err := os.StartProcess(args[0], args, &procAttr)
		if err == nil {
			return p, nil
		}
	}
	return nil, err
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

func TestEtcdMetadataRepositoryPropose(t *testing.T) {
	etcd := fmt.Sprintf("./etcd/%s/etcd", runtime.GOOS)
	p, err := startProcess(etcd)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Kill()

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

			for epoch := uint64(0); epoch < uint64(100); epoch++ {
				projection := makeDummyProjection(epoch + 1)
				err := metaRepos.Propose(epoch, projection)
				if err != nil {
					t.Fatalf("propose error: %v", err)
				}

				recv, err := metaRepos.Get(epoch + 1)
				if err != nil {
					t.Fatalf("get error: %v", err)
				}

				if recv == nil {
					t.Fatalf("get projection[%d] should success", epoch+1)
				}

				if recv.Epoch != epoch+1 {
					t.Fatalf("expected projection[%d] actual[%d]", epoch+1, recv.Epoch)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("dur %v\n", time.Now().Sub(dur))

}

func TestEtcdProxyMetadataRepositoryPropose(t *testing.T) {
	etcd := fmt.Sprintf("./etcd/%s/etcd", runtime.GOOS)
	p, err := startProcess(etcd)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Kill()

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
	defer server.GracefulStop()

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

			for epoch := uint64(0); epoch < uint64(100); epoch++ {
				projection := makeDummyProjection(epoch + 1)
				err = metaRepoClient.Propose(context.Background(), epoch, projection)
				if err != nil {
					t.Fatalf("propose error: %v", err)
				}

				recvProjection, err := metaRepoClient.Get(context.Background(), epoch+1)
				if err != nil {
					t.Fatalf("get error: %v", err)
				}

				if recvProjection.Epoch != epoch+1 {
					t.Fatalf("expected projection[%d] actual[%d]", epoch+1, recvProjection.Epoch)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("dur %v\n", time.Now().Sub(dur))
}
