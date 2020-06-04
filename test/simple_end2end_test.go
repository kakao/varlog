package test

import (
	"fmt"
	"log"
	"math"
	"net"
	"testing"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/internal/sequencer"
	"github.daumkakao.com/varlog/varlog/internal/storage"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"google.golang.org/grpc"

	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

func createServer() (net.Listener, *grpc.Server, error) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("could not listen: %v", err)
	}
	server := grpc.NewServer()
	return lis, server, err
}

func startServer(lis net.Listener, server *grpc.Server) {
	if err := server.Serve(lis); err != nil {
		log.Fatalf("could not serve: %v", err)
	}
}

func createMetadataRepository(server *grpc.Server, addr string) {
	metaRepos := metadata_repository.NewInMemoryMetadataRepository()
	projection := &varlogpb.ProjectionDescriptor{}
	projection.Epoch = 0
	projection.Sequencer = varlogpb.SequencerDescriptor{Address: addr}
	projection.StorageNodes = []varlogpb.StorageNodeDescriptor{
		{
			StorageNodeId: addr,
			Address:       addr,
		},
	}
	projection.Replicas = []varlogpb.ReplicaDescriptor{
		{
			MinLsn:         0,
			MaxLsn:         math.MaxUint64,
			StorageNodeIds: []string{addr},
		},
	}

	// FIXME:
	err := metaRepos.Propose(0, projection)
	if err != nil {
		log.Fatalf("propose error: %v", err)
	}
	service := metadata_repository.NewMetadataRepositoryService(metaRepos)
	service.Register(server)
}

func createStorageNode(server *grpc.Server) {
	stg := storage.NewInMemoryStorage(1000)
	service := storage.NewStorageNodeService(stg)
	service.Register(server)
}

func createSequencer(server *grpc.Server) {
	sqr := sequencer.NewSequencer()
	service := sequencer.NewSequencerService(sqr)
	service.Register(server)
}

func TestClient(t *testing.T) {
	lis, server, err := createServer()
	if err != nil {
		t.Errorf("server error: %v", err)
	}
	addr := lis.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	address := fmt.Sprintf("localhost:%d", tcpAddr.Port)
	t.Logf("address: %v", address)
	createSequencer(server)
	createStorageNode(server)
	createMetadataRepository(server, address)
	go startServer(lis, server)
	defer server.GracefulStop()

	const msg = "hello"
	opts := varlog.Options{
		MetadataRepositoryAddress: address,
	}
	client, err := varlog.Open("foo", opts)
	if err != nil {
		t.Errorf("uninitialied client: %v", err)
	}

	pos, err := client.Append([]byte(msg))
	if err != nil {
		t.Errorf("write error: %v", err)
	}
	buf, err := client.Read(pos)
	if err != nil {
		t.Errorf("read error: %v", err)
	}
	if string(buf) != msg {
		t.Errorf("write=%v read=%v", msg, string(buf))
	}
}
