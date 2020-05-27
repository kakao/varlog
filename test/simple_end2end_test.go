package test

import (
	"log"
	"math"
	"net"
	"testing"

	"github.daumkakao.com/solar/solar/internal/metadata_repository"
	"github.daumkakao.com/solar/solar/internal/sequencer"
	"github.daumkakao.com/solar/solar/internal/storage"
	"github.daumkakao.com/solar/solar/pkg/solar"
	"google.golang.org/grpc"

	solarpb "github.daumkakao.com/solar/solar/proto/solar"
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
	projection := &solarpb.ProjectionDescriptor{}
	projection.Epoch = 0
	projection.Sequencer = solarpb.SequencerDescriptor{Address: addr}
	projection.StorageNodes = []solarpb.StorageNodeDescriptor{
		{
			StorageNodeId: addr,
			Address:       addr,
		},
	}
	projection.Replicas = []solarpb.ReplicaDescriptor{
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
	address := lis.Addr().String()
	t.Logf("address: %v", address)
	createSequencer(server)
	createStorageNode(server)
	createMetadataRepository(server, address)
	go startServer(lis, server)
	defer server.GracefulStop()

	const msg = "hello"
	opts := solar.Options{
		MetadataRepositoryAddress: address,
	}
	client, err := solar.Open("foo", opts)
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
