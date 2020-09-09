package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const testAddr = "127.0.0.1:10000"

type testEnv struct {
	addr string
	cli  varlog.MetadataRepositoryClient
	mr   metadata_repository.MetadataRepository
}

func createMetadataRepository(server *grpc.Server) metadata_repository.MetadataRepository {
	metaRepos := metadata_repository.NewInMemoryMetadataRepository()
	service := metadata_repository.NewMetadataRepositoryService(metaRepos)
	service.Register(server)

	return metaRepos
}

func createRaftMetadataRepository(addr string) metadata_repository.MetadataRepository {
	var cluster []string

	cluster = append(cluster, fmt.Sprintf("http://%s", testAddr))
	nodeID := types.NewNodeID(testAddr)

	os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
	os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))

	logger, _ := zap.NewDevelopment()
	options := &metadata_repository.MetadataRepositoryOptions{
		NodeID:            nodeID,
		NumRep:            1,
		PeerList:          *cli.NewStringSlice(cluster...),
		RPCBindAddress:    addr,
		Logger:            logger,
		ReporterClientFac: metadata_repository.NewEmptyReporterClientFactory(),
	}

	metaRepos := metadata_repository.NewRaftMetadataRepository(options)
	metaRepos.Run()

	return metaRepos
}

func CreateEnv(t *testing.T) *testEnv {
	mr := createRaftMetadataRepository(":0")
	addr := mr.(*metadata_repository.RaftMetadataRepository).GetServerAddr()

	cli, err := varlog.NewMetadataRepositoryClient(addr)
	if err != nil {
		t.Fatal(err)
	}

	env := &testEnv{
		addr: addr,
		cli:  cli,
		mr:   mr,
	}

	return env
}

func (env *testEnv) Close() {
	env.cli.Close()
	env.mr.Close()

	nodeID := types.NewNodeID(testAddr)

	os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
	os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))
}

func TestMetadataRepositoryClientSimpleRegister(t *testing.T) {
	var env *testEnv

	Convey("Create Env", t, func(ctx C) {
		env = CreateEnv(t)
	})
	defer env.Close()

	Convey("Register Storage Node", t, func(ctx C) {
		snId := types.StorageNodeID(time.Now().UnixNano())

		s := &varlogpb.StorageDescriptor{
			Path:  "test",
			Used:  0,
			Total: 100,
		}
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snId,
			Address:       "localhost",
		}
		sn.Storages = append(sn.Storages, s)

		err := env.cli.RegisterStorageNode(context.TODO(), sn)
		So(err, ShouldBeNil)

		Convey("Get Storage Node info from Metadata", func(ctx C) {
			meta, err := env.cli.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(meta.GetStorageNode(snId), ShouldNotEqual, nil)
		})

		Convey("Register Exist Storage Node", func(ctx C) {
			err := env.cli.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		})
	})
}
