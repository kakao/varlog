package storage

import (
	"context"
	"testing"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"
	"go.uber.org/zap"
)

func TestStorageNode(t *testing.T) {
	const (
		clusterID     = types.ClusterID(1)
		storageNodeID = types.StorageNodeID(1)
		logStreamID   = types.LogStreamID(1)
	)

	logger, _ := zap.NewDevelopment()
	opts := &StorageNodeOptions{
		RPCOptions:               RPCOptions{RPCBindAddress: ":0"},
		LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
		LogStreamReporterOptions: DefaultLogStreamReporterOptions,
		ClusterID:                clusterID,
		StorageNodeID:            storageNodeID,
		Verbose:                  true,
		Logger:                   logger,
	}
	var err error
	sn, err := NewStorageNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	err = sn.Run()
	if err != nil {
		t.Fatal(err)
	}

	mcl, err := varlog.NewManagementClient(sn.serverAddr)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := mcl.GetMetadata(context.TODO(), clusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		t.Error(err)
	}
	t.Log(meta)

	err = mcl.AddLogStream(context.TODO(), clusterID, storageNodeID, logStreamID, "/tmp")
	if err != nil {
		t.Error(err)
	}

	meta, err = mcl.GetMetadata(context.TODO(), clusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		t.Error(err)
	}
	t.Log(meta)

	go func() {
		sn.Close()
	}()
	sn.Wait()
}
