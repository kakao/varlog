package storage

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"go.uber.org/zap"
)

const (
	bindAddress = "127.0.0.1:0"
	clusterID   = types.ClusterID(1)
	numSN       = 3
	logStreamID = types.LogStreamID(1)
)

func TestStorageNode(t *testing.T) {
	Convey("StorageNode Integration Test", t, func() {
		var snList []*StorageNode
		for i := 0; i < numSN; i++ {
			logger, _ := zap.NewDevelopment()
			opts := &StorageNodeOptions{
				RPCOptions:               RPCOptions{RPCBindAddress: bindAddress},
				LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
				LogStreamReporterOptions: DefaultLogStreamReporterOptions,
				ClusterID:                clusterID,
				StorageNodeID:            types.StorageNodeID(i),
				Verbose:                  true,
				Logger:                   logger,
			}
			var err error
			sn, err := NewStorageNode(opts)
			if err != nil {
				t.Fatal(err)
			}
			snList = append(snList, sn)
		}

		for _, sn := range snList {
			if err := sn.Run(); err != nil {
				t.Fatal(err)
			}
		}

		var mclList []varlog.ManagementClient
		for _, sn := range snList {
			mcl, err := varlog.NewManagementClient(sn.serverAddr)
			if err != nil {
				t.Fatal(err)
			}
			mclList = append(mclList, mcl)
		}

		for i := range mclList {
			sn := snList[i]
			mcl := mclList[i]

			meta, err := mcl.GetMetadata(context.TODO(), clusterID, snpb.MetadataTypeLogStreams)
			So(err, ShouldBeNil)
			So(meta.GetClusterID(), ShouldEqual, clusterID)
			So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
			So(meta.GetLogStreams(), ShouldHaveLength, 0)

			err = mcl.AddLogStream(context.TODO(), clusterID, sn.storageNodeID, logStreamID, "/tmp")
			So(err, ShouldBeNil)

			meta, err = mcl.GetMetadata(context.TODO(), clusterID, snpb.MetadataTypeLogStreams)
			So(err, ShouldBeNil)
			So(meta.GetClusterID(), ShouldEqual, clusterID)
			So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
			So(meta.GetLogStreams(), ShouldHaveLength, 1)
			So(meta.GetLogStreams()[0].GetLogStreamID(), ShouldEqual, logStreamID)
		}

		for _, sn := range snList {
			sn.Close()
			sn.Wait()
		}
	})
}
