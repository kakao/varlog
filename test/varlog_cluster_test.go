package main

import (
	"context"
	"testing"
	"time"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/testutil"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMetadataRepositoryClientSimpleRegister(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewEmptyReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.MRs[0]
		addr := mr.GetServerAddr()

		cli, err := varlog.NewMetadataRepositoryClient(addr)
		So(err, ShouldBeNil)
		defer cli.Close()

		Convey("When register storage node by CLI", func(ctx C) {
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

			err = cli.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)

			Convey("Then getting storage node info from metadata should be success", func(ctx C) {
				meta, err := cli.GetMetadata(context.TODO())
				So(err, ShouldBeNil)
				So(meta.GetStorageNode(snId), ShouldNotEqual, nil)
			})
		})
	})
}

func TestVarlogRegisterStorageNode(t *testing.T) {
	Convey("Given Varlog cluster", t, func(ctx C) {
		opts := VarlogClusterOptions{
			NrMR:              1,
			NrRep:             1,
			ReporterClientFac: metadata_repository.NewReporterClientFactory(),
		}
		env := NewVarlogCluster(opts)
		env.Start()
		defer env.Close()

		So(testutil.CompareWait(func() bool {
			return env.LeaderElected()
		}, time.Second), ShouldBeTrue)

		mr := env.MRs[0]

		Convey("When register SN & LS", func(ctx C) {
			err := env.AddSN()
			So(err, ShouldBeNil)

			err = env.AddLS()
			So(err, ShouldBeNil)

			meta, err := mr.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(meta, ShouldNotBeNil)
			So(len(meta.StorageNodes), ShouldBeGreaterThan, 0)
			So(len(meta.LogStreams), ShouldBeGreaterThan, 0)

			Convey("Then nrReport of MR should be updated", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					return mr.GetReportCount() > 0
				}, time.Second), ShouldBeTrue)
			})
		})
	})
}
