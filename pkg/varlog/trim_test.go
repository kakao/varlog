package varlog

import (
	"context"
	"strconv"
	"testing"

	gomock "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	_ "github.daumkakao.com/varlog/varlog/vtesting"
)

func TestTrim(t *testing.T) {
	Convey("Given varlog client", t, func() {
		const (
			numStorageNodes  = 10
			minStorageNodeID = 0
		)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		replicasRetriever := NewMockReplicasRetriever(ctrl)
		replicasMap := make(map[types.LogStreamID][]varlogpb.LogStreamReplicaDescriptor, numStorageNodes)
		for i := minStorageNodeID; i < numStorageNodes; i++ {
			replicasMap[types.LogStreamID(i)] = []varlogpb.LogStreamReplicaDescriptor{
				{
					StorageNodeID: types.StorageNodeID(i),
					LogStreamID:   types.LogStreamID(i),
					Address:       "127.0.0.1:" + strconv.Itoa(i),
				},
			}
		}
		replicasRetriever.EXPECT().All().Return(replicasMap).MaxTimes(1)

		createMockLogClientManager := func(expectedTrimResults []error) *logc.MockLogClientManager {
			logCLManager := logc.NewMockLogClientManager(ctrl)
			logCLManager.EXPECT().GetOrConnect(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, storageNodeID types.StorageNodeID, storagNodeAddr string) (logc.LogIOClient, error) {
					logCL := logc.NewMockLogIOClient(ctrl)
					expectedTrimResult := expectedTrimResults[int(storageNodeID)]
					logCL.EXPECT().Trim(gomock.Any(), gomock.Any()).Return(expectedTrimResult)
					return logCL, nil
				},
			).Times(numStorageNodes)
			return logCLManager
		}

		vlg := &varlog{}
		vlg.logger = zap.L()
		vlg.runner = runner.New("varlog-test", zap.L())
		vlg.replicasRetriever = replicasRetriever

		Convey("When all storage nodes fail to trim", func() {
			var errs []error
			for i := 0; i < numStorageNodes; i++ {
				errs = append(errs, verrors.ErrInternal)
			}
			vlg.logCLManager = createMockLogClientManager(errs)

			Convey("Then the trim should fail", func() {
				err := vlg.Trim(context.TODO(), types.GLSN(1), TrimOption{})
				So(err, ShouldNotBeNil)
			})
		})

		Convey("When at least one of all storage node (first) succeeds to trim", func() {
			var errs []error
			errs = append(errs, nil)
			for i := 1; i < numStorageNodes; i++ {
				errs = append(errs, verrors.ErrInternal)
			}
			vlg.logCLManager = createMockLogClientManager(errs)

			Convey("Then the trim should succeed", func() {
				err := vlg.Trim(context.TODO(), types.GLSN(1), TrimOption{})
				So(err, ShouldBeNil)
			})
		})

		Convey("When at least one of all storage node (last) succeeds to trim", func() {
			var errs []error
			for i := 0; i < numStorageNodes-1; i++ {
				errs = append(errs, verrors.ErrInternal)
			}
			errs = append(errs, nil)
			vlg.logCLManager = createMockLogClientManager(errs)

			Convey("Then the trim should succeed", func() {
				err := vlg.Trim(context.TODO(), types.GLSN(1), TrimOption{})
				So(err, ShouldBeNil)
			})
		})

		Convey("When at least one of all storage node (middle) succeeds to trim", func() {
			var errs []error
			for i := 0; i < numStorageNodes; i++ {
				errs = append(errs, verrors.ErrInternal)
			}
			errs[numStorageNodes/2] = nil
			vlg.logCLManager = createMockLogClientManager(errs)

			Convey("Then the trim should succeed", func() {
				err := vlg.Trim(context.TODO(), types.GLSN(1), TrimOption{})
				So(err, ShouldBeNil)
			})
		})
	})
}
