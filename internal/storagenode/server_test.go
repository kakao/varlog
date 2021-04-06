package storagenode

//
//import (
//	"context"
//	"github.daumkakao.com/varlog/varlog/internal/storagenode"
//	"testing"
//
//	"github.com/golang/mock/gomock"
//	. "github.com/smartystreets/goconvey/convey"
//	"go.uber.org/zap"
//
//	"github.daumkakao.com/varlog/varlog/pkg/types"
//	"github.daumkakao.com/varlog/varlog/pkg/verrors"
//	"github.daumkakao.com/varlog/varlog/proto/snpb"
//	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
//)
//
//func TestManagementServiceGetMetadata(t *testing.T) {
//	Convey("Given a ManagementService", t, func() {
//		const clusterID = types.ClusterID(1)
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mock := NewMockManagement(ctrl)
//		mock.EXPECT().ClusterID().Return(clusterID).AnyTimes()
//		mock.EXPECT().StorageNodeID().Return(types.StorageNodeID(1)).AnyTimes()
//		service := New(mock, storagenode.newNopTelmetryStub(), zap.NewNop())
//
//		gmReq := &snpb.GetMetadataRequest{ClusterID: clusterID}
//
//		Convey("When the passed clusterID is not the same", func() {
//			Convey("Then the GetMetadata should return an error", func() {
//				gmReq.ClusterID += 1
//				_, err := service.GetMetadata(context.TODO(), gmReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the underlying Server failed to get metadata", func() {
//			mock.EXPECT().GetMetadata(gomock.Any()).Return(nil, verrors.ErrInternal)
//			Convey("Then the GetMetadata should return an error", func() {
//				_, err := service.GetMetadata(context.TODO(), gmReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the underlying Server succeeds to get metadata", func() {
//			mock.EXPECT().GetMetadata(gomock.Any()).Return(&varlogpb.StorageNodeMetadataDescriptor{}, nil)
//			Convey("Then the GetMetadata should return the metadata", func() {
//				_, err := service.GetMetadata(context.TODO(), gmReq)
//				So(err, ShouldBeNil)
//			})
//		})
//	})
//}
//
//func TestManagementServiceAddLogStream(t *testing.T) {
//	Convey("Given a ManagementService", t, func() {
//		const (
//			clusterID     = types.ClusterID(1)
//			storageNodeID = types.StorageNodeID(1)
//		)
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mock := NewMockManagement(ctrl)
//		mock.EXPECT().ClusterID().Return(clusterID).AnyTimes()
//		mock.EXPECT().StorageNodeID().Return(storageNodeID).AnyTimes()
//		service := New(mock, storagenode.newNopTelmetryStub(), zap.NewNop())
//
//		alsReq := &snpb.AddLogStreamRequest{ClusterID: clusterID, StorageNodeID: storageNodeID}
//
//		Convey("When the underlying Server failed to add the LogStream", func() {
//			mock.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return("", verrors.ErrInternal)
//			Convey("Then the AddLogStream should return an error", func() {
//				_, err := service.AddLogStream(context.TODO(), alsReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the passed ClusterID is not the same", func() {
//			Convey("Then the AddLogStream should return an error", func() {
//				alsReq.ClusterID += 1
//				_, err := service.AddLogStream(context.TODO(), alsReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the passed StorageNodeID is not the same", func() {
//			Convey("Then the AddLogStream should return an error", func() {
//				alsReq.StorageNodeID += 1
//				_, err := service.AddLogStream(context.TODO(), alsReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the underlying Server succeeds to add the LogStream", func() {
//			mock.EXPECT().AddLogStream(gomock.Any(), gomock.Any(), gomock.Any()).Return("/tmp", nil)
//			Convey("Then the AddLogStream should return a response message about LogStream", func() {
//				_, err := service.AddLogStream(context.TODO(), alsReq)
//				So(err, ShouldBeNil)
//			})
//		})
//	})
//}
//
//func TestManagementServiceRemoveLogStream(t *testing.T) {
//	Convey("Given a ManagementService", t, func() {
//		const (
//			clusterID     = types.ClusterID(1)
//			storageNodeID = types.StorageNodeID(1)
//		)
//
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mock := NewMockManagement(ctrl)
//		mock.EXPECT().ClusterID().Return(clusterID).AnyTimes()
//		mock.EXPECT().StorageNodeID().Return(storageNodeID).AnyTimes()
//		service := New(mock, storagenode.newNopTelmetryStub(), zap.NewNop())
//
//		rmReq := &snpb.RemoveLogStreamRequest{ClusterID: clusterID, StorageNodeID: storageNodeID}
//
//		Convey("When the underlying Server failed to remove the LogStream", func() {
//			mock.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)
//			Convey("Then the RemoveLogStream should return an error", func() {
//				_, err := service.RemoveLogStream(context.TODO(), rmReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the passed ClusterID is invalid", func() {
//			Convey("Then the RemoveLogStream should return an error", func() {
//				rmReq.ClusterID += 1
//				_, err := service.RemoveLogStream(context.TODO(), rmReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the passed StorageNodeID is invalid", func() {
//			Convey("Then the RemoveLogStream should return an error", func() {
//				rmReq.StorageNodeID += 1
//				_, err := service.RemoveLogStream(context.TODO(), rmReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the underlying Server succeeds to remove the LogStream", func() {
//			mock.EXPECT().RemoveLogStream(gomock.Any(), gomock.Any()).Return(nil)
//			Convey("Then the RemoveLogStream should not return an error", func() {
//				_, err := service.RemoveLogStream(context.TODO(), rmReq)
//				So(err, ShouldBeNil)
//			})
//		})
//	})
//}
//
//func TestManagementServiceSeal(t *testing.T) {
//	Convey("Given a ManagementService", t, func() {
//		const (
//			clusterID     = types.ClusterID(1)
//			storageNodeID = types.StorageNodeID(1)
//		)
//
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mock := NewMockManagement(ctrl)
//		mock.EXPECT().ClusterID().Return(clusterID).AnyTimes()
//		mock.EXPECT().StorageNodeID().Return(storageNodeID).AnyTimes()
//		service := New(mock, storagenode.newNopTelmetryStub(), zap.NewNop())
//
//		sealReq := &snpb.SealRequest{ClusterID: clusterID, StorageNodeID: storageNodeID}
//
//		Convey("When the underlying Server failed to seal the LogStream", func() {
//			mock.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any()).Return(varlogpb.LogStreamStatusRunning, types.GLSN(1), verrors.ErrInternal)
//			Convey("Then the Seal should return an error", func() {
//				_, err := service.Seal(context.TODO(), sealReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the underlying Server succeeds to seal the LogStream", func() {
//			mock.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any()).Return(varlogpb.LogStreamStatusSealed, types.GLSN(1), nil)
//			Convey("Then the Seal should not return an error", func() {
//				_, err := service.Seal(context.TODO(), sealReq)
//				So(err, ShouldBeNil)
//			})
//		})
//
//		Convey("When the passed ClusterID is not the same", func() {
//			Convey("Then the Seal should return an error", func() {
//				sealReq.ClusterID += 1
//				_, err := service.Seal(context.TODO(), sealReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the passed StorageNodeID is not the same", func() {
//			Convey("Then the Seal should return an error", func() {
//				sealReq.StorageNodeID += 1
//				_, err := service.Seal(context.TODO(), sealReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//	})
//}
//
//func TestManagementServiceUnseal(t *testing.T) {
//	Convey("Given that a ManagementService handles Unseal RPC call", t, func() {
//		const (
//			clusterID     = types.ClusterID(1)
//			storageNodeID = types.StorageNodeID(1)
//		)
//
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//
//		mock := NewMockManagement(ctrl)
//		mock.EXPECT().ClusterID().Return(clusterID).AnyTimes()
//		mock.EXPECT().StorageNodeID().Return(storageNodeID).AnyTimes()
//		service := New(mock, storagenode.newNopTelmetryStub(), zap.NewNop())
//
//		unsealReq := &snpb.UnsealRequest{ClusterID: clusterID, StorageNodeID: storageNodeID}
//
//		Convey("When the underlying Server failed to unseal the LogStream", func() {
//			mock.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(verrors.ErrInternal)
//			Convey("Then the Unseal should return an error", func() {
//				_, err := service.Unseal(context.TODO(), unsealReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the underlying Server succeeds to unseal the LogStream", func() {
//			mock.EXPECT().Unseal(gomock.Any(), gomock.Any()).Return(nil)
//			Convey("Then the ManagementService should not return an error", func() {
//				_, err := service.Unseal(context.TODO(), unsealReq)
//				So(err, ShouldBeNil)
//			})
//		})
//
//		Convey("When the passed ClusterID is not the same", func() {
//			Convey("Then the Unseal should return an error", func() {
//				unsealReq.ClusterID += 1
//				_, err := service.Unseal(context.TODO(), unsealReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//
//		Convey("When the passed StorageNodeID is not the same", func() {
//			Convey("Then the Unseal should return an error", func() {
//				unsealReq.StorageNodeID += 1
//				_, err := service.Unseal(context.TODO(), unsealReq)
//				So(err, ShouldNotBeNil)
//			})
//		})
//	})
//}
