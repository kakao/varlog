package snwatcher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestStorageNodeWatcher_InvalidConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eventHandler := NewMockEventHandler(ctrl)
	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	snmgr := snmanager.NewMockStorageNodeManager(ctrl)

	// no event handler
	_, err := New(WithClusterMetadataView(cmview), WithStorageNodeManager(snmgr))
	assert.Error(t, err)

	// no cmview
	_, err = New(WithStorageNodeWatcherHandler(eventHandler), WithStorageNodeManager(snmgr))
	assert.Error(t, err)

	// no snmgr
	_, err = New(WithStorageNodeWatcherHandler(eventHandler), WithClusterMetadataView(cmview))
	assert.Error(t, err)

	// bad heartbeat timeout
	_, err = New(WithStorageNodeWatcherHandler(eventHandler),
		WithClusterMetadataView(cmview),
		WithStorageNodeManager(snmgr),
		WithHeartbeatTimeout(0),
	)
	assert.Error(t, err)

	// bad tick
	_, err = New(WithStorageNodeWatcherHandler(eventHandler),
		WithClusterMetadataView(cmview),
		WithStorageNodeManager(snmgr),
		WithTick(0),
	)
	assert.Error(t, err)

	// bad report interval
	_, err = New(WithStorageNodeWatcherHandler(eventHandler),
		WithClusterMetadataView(cmview),
		WithStorageNodeManager(snmgr),
		WithReportInterval(0),
	)
	assert.Error(t, err)

	// no logger
	_, err = New(WithStorageNodeWatcherHandler(eventHandler),
		WithClusterMetadataView(cmview),
		WithStorageNodeManager(snmgr),
		WithLogger(nil),
	)
	assert.Error(t, err)
}

func TestStorageNodeWatcher_BadClusterMetadataView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const tick = 10 * time.Millisecond
	const interval = 3

	eventHandler := NewMockEventHandler(ctrl)
	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	snmgr := snmanager.NewMockStorageNodeManager(ctrl)

	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("error")).AnyTimes()

	sw, err := New(WithStorageNodeWatcherHandler(eventHandler),
		WithClusterMetadataView(cmview),
		WithStorageNodeManager(snmgr),
		WithTick(tick),
		WithHeartbeatTimeout(interval),
		WithReportInterval(interval),
	)
	assert.NoError(t, err)
	assert.NoError(t, sw.Start())
	time.Sleep(2 * interval * tick)
	assert.NoError(t, sw.Stop())
}

func TestStorageNodeWatcher(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const tick = 10 * time.Millisecond
	const interval = 3

	eventHandler := NewMockEventHandler(ctrl)
	cmview := mrmanager.NewMockClusterMetadataView(ctrl)
	snmgr := snmanager.NewMockStorageNodeManager(ctrl)

	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(
		&varlogpb.MetadataDescriptor{
			StorageNodes: []*varlogpb.StorageNodeDescriptor{
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: 1,
						Address:       "127.0.0.1:10001",
					},
				},
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: 2,
						Address:       "127.0.0.1:10002",
					},
				},
			},
		}, nil,
	).AnyTimes()

	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(1))).Return(
		&snpb.StorageNodeMetadataDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: 1,
			},
		}, nil,
	).AnyTimes()
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(2))).Return(
		nil, errors.New("error"),
	).AnyTimes()

	numHeartbeatHandlerCalled, numReportHandlerCalled := int64(0), int64(0)
	eventHandler.EXPECT().HandleReport(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, snmd *snpb.StorageNodeMetadataDescriptor) {
			assert.Equal(t, types.StorageNodeID(1), snmd.StorageNode.StorageNodeID)
			atomic.AddInt64(&numReportHandlerCalled, 1)
		},
	).MinTimes(1)
	eventHandler.EXPECT().HandleHeartbeatTimeout(gomock.Any(), gomock.Eq(types.StorageNodeID(2))).DoAndReturn(
		func(context.Context, types.StorageNodeID) {
			atomic.AddInt64(&numHeartbeatHandlerCalled, 1)
		},
	).MinTimes(1)

	snw, err := New(WithStorageNodeWatcherHandler(eventHandler),
		WithClusterMetadataView(cmview),
		WithStorageNodeManager(snmgr),
		WithTick(tick),
		WithHeartbeatTimeout(interval),
		WithReportInterval(interval),
	)
	assert.NoError(t, err)
	assert.NoError(t, snw.Start())
	assert.Eventually(t, func() bool {
		return atomic.LoadInt64(&numHeartbeatHandlerCalled) > 0 &&
			atomic.LoadInt64(&numReportHandlerCalled) > 0
	}, tick*interval*10, tick)
	assert.NoError(t, snw.Stop())
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
