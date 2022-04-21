package varlogadm

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestAdmin_StorageNodes(t *testing.T) {
	const cid = types.ClusterID(1)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmview := NewMockClusterMetadataView(ctrl)
	cmview.EXPECT().ClusterMetadata(gomock.Any()).Return(&varlogpb.MetadataDescriptor{
		StorageNodes: []*varlogpb.StorageNodeDescriptor{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(1),
				},
			},
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(2),
				},
			},
		},
	}, nil).AnyTimes()

	snmgr := NewMockStorageNodeManager(ctrl)
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(1))).Return(nil, errors.New("error")).AnyTimes()
	snmgr.EXPECT().GetMetadata(gomock.Any(), gomock.Eq(types.StorageNodeID(2))).Return(&snpb.StorageNodeMetadataDescriptor{
		ClusterID: cid,
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(2),
				Address:       "sn2",
			},
			Status: varlogpb.StorageNodeStatusRunning,
		},
	}, nil).AnyTimes()

	var cm ClusterManager = &clusterManager{
		cmView: cmview,
		snMgr:  snmgr,
		options: &Options{
			ClusterID: cid,
		},
	}
	snmds, err := cm.StorageNodes(context.Background())
	assert.NoError(t, err)
	assert.Len(t, snmds, 2)

	assert.Contains(t, snmds, types.StorageNodeID(1))
	assert.Equal(t, varlogpb.StorageNodeStatusUnavailable, snmds[types.StorageNodeID(1)].StorageNode.Status)

	assert.Contains(t, snmds, types.StorageNodeID(2))
	assert.Equal(t, varlogpb.StorageNodeStatusRunning, snmds[types.StorageNodeID(2)].StorageNode.Status)
}
