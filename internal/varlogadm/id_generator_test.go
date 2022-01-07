package varlogadm

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestLogStreamIDGenerator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		storageNodeID   = types.StorageNodeID(1)
		storageNodeAddr = "sn1"
	)

	metadataDesc := &varlogpb.MetadataDescriptor{
		AppliedIndex: 1,
		StorageNodes: []*varlogpb.StorageNodeDescriptor{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: storageNodeID,
					Address:       storageNodeAddr,
				},
			},
		},
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(2),
			},
		},
	}

	cmView := NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metadataDesc, nil)

	snMgr := NewMockStorageNodeManager(ctrl)

	lsIDGen, err := NewSequentialLogStreamIDGenerator(context.Background(), cmView, snMgr)
	require.NoError(t, err)

	require.Equal(t, types.LogStreamID(3), lsIDGen.Generate())
}
