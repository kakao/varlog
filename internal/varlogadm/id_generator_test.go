package varlogadm

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/internal/varlogadm/mrmanager"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestTopicIDGenerator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := &varlogpb.MetadataDescriptor{
		Topics: []*varlogpb.TopicDescriptor{
			{TopicID: 1},
			{TopicID: 2},
		},
	}

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil)

	gen, err := NewTopicIDGenerator(context.Background(), cmView)
	assert.NoError(t, err)
	assert.Equal(t, types.TopicID(3), gen.Generate())
}

func TestTopicIDGenerator_MetadataError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("fail"))

	// could not fetch cluster metadata
	_, err := NewTopicIDGenerator(context.Background(), cmView)
	assert.Error(t, err)

	// too old metadata
	md := &varlogpb.MetadataDescriptor{
		Topics: []*varlogpb.TopicDescriptor{
			{TopicID: 1},
			{TopicID: 2},
			{TopicID: 3},
		},
	}
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil)
	gen, err := NewTopicIDGenerator(context.Background(), cmView)
	assert.NoError(t, err)
	md.Topics = md.Topics[0:2]
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil)
	err = gen.Refresh(context.Background())
	assert.Error(t, err)
}

func TestLogStreamIDGenerator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := &varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{LogStreamID: 1},
			{LogStreamID: 2},
		},
	}

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil)

	gen, err := NewLogStreamIDGenerator(context.Background(), cmView)
	assert.NoError(t, err)
	assert.Equal(t, types.LogStreamID(3), gen.Generate())
}

func TestLogStreamIDGenerator_MetadataError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := mrmanager.NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(nil, errors.New("fail"))

	// could not fetch cluster metadata
	_, err := NewLogStreamIDGenerator(context.Background(), cmView)
	assert.Error(t, err)

	// too old metadata
	md := &varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{LogStreamID: 1},
			{LogStreamID: 2},
			{LogStreamID: 3},
		},
	}
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil)
	gen, err := NewLogStreamIDGenerator(context.Background(), cmView)
	assert.NoError(t, err)
	md.LogStreams = md.LogStreams[0:2]
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(md, nil)
	err = gen.Refresh(context.Background())
	assert.Error(t, err)
}
