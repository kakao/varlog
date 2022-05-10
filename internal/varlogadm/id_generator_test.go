package varlogadm

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestLogStreamIDGenerator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metadataDesc := &varlogpb.MetadataDescriptor{
		LogStreams: []*varlogpb.LogStreamDescriptor{
			{LogStreamID: 1},
			{LogStreamID: 2},
		},
	}

	cmView := NewMockClusterMetadataView(ctrl)
	cmView.EXPECT().ClusterMetadata(gomock.Any()).Return(metadataDesc, nil)

	gen, err := NewLogStreamIDGenerator(context.Background(), cmView)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, gen.Generate())
}

func TestLogStreamIDGenerator_MetadataError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cmView := NewMockClusterMetadataView(ctrl)
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
