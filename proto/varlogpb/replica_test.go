package varlogpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/proto/varlogpb"
)

func TestEqualReplicas(t *testing.T) {
	tcs := []struct {
		name string
		xs   []varlogpb.LogStreamReplica
		ys   []varlogpb.LogStreamReplica
		want bool
	}{
		{
			name: "EqualReplicas",
			xs: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			ys: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			want: true,
		},
		{
			name: "DifferentLengths",
			xs: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			ys:   []varlogpb.LogStreamReplica{},
			want: false,
		},
		{
			name: "DifferentStorageNodeID",
			xs: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			ys: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 2},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			want: false,
		},
		{
			name: "DifferentLogStreamID",
			xs: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			ys: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 2},
				},
			},
			want: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := varlogpb.EqualReplicas(tc.xs, tc.ys)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestValidReplicas(t *testing.T) {
	tcs := []struct {
		name     string
		replicas []varlogpb.LogStreamReplica
		wantErr  bool
	}{
		{
			name: "ValidReplicas",
			replicas: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 2},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			wantErr: false,
		},
		{
			name:     "NoReplicas",
			replicas: []varlogpb.LogStreamReplica{},
			wantErr:  true,
		},
		{
			name: "LogStreamIDMismatch",
			replicas: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 2},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 2},
				},
			},
			wantErr: true,
		},
		{
			name: "StorageNodeIDDuplicated",
			replicas: []varlogpb.LogStreamReplica{
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
				{
					StorageNode:    varlogpb.StorageNode{StorageNodeID: 1},
					TopicLogStream: varlogpb.TopicLogStream{LogStreamID: 1},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := varlogpb.ValidReplicas(tc.replicas)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
