package snpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestStorageNodeMetadataDescriptor_ToStorageNodeDescriptor(t *testing.T) {
	tcs := []struct {
		snmd *snpb.StorageNodeMetadataDescriptor
		want *varlogpb.StorageNodeDescriptor
		name string
	}{
		{
			name: "Nil",
			snmd: nil,
			want: nil,
		},
		{
			name: "NonNil",
			snmd: &snpb.StorageNodeMetadataDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.MinStorageNodeID,
					Address:       "node1",
				},
				Storages: []varlogpb.StorageDescriptor{
					{Path: "/path1"},
					{Path: "/path2"},
				},
			},
			want: &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.MinStorageNodeID,
					Address:       "node1",
				},
				Paths: []string{"/path1", "/path2"},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.snmd.ToStorageNodeDescriptor()
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStorageNodeMetadataDescriptor_GetLogStream(t *testing.T) {
	tcs := []struct {
		name        string
		logStreamID types.LogStreamID
		want        snpb.LogStreamReplicaMetadataDescriptor
		wantFound   bool
	}{
		{
			name:        "Found",
			logStreamID: types.LogStreamID(1),
			want: snpb.LogStreamReplicaMetadataDescriptor{
				LogStreamReplica: varlogpb.LogStreamReplica{
					TopicLogStream: varlogpb.TopicLogStream{
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			wantFound: true,
		},
		{
			name:        "NotFound",
			logStreamID: types.LogStreamID(3),
			want:        snpb.LogStreamReplicaMetadataDescriptor{},
			wantFound:   false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			snmd := &snpb.StorageNodeMetadataDescriptor{
				LogStreamReplicas: []snpb.LogStreamReplicaMetadataDescriptor{
					{
						LogStreamReplica: varlogpb.LogStreamReplica{
							TopicLogStream: varlogpb.TopicLogStream{
								LogStreamID: types.LogStreamID(1),
							},
						},
					},
					{
						LogStreamReplica: varlogpb.LogStreamReplica{
							TopicLogStream: varlogpb.TopicLogStream{
								LogStreamID: types.LogStreamID(2),
							},
						},
					},
				},
			}

			got, found := snmd.GetLogStream(tc.logStreamID)
			require.Equal(t, tc.wantFound, found)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLogStreamReplicaMetadataDescriptor_Head(t *testing.T) {
	tcs := []struct {
		lsrmd *snpb.LogStreamReplicaMetadataDescriptor
		name  string
		want  varlogpb.LogEntryMeta
	}{
		{
			name:  "Nil",
			lsrmd: nil,
			want:  varlogpb.LogEntryMeta{},
		},
		{
			name: "NonNil",
			lsrmd: &snpb.LogStreamReplicaMetadataDescriptor{
				LogStreamReplica: varlogpb.LogStreamReplica{
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(2),
					},
				},
				LocalLowWatermark: varlogpb.LogSequenceNumber{
					LLSN: types.LLSN(3),
					GLSN: types.GLSN(4),
				},
			},
			want: varlogpb.LogEntryMeta{
				TopicID:     1,
				LogStreamID: 2,
				LLSN:        3,
				GLSN:        4,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.lsrmd.Head()
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLogStreamReplicaMetadataDescriptor_Tail(t *testing.T) {
	tcs := []struct {
		lsrmd *snpb.LogStreamReplicaMetadataDescriptor
		name  string
		want  varlogpb.LogEntryMeta
	}{
		{
			name:  "Nil",
			lsrmd: nil,
			want:  varlogpb.LogEntryMeta{},
		},
		{
			name: "NonNil",
			lsrmd: &snpb.LogStreamReplicaMetadataDescriptor{
				LogStreamReplica: varlogpb.LogStreamReplica{
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(2),
					},
				},
				LocalHighWatermark: varlogpb.LogSequenceNumber{
					LLSN: types.LLSN(5),
					GLSN: types.GLSN(6),
				},
			},
			want: varlogpb.LogEntryMeta{
				TopicID:     1,
				LogStreamID: 2,
				LLSN:        5,
				GLSN:        6,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.lsrmd.Tail()
			require.Equal(t, tc.want, got)
		})
	}
}
