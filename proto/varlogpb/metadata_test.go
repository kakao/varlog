package varlogpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func TestLogStreamStatusMarshalJSON(t *testing.T) {
	tcs := []struct {
		in    LogStreamStatus
		want  string
		isErr bool
	}{
		{in: LogStreamStatusRunning, want: `"running"`, isErr: false},
		{in: LogStreamStatusSealing, want: `"sealing"`, isErr: false},
		{in: LogStreamStatusSealed, want: `"sealed"`, isErr: false},
		{in: LogStreamStatusDeleted, want: `"deleted"`, isErr: false},
		{in: LogStreamStatusUnsealing, want: `"unsealing"`, isErr: false},
		{in: LogStreamStatus(LogStreamStatusUnsealing + 1), isErr: true},
	}

	for _, tc := range tcs {
		t.Run(tc.in.String(), func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if tc.isErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got))
		})
	}
}

func TestLogStreamStatusUnmarshalJSON(t *testing.T) {
	tcs := []struct {
		in    string
		want  LogStreamStatus
		isErr bool
	}{
		{in: `"running"`, want: LogStreamStatusRunning, isErr: false},
		{in: `"sealing"`, want: LogStreamStatusSealing, isErr: false},
		{in: `"sealed"`, want: LogStreamStatusSealed, isErr: false},
		{in: `"deleted"`, want: LogStreamStatusDeleted, isErr: false},
		{in: `"unsealing"`, want: LogStreamStatusUnsealing, isErr: false},
		{in: `"unknown"`, want: LogStreamStatus(0), isErr: true},
		{in: `{malformed}`, want: LogStreamStatus(0), isErr: true},
	}

	for _, tc := range tcs {
		t.Run(tc.in, func(t *testing.T) {
			var got LogStreamStatus
			err := json.Unmarshal([]byte(tc.in), &got)
			if tc.isErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLogStreamStatus(t *testing.T) {
	tcs := []struct {
		in   LogStreamStatus
		f    func(st LogStreamStatus) bool
		want bool
	}{
		{in: LogStreamStatusRunning, f: LogStreamStatus.Deleted, want: false},
		{in: LogStreamStatusRunning, f: LogStreamStatus.Running, want: true},
		{in: LogStreamStatusRunning, f: LogStreamStatus.Sealed, want: false},

		{in: LogStreamStatusSealing, f: LogStreamStatus.Deleted, want: false},
		{in: LogStreamStatusSealing, f: LogStreamStatus.Running, want: false},
		{in: LogStreamStatusSealing, f: LogStreamStatus.Sealed, want: true},

		{in: LogStreamStatusSealed, f: LogStreamStatus.Deleted, want: false},
		{in: LogStreamStatusSealed, f: LogStreamStatus.Running, want: false},
		{in: LogStreamStatusSealed, f: LogStreamStatus.Sealed, want: true},

		{in: LogStreamStatusDeleted, f: LogStreamStatus.Deleted, want: true},
		{in: LogStreamStatusDeleted, f: LogStreamStatus.Running, want: false},
		{in: LogStreamStatusDeleted, f: LogStreamStatus.Sealed, want: false},
	}

	for _, tc := range tcs {
		t.Run(tc.in.String(), func(t *testing.T) {
			got := tc.f(tc.in)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestStorageNodeStatus(t *testing.T) {
	tcs := []struct {
		in   StorageNodeStatus
		f    func(st StorageNodeStatus) bool
		want bool
	}{
		{in: StorageNodeStatusRunning, f: StorageNodeStatus.Running, want: true},
		{in: StorageNodeStatusRunning, f: StorageNodeStatus.Deleted, want: false},
		{in: StorageNodeStatusDeleted, f: StorageNodeStatus.Running, want: false},
		{in: StorageNodeStatusDeleted, f: StorageNodeStatus.Deleted, want: true},
	}

	for _, tc := range tcs {
		t.Run(tc.in.String(), func(t *testing.T) {
			got := tc.f(tc.in)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTopicStatus(t *testing.T) {
	tcs := []struct {
		in   TopicStatus
		f    func(ts TopicStatus) bool
		want bool
	}{
		{in: TopicStatusDeleted, f: TopicStatus.Deleted, want: true},
		{in: TopicStatus(0), f: TopicStatus.Deleted, want: false}, // Assuming 0 is not a deleted status
	}

	for _, tc := range tcs {
		t.Run(tc.in.String(), func(t *testing.T) {
			got := tc.f(tc.in)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLogStreamDescriptor_Validate(t *testing.T) {
	tcs := []struct {
		name    string
		lsd     LogStreamDescriptor
		wantErr bool
	}{
		{
			name: "Valid",
			lsd: LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
				},
			},
			wantErr: false,
		},
		{
			name:    "NoReplicas",
			lsd:     LogStreamDescriptor{Replicas: []*ReplicaDescriptor{}},
			wantErr: true,
		},
		{
			name:    "NilReplica",
			lsd:     LogStreamDescriptor{Replicas: []*ReplicaDescriptor{nil}},
			wantErr: true,
		},
		{
			name: "DuplicateStorageNodes",
			lsd: LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
				},
			},
			wantErr: true,
		},
		{
			name: "InvalidReplica",
			lsd: LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
					{StorageNodeID: types.StorageNodeID(2), StorageNodePath: ""},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.lsd.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLogStreamDescriptor_Valid(t *testing.T) {
	tcs := []struct {
		name string
		lsd  *LogStreamDescriptor
		want bool
	}{
		{
			name: "Valid",
			lsd: &LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
				},
			},
			want: true,
		},
		{
			name: "Nil",
			lsd:  nil,
			want: false,
		},
		{
			name: "NoReplicas",
			lsd: &LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{},
			},
			want: false,
		},
		{
			name: "InvalidReplica",
			lsd: &LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: ""},
				},
			},
			want: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.lsd.Valid())
		})
	}
}

func TestLogStreamDescriptor_IsReplica(t *testing.T) {
	tcs := []struct {
		name string
		lsd  *LogStreamDescriptor
		snid types.StorageNodeID
		want bool
	}{
		{
			name: "is replica",
			lsd: &LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
				},
			},
			snid: types.StorageNodeID(1),
			want: true,
		},
		{
			name: "not a replica",
			lsd: &LogStreamDescriptor{
				Replicas: []*ReplicaDescriptor{
					{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
				},
			},
			snid: types.StorageNodeID(2),
			want: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.lsd.IsReplica(tc.snid))
		})
	}
}

func TestReplicaDescriptor_Validate(t *testing.T) {
	tcs := []struct {
		name    string
		rd      ReplicaDescriptor
		wantErr bool
	}{
		{
			name:    "Valid",
			rd:      ReplicaDescriptor{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
			wantErr: false,
		},
		{
			name:    "InvalidStorageNodeId",
			rd:      ReplicaDescriptor{StorageNodeID: types.StorageNodeID(0), StorageNodePath: "path"},
			wantErr: true,
		},
		{
			name:    "NoPath",
			rd:      ReplicaDescriptor{StorageNodeID: types.StorageNodeID(1), StorageNodePath: ""},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.rd.Validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReplicaDescriptor_Valid(t *testing.T) {
	tcs := []struct {
		name string
		rd   *ReplicaDescriptor
		want bool
	}{
		{
			name: "Valid",
			rd:   &ReplicaDescriptor{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path"},
			want: true,
		},
		{
			name: "Nil",
			rd:   nil,
			want: false,
		},
		{
			name: "NoPath",
			rd:   &ReplicaDescriptor{StorageNodeID: types.StorageNodeID(1), StorageNodePath: ""},
			want: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.rd.valid())
		})
	}
}

func TestMetadataDescriptor_Must(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		want *MetadataDescriptor
	}{
		{
			name: "Valid",
			md:   &MetadataDescriptor{},
			want: &MetadataDescriptor{},
		},
		{
			name: "Nil",
			md:   nil,
			want: nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.md == nil {
				require.Panics(t, func() { tc.md.Must() })
			} else {
				require.Equal(t, tc.want, tc.md.Must())
			}
		})
	}
}

func TestMetadataDescriptor_InsertStorageNode(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		sn      *StorageNodeDescriptor
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name: "ValidStorageNode",
			md:   &MetadataDescriptor{},
			sn: &StorageNodeDescriptor{
				StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
			},
			want: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "NilStorageNode",
			md:      &MetadataDescriptor{},
			sn:      nil,
			want:    &MetadataDescriptor{},
			wantErr: false,
		},
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			sn: &StorageNodeDescriptor{
				StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "DuplicateStorageNode",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					},
				},
			},
			sn: &StorageNodeDescriptor{
				StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
			},
			want: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					},
				},
			},

			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.InsertStorageNode(tc.sn)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_UpdateStorageNode(t *testing.T) {
	tcs := []struct {
		name         string
		initialNodes []*StorageNodeDescriptor
		updateNode   *StorageNodeDescriptor
		want         []*StorageNodeDescriptor
		wantErr      bool
	}{
		{
			name: "UpdateExistingNode",
			initialNodes: []*StorageNodeDescriptor{
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					Paths:       []string{"path1"},
				},
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(2)},
					Paths:       []string{"path2"},
				},
			},
			updateNode: &StorageNodeDescriptor{
				StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
				Paths:       []string{"newpath1"},
			},
			want: []*StorageNodeDescriptor{
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					Paths:       []string{"newpath1"},
				},
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(2)},
					Paths:       []string{"path2"},
				},
			},
			wantErr: false,
		},
		{
			name: "UpdateNonExistingNode",
			initialNodes: []*StorageNodeDescriptor{
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					Paths:       []string{"path1"},
				},
			},
			updateNode: &StorageNodeDescriptor{
				StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(2)},
				Paths:       []string{"newpath2"},
			},
			want: []*StorageNodeDescriptor{
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					Paths:       []string{"path1"},
				},
			},
			wantErr: true,
		},
		{
			name:         "NilMetadataDescriptor",
			initialNodes: nil,
			updateNode: &StorageNodeDescriptor{
				StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
				Paths:       []string{"newpath1"},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "NilNode",
			initialNodes: []*StorageNodeDescriptor{
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					Paths:       []string{"path1"},
				},
			},
			updateNode: nil,
			want: []*StorageNodeDescriptor{
				{
					StorageNode: StorageNode{StorageNodeID: types.StorageNodeID(1)},
					Paths:       []string{"path1"},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			md := &MetadataDescriptor{
				StorageNodes: tc.initialNodes,
			}

			err := md.UpdateStorageNode(tc.updateNode)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, md.StorageNodes)
		})
	}
}

func TestMetadataDescriptor_UpsertStorageNode(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		sn   *StorageNodeDescriptor
		want *MetadataDescriptor
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			sn: &StorageNodeDescriptor{
				StorageNode: StorageNode{
					StorageNodeID: types.StorageNodeID(1),
				},
			},
			want: nil,
		},
		{
			name: "NilStorageNode",
			md:   &MetadataDescriptor{},
			sn:   nil,
			want: &MetadataDescriptor{},
		},
		{
			name: "InsertNew",
			md:   &MetadataDescriptor{},
			sn: &StorageNodeDescriptor{
				StorageNode: StorageNode{
					StorageNodeID: types.StorageNodeID(1),
				},
			},
			want: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
						},
					},
				},
			},
		},
		{
			name: "UpdateExisting",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "old-address",
						},
					},
				},
			},
			sn: &StorageNodeDescriptor{
				StorageNode: StorageNode{
					StorageNodeID: types.StorageNodeID(1),
					Address:       "new-address",
				},
			},
			want: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "new-address",
						},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.UpsertStorageNode(tc.sn)
			require.NoError(t, err)
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_DeleteStorageNode(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		id      types.StorageNodeID
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			id:      types.StorageNodeID(1),
			want:    nil,
			wantErr: false,
		},
		{
			name:    "NotExists",
			md:      &MetadataDescriptor{},
			id:      types.StorageNodeID(1),
			want:    &MetadataDescriptor{},
			wantErr: true,
		},
		{
			name: "DeleteExisting",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
						},
					},
				},
			},
			id: types.StorageNodeID(1),
			want: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{},
			},
			wantErr: false,
		},
		{
			name: "DeleteExistingWithMultipleNodes",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
						},
					},
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(2),
						},
					},
				},
			},
			id: types.StorageNodeID(1),
			want: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(2),
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.DeleteStorageNode(tc.id)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_GetStorageNode(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		id   types.StorageNodeID
		want *StorageNodeDescriptor
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			id:   types.StorageNodeID(1),
			want: nil,
		},
		{
			name: "NotExists",
			md:   &MetadataDescriptor{},
			id:   types.StorageNodeID(1),
			want: nil,
		},
		{
			name: "GetExisting",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "addr1",
						},
					},
				},
			},
			id: types.StorageNodeID(1),
			want: &StorageNodeDescriptor{
				StorageNode: StorageNode{
					StorageNodeID: types.StorageNodeID(1),
					Address:       "addr1",
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.md.GetStorageNode(tc.id)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_HaveStorageNode(t *testing.T) {
	tcs := []struct {
		name      string
		md        *MetadataDescriptor
		id        types.StorageNodeID
		want      *StorageNodeDescriptor
		wantError bool
	}{
		{
			name:      "NilMetadataDescriptor",
			md:        nil,
			id:        types.StorageNodeID(1),
			want:      nil,
			wantError: true,
		},
		{
			name:      "NotExists",
			md:        &MetadataDescriptor{},
			id:        types.StorageNodeID(1),
			want:      nil,
			wantError: true,
		},
		{
			name: "Exists",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "addr1",
						},
					},
				},
			},
			id: types.StorageNodeID(1),
			want: &StorageNodeDescriptor{
				StorageNode: StorageNode{
					StorageNodeID: types.StorageNodeID(1),
					Address:       "addr1",
				},
			},
			wantError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.md.HaveStorageNode(tc.id)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_MustHaveStorageNode(t *testing.T) {
	tcs := []struct {
		name      string
		md        *MetadataDescriptor
		id        types.StorageNodeID
		want      *StorageNodeDescriptor
		wantPanic bool
		wantError bool
	}{
		{
			name:      "NilMetadataDescriptor",
			md:        nil,
			id:        types.StorageNodeID(1),
			want:      nil,
			wantPanic: true,
			wantError: false,
		},
		{
			name:      "NotExists",
			md:        &MetadataDescriptor{},
			id:        types.StorageNodeID(1),
			want:      nil,
			wantPanic: false,
			wantError: true,
		},
		{
			name: "Exists",
			md: &MetadataDescriptor{
				StorageNodes: []*StorageNodeDescriptor{
					{
						StorageNode: StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "addr1",
						},
					},
				},
			},
			id: types.StorageNodeID(1),
			want: &StorageNodeDescriptor{
				StorageNode: StorageNode{
					StorageNodeID: types.StorageNodeID(1),
					Address:       "addr1",
				},
			},
			wantPanic: false,
			wantError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantPanic {
				require.Panics(t, func() {
					_, _ = tc.md.MustHaveStorageNode(tc.id)
				})
				return
			}

			got, err := tc.md.MustHaveStorageNode(tc.id)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_InsertLogStream(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		ls      *LogStreamDescriptor
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:    "NilLogStream",
			md:      &MetadataDescriptor{},
			ls:      nil,
			want:    &MetadataDescriptor{},
			wantErr: false,
		},
		{
			name: "InsertNew",
			md:   &MetadataDescriptor{},
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "AlreadyExists",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.InsertLogStream(tc.ls)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_UpdateLogStream(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		ls      *LogStreamDescriptor
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "NilLogStream",
			md:      &MetadataDescriptor{},
			ls:      nil,
			want:    &MetadataDescriptor{},
			wantErr: true,
		},
		{
			name: "NotExists",
			md:   &MetadataDescriptor{},
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			want:    &MetadataDescriptor{},
			wantErr: true,
		},
		{
			name: "UpdateExisting",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
						Status:      LogStreamStatusRunning,
					},
				},
			},
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
				Status:      LogStreamStatusSealed,
			},
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
						Status:      LogStreamStatusSealed,
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.UpdateLogStream(tc.ls)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_UpsertLogStream(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		ls   *LogStreamDescriptor
		want *MetadataDescriptor
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
			},
			want: nil,
		},
		{
			name: "NilLogStream",
			md:   &MetadataDescriptor{},
			ls:   nil,
			want: &MetadataDescriptor{},
		},
		{
			name: "InsertNew",
			md:   &MetadataDescriptor{},
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
				Status:      LogStreamStatusRunning,
			},
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
						Status:      LogStreamStatusRunning,
					},
				},
			},
		},
		{
			name: "UpdateExisting",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
						Status:      LogStreamStatusRunning,
					},
				},
			},
			ls: &LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(1),
				Status:      LogStreamStatusSealed,
			},
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
						Status:      LogStreamStatusSealed,
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.UpsertLogStream(tc.ls)
			require.NoError(t, err)
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_DeleteLogStream(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		id      types.LogStreamID
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			id:      types.LogStreamID(1),
			want:    nil,
			wantErr: false,
		},
		{
			name:    "NotExists",
			md:      &MetadataDescriptor{},
			id:      types.LogStreamID(1),
			want:    &MetadataDescriptor{},
			wantErr: true,
		},
		{
			name: "DeleteExisting",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			id: types.LogStreamID(1),
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{},
			},
			wantErr: false,
		},
		{
			name: "DeleteExistingWithMultipleStreams",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(1),
					},
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(2),
					},
				},
			},
			id: types.LogStreamID(1),
			want: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						TopicID:     types.TopicID(1),
						LogStreamID: types.LogStreamID(2),
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.DeleteLogStream(tc.id)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_GetLogStream(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		id   types.LogStreamID
		want *LogStreamDescriptor
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			id:   types.LogStreamID(1),
			want: nil,
		},
		{
			name: "ExistingLogStream",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{LogStreamID: types.LogStreamID(1)},
					{LogStreamID: types.LogStreamID(2)},
					{LogStreamID: types.LogStreamID(3)},
				},
			},
			id:   types.LogStreamID(2),
			want: &LogStreamDescriptor{LogStreamID: types.LogStreamID(2)},
		},
		{
			name: "NotExistingLogStream",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{LogStreamID: types.LogStreamID(1)},
					{LogStreamID: types.LogStreamID(2)},
					{LogStreamID: types.LogStreamID(3)},
				},
			},

			id:   types.LogStreamID(4),
			want: nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.md.GetLogStream(tc.id)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_HaveLogStream(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		id      types.LogStreamID
		want    *LogStreamDescriptor
		wantErr bool
	}{
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			id:      types.LogStreamID(1),
			want:    nil,
			wantErr: true,
		},
		{
			name: "ExistingLogStream",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{LogStreamID: types.LogStreamID(1)},
					{LogStreamID: types.LogStreamID(2)},
					{LogStreamID: types.LogStreamID(3)},
				},
			},
			id:      types.LogStreamID(2),
			want:    &LogStreamDescriptor{LogStreamID: types.LogStreamID(2)},
			wantErr: false,
		},
		{
			name: "NotExistingLogStream",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{LogStreamID: types.LogStreamID(1)},
					{LogStreamID: types.LogStreamID(2)},
					{LogStreamID: types.LogStreamID(3)},
				},
			},
			id:      types.LogStreamID(4),
			want:    nil,
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.md.HaveLogStream(tc.id)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_MustHaveLogStream(t *testing.T) {
	tcs := []struct {
		name      string
		md        *MetadataDescriptor
		id        types.LogStreamID
		want      *LogStreamDescriptor
		wantPanic bool
		wantError bool
	}{
		{
			name:      "NilMetadataDescriptor",
			md:        nil,
			id:        types.LogStreamID(1),
			want:      nil,
			wantPanic: true,
			wantError: false,
		},
		{
			name:      "NotExists",
			md:        &MetadataDescriptor{},
			id:        types.LogStreamID(1),
			want:      nil,
			wantPanic: false,
			wantError: true,
		},
		{
			name: "Exists",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			id:        types.LogStreamID(1),
			want:      &LogStreamDescriptor{LogStreamID: types.LogStreamID(1)},
			wantPanic: false,
			wantError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantPanic {
				require.Panics(t, func() {
					_, _ = tc.md.MustHaveLogStream(tc.id)
				})
				return
			}

			got, err := tc.md.MustHaveLogStream(tc.id)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_NotHaveLogStream(t *testing.T) {
	tcs := []struct {
		name      string
		md        *MetadataDescriptor
		id        types.LogStreamID
		wantError bool
	}{
		{
			name:      "NilMetadataDescriptor",
			md:        nil,
			id:        types.LogStreamID(1),
			wantError: true,
		},
		{
			name:      "NotExists",
			md:        &MetadataDescriptor{},
			id:        types.LogStreamID(1),
			wantError: false,
		},
		{
			name: "Exists",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			id:        types.LogStreamID(1),
			wantError: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.NotHaveLogStream(tc.id)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMetadataDescriptor_MustNotHaveLogStream(t *testing.T) {
	tcs := []struct {
		name      string
		md        *MetadataDescriptor
		id        types.LogStreamID
		wantPanic bool
		wantError bool
	}{
		{
			name:      "NilMetadataDescriptor",
			md:        nil,
			id:        types.LogStreamID(1),
			wantPanic: true,
			wantError: false,
		},
		{
			name:      "NotExists",
			md:        &MetadataDescriptor{},
			id:        types.LogStreamID(1),
			wantPanic: false,
			wantError: false,
		},
		{
			name: "Exists",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						LogStreamID: types.LogStreamID(1),
					},
				},
			},
			id:        types.LogStreamID(1),
			wantPanic: false,
			wantError: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantPanic {
				require.Panics(t, func() {
					_ = tc.md.MustNotHaveLogStream(tc.id)
				})
				return
			}

			err := tc.md.MustNotHaveLogStream(tc.id)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMetadataDescriptor_GetReplicasByStorageNodeID(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		id   types.StorageNodeID
		want []*ReplicaDescriptor
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			id:   types.StorageNodeID(1),
			want: nil,
		},
		{
			name: "NoReplicas",
			md:   &MetadataDescriptor{},
			id:   types.StorageNodeID(1),
			want: []*ReplicaDescriptor{},
		},
		{
			name: "ReplicasExist",
			md: &MetadataDescriptor{
				LogStreams: []*LogStreamDescriptor{
					{
						Replicas: []*ReplicaDescriptor{
							{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path1"},
							{StorageNodeID: types.StorageNodeID(2), StorageNodePath: "path2"},
						},
					},
					{
						Replicas: []*ReplicaDescriptor{
							{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path3"},
						},
					},
				},
			},
			id: types.StorageNodeID(1),
			want: []*ReplicaDescriptor{
				{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path1"},
				{StorageNodeID: types.StorageNodeID(1), StorageNodePath: "path3"},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.md.GetReplicasByStorageNodeID(tc.id)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_GetTopic(t *testing.T) {
	tcs := []struct {
		name string
		md   *MetadataDescriptor
		id   types.TopicID
		want *TopicDescriptor
	}{
		{
			name: "NilMetadataDescriptor",
			md:   nil,
			id:   types.TopicID(1),
			want: nil,
		},
		{
			name: "TopicNotExists",
			md:   &MetadataDescriptor{},
			id:   types.TopicID(1),
			want: nil,
		},
		{
			name: "TopicExists",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
					{TopicID: types.TopicID(2)},
				},
			},
			id:   types.TopicID(1),
			want: &TopicDescriptor{TopicID: types.TopicID(1)},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.md.GetTopic(tc.id)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_InsertTopic(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		topic   *TopicDescriptor
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			topic:   &TopicDescriptor{TopicID: types.TopicID(1)},
			want:    nil,
			wantErr: false,
		},
		{
			name:    "NilTopic",
			md:      &MetadataDescriptor{},
			topic:   nil,
			want:    &MetadataDescriptor{},
			wantErr: false,
		},
		{
			name:  "InsertNew",
			md:    &MetadataDescriptor{},
			topic: &TopicDescriptor{TopicID: types.TopicID(1)},
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			wantErr: false,
		},
		{
			name: "AlreadyExists",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topic: &TopicDescriptor{TopicID: types.TopicID(1)},
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.InsertTopic(tc.topic)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_DeleteTopic(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		id      types.TopicID
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			id:      types.TopicID(1),
			want:    nil,
			wantErr: false,
		},
		{
			name:    "NotExists",
			md:      &MetadataDescriptor{},
			id:      types.TopicID(1),
			want:    &MetadataDescriptor{},
			wantErr: true,
		},
		{
			name: "DeleteExisting",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			id: types.TopicID(1),
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{},
			},
			wantErr: false,
		},
		{
			name: "DeleteExistingWithMultipleTopics",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
					{TopicID: types.TopicID(2)},
				},
			},
			id: types.TopicID(1),
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(2)},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.DeleteTopic(tc.id)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_UpdateTopic(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		topic   *TopicDescriptor
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name: "ValidUpdate",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topic: &TopicDescriptor{TopicID: types.TopicID(1), Status: TopicStatusDeleted},
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1), Status: TopicStatusDeleted},
				},
			},
			wantErr: false,
		},
		{
			name: "NonExistingTopic",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topic: &TopicDescriptor{TopicID: types.TopicID(2), Status: TopicStatusDeleted},
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			wantErr: true,
		},
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			topic:   &TopicDescriptor{TopicID: types.TopicID(1)},
			want:    nil,
			wantErr: true,
		},
		{
			name: "NilTopicDescriptor",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topic: nil,
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.UpdateTopic(tc.topic)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_UpsertTopic(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		topic   *TopicDescriptor
		want    *MetadataDescriptor
		wantErr bool
	}{
		{
			name: "InsertNewTopic",
			md:   &MetadataDescriptor{},
			topic: &TopicDescriptor{
				TopicID: types.TopicID(1),
				Status:  TopicStatusRunning,
			},
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{
						TopicID: types.TopicID(1),
						Status:  TopicStatusRunning,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "UpdateExistingTopic",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{
						TopicID: types.TopicID(1),
						Status:  TopicStatusRunning,
					},
				},
			},
			topic: &TopicDescriptor{
				TopicID: types.TopicID(1),
				Status:  TopicStatusDeleted,
			},
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{
						TopicID: types.TopicID(1),
						Status:  TopicStatusDeleted,
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			topic:   &TopicDescriptor{TopicID: types.TopicID(1)},
			want:    nil,
			wantErr: false,
		},
		{
			name: "NilTopicDescriptor",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topic: nil,
			want: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.md.UpsertTopic(tc.topic)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, tc.md)
		})
	}
}

func TestMetadataDescriptor_HaveTopic(t *testing.T) {
	tcs := []struct {
		name    string
		md      *MetadataDescriptor
		topicID types.TopicID
		want    *TopicDescriptor
		wantErr bool
	}{
		{
			name: "TopicExists",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topicID: types.TopicID(1),
			want:    &TopicDescriptor{TopicID: types.TopicID(1)},
			wantErr: false,
		},
		{
			name: "TopicNotExists",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topicID: types.TopicID(2),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "NilMetadataDescriptor",
			md:      nil,
			topicID: types.TopicID(1),
			want:    nil,
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.md.HaveTopic(tc.topicID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestMetadataDescriptor_MustHaveTopic(t *testing.T) {
	tcs := []struct {
		name      string
		md        *MetadataDescriptor
		topicID   types.TopicID
		want      *TopicDescriptor
		wantPanic bool
		wantErr   bool
	}{
		{
			name: "TopicExists",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topicID:   types.TopicID(1),
			want:      &TopicDescriptor{TopicID: types.TopicID(1)},
			wantPanic: false,
			wantErr:   false,
		},
		{
			name: "TopicNotExists",
			md: &MetadataDescriptor{
				Topics: []*TopicDescriptor{
					{TopicID: types.TopicID(1)},
				},
			},
			topicID:   types.TopicID(2),
			want:      nil,
			wantPanic: false,
			wantErr:   true,
		},
		{
			name:      "NilMetadataDescriptor",
			md:        nil,
			topicID:   types.TopicID(1),
			want:      nil,
			wantPanic: true,
			wantErr:   false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantPanic {
				require.Panics(t, func() {
					_, _ = tc.md.MustHaveTopic(tc.topicID)
				})
				return
			}

			got, err := tc.md.MustHaveTopic(tc.topicID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTopicDescriptor_InsertLogStream(t *testing.T) {
	tcs := []struct {
		name    string
		td      *TopicDescriptor
		lsID    types.LogStreamID
		want    *TopicDescriptor
		wantErr bool
	}{
		{
			name: "InsertNewLogStream",
			td:   &TopicDescriptor{},
			lsID: types.LogStreamID(1),
			want: &TopicDescriptor{
				LogStreams: []types.LogStreamID{types.LogStreamID(1)},
			},
			wantErr: false,
		},
		{
			name: "InsertExistingLogStream",
			td: &TopicDescriptor{
				LogStreams: []types.LogStreamID{types.LogStreamID(1)},
			},
			lsID: types.LogStreamID(1),
			want: &TopicDescriptor{
				LogStreams: []types.LogStreamID{types.LogStreamID(1)},
			},
			wantErr: false,
		},
		{
			name:    "NilTopicDescriptor",
			td:      nil,
			lsID:    types.LogStreamID(1),
			want:    nil,
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc.td.InsertLogStream(tc.lsID)
			require.Equal(t, tc.want, tc.td)
		})
	}
}

func TestTopicDescriptor_HasLogStream(t *testing.T) {
	tcs := []struct {
		name string
		td   *TopicDescriptor
		lsID types.LogStreamID
		want bool
	}{
		{
			name: "LogStreamExists",
			td: &TopicDescriptor{
				LogStreams: []types.LogStreamID{types.LogStreamID(1)},
			},
			lsID: types.LogStreamID(1),
			want: true,
		},
		{
			name: "LogStreamNotExists",
			td: &TopicDescriptor{
				LogStreams: []types.LogStreamID{types.LogStreamID(1)},
			},
			lsID: types.LogStreamID(2),
			want: false,
		},
		{
			name: "NilTopicDescriptor",
			td:   nil,
			lsID: types.LogStreamID(1),
			want: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.td.HasLogStream(tc.lsID)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLogSequenceNumber_Invalid(t *testing.T) {
	tcs := []struct {
		name string
		lsn  LogSequenceNumber
		want bool
	}{
		{
			name: "BothValid",
			lsn: LogSequenceNumber{
				LLSN: types.LLSN(1),
				GLSN: types.GLSN(1),
			},
			want: false,
		},
		{
			name: "LLSNInvalid",
			lsn: LogSequenceNumber{
				LLSN: types.InvalidLLSN,
				GLSN: types.GLSN(1),
			},
			want: true,
		},
		{
			name: "GLSNInvalid",
			lsn: LogSequenceNumber{
				LLSN: types.LLSN(1),
				GLSN: types.InvalidGLSN,
			},
			want: true,
		},
		{
			name: "BothInvalid",
			lsn: LogSequenceNumber{
				LLSN: types.InvalidLLSN,
				GLSN: types.InvalidGLSN,
			},
			want: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.lsn.Invalid()
			require.Equal(t, tc.want, got)
		})
	}
}
