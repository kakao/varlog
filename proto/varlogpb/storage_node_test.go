package varlogpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/proto/varlogpb"
)

func TestStorageNodeDescriptor_Valid(t *testing.T) {
	tcs := []struct {
		name string
		snd  *varlogpb.StorageNodeDescriptor
		want bool
	}{
		{
			name: "Valid",
			snd: &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "127.0.0.1"},
				Paths:       []string{"/path/to/storage"},
			},
			want: true,
		},
		{
			name: "NilDescriptor",
			snd:  nil,
			want: false,
		},
		{
			name: "EmptyAddress",
			snd: &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: ""},
				Paths:       []string{"/path/to/storage"},
			},
			want: false,
		},
		{
			name: "EmptyPaths",
			snd: &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "127.0.0.1"},
				Paths:       []string{},
			},
			want: false,
		},
		{
			name: "EmptyPathInPaths",
			snd: &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{StorageNodeID: 1, Address: "127.0.0.1"},
				Paths:       []string{""},
			},
			want: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.snd.Valid()
			require.Equal(t, tc.want, got)
		})
	}
}
