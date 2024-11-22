package snpb_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
)

func TestValidateTopicLogStream(t *testing.T) {
	type testMessage interface {
		GetTopicID() types.TopicID
		GetLogStreamID() types.LogStreamID
	}
	tcs := []struct {
		msg   testMessage
		isErr bool
	}{
		{
			msg: &snpb.AppendRequest{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(2),
			},
			isErr: false,
		},
		{
			msg: &snpb.AppendRequest{
				TopicID:     types.TopicID(0),
				LogStreamID: types.LogStreamID(2),
			},
			isErr: true,
		},
		{
			msg: &snpb.AppendRequest{
				TopicID:     types.TopicID(-1),
				LogStreamID: types.LogStreamID(2),
			},
			isErr: true,
		},
		{
			msg: &snpb.AppendRequest{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(0),
			},
			isErr: true,
		},
		{
			msg: &snpb.AppendRequest{
				TopicID:     types.TopicID(1),
				LogStreamID: types.LogStreamID(-1),
			},
			isErr: true,
		},
	}

	for _, tc := range tcs {
		name := "unknown"
		if s, ok := tc.msg.(fmt.Stringer); ok {
			name = s.String()
		}
		t.Run(name, func(t *testing.T) {
			err := snpb.ValidateTopicLogStream(tc.msg)
			if tc.isErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
