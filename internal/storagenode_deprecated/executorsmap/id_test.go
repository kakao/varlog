package executorsmap

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func TestLogStreamTopicID(t *testing.T) {
	const n = 10
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		expectedLogStreamID := types.LogStreamID(rng.Int31())
		expectedTopicID := types.TopicID(rng.Int31())

		actualLogStreamID, actualTopicID := packLogStreamTopicID(expectedLogStreamID, expectedTopicID).unpack()
		require.Equal(t, expectedLogStreamID, actualLogStreamID)
		require.Equal(t, expectedTopicID, actualTopicID)
	}
}
