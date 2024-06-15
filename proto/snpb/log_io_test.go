package snpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendRequest(t *testing.T) {
	smallRequest := AppendRequest{
		TopicID:     1,
		LogStreamID: 2,
		Payload:     [][]byte{[]byte("I")},
	}
	smallPayload, err := smallRequest.Marshal()
	require.NoError(t, err)

	mediumRequest := AppendRequest{
		TopicID:     3,
		LogStreamID: 4,
		Payload:     [][]byte{[]byte("XI"), []byte("XII"), []byte("XIII"), []byte("XIV"), []byte("XV")},
	}
	mediumPayload, err := mediumRequest.Marshal()
	require.NoError(t, err)

	largeRequest := AppendRequest{
		TopicID:     5,
		LogStreamID: 6,
		Payload:     [][]byte{[]byte("XXI"), []byte("XXII"), []byte("XXIII"), []byte("XXIV"), []byte("XXV"), []byte("XXVI"), []byte("XXVII"), []byte("XXVIII"), []byte("XXIX"), []byte("XXX")},
	}
	laregePayload, err := largeRequest.Marshal()
	require.NoError(t, err)

	t.Run("EqualLength", func(t *testing.T) {
		tcs := []struct {
			name    string
			req     AppendRequest
			payload []byte
		}{
			{
				name:    "Small",
				req:     smallRequest,
				payload: smallPayload,
			},
			{
				name:    "Medium",
				req:     mediumRequest,
				payload: mediumPayload,
			},
			{
				name:    "Large",
				req:     largeRequest,
				payload: laregePayload,
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				var req AppendRequest

				err := req.Unmarshal(tc.payload)
				require.NoError(t, err)
				require.Equal(t, tc.req, req)
				wantPayloadPtrs := get2DPointers(req.Payload)

				req.ResetReuse()
				err = req.Unmarshal(tc.payload)
				require.NoError(t, err)
				require.Equal(t, tc.req, req)
				gotPayloadPtrs := get2DPointers(req.Payload)
				require.Equal(t, wantPayloadPtrs, gotPayloadPtrs)
			})
		}
	})

	t.Run("GrowingLengthGrowingCapacity", func(t *testing.T) {
		var req AppendRequest

		err := req.Unmarshal(smallPayload)
		require.NoError(t, err)
		require.Equal(t, smallRequest, req)
		smallPayloadPtrs := get2DPointers(req.Payload)
		smallPayloadCap := cap(req.Payload)

		req.ResetReuse()
		err = req.Unmarshal(mediumPayload)
		require.NoError(t, err)
		require.Equal(t, mediumRequest, req)
		mediumPayloadPtrs := get2DPointers(req.Payload)
		mediumPayloadCap := cap(req.Payload)

		require.NotEqual(t, smallPayloadPtrs, mediumPayloadPtrs)
		require.Less(t, smallPayloadCap, mediumPayloadCap)

		req.ResetReuse()
		err = req.Unmarshal(laregePayload)
		require.NoError(t, err)
		require.Equal(t, largeRequest, req)
		largePayloadPtrs := get2DPointers(req.Payload)
		largePayloadCap := cap(req.Payload)

		require.NotEqual(t, mediumPayloadPtrs, largePayloadPtrs)
		require.Less(t, mediumPayloadCap, largePayloadCap)
	})

	t.Run("ShrinkingLengthEqualCapacity", func(t *testing.T) {
		var req AppendRequest

		err := req.Unmarshal(laregePayload)
		require.NoError(t, err)
		require.Equal(t, largeRequest, req)
		largePayloadPtrs := get2DPointers(req.Payload)
		largePayloadCap := cap(req.Payload)

		req.ResetReuse()
		err = req.Unmarshal(mediumPayload)
		require.NoError(t, err)
		require.Equal(t, mediumRequest, req)
		mediumPayloadPtrs := get2DPointers(req.Payload)
		mediumPayloadCap := cap(req.Payload)

		require.NotEqual(t, largePayloadPtrs, mediumPayloadPtrs)
		require.Equal(t, largePayloadCap, mediumPayloadCap)

		req.ResetReuse()
		err = req.Unmarshal(smallPayload)
		require.NoError(t, err)
		require.Equal(t, smallRequest, req)
		smallPayloadPtrs := get2DPointers(req.Payload)
		smallPayloadCap := cap(req.Payload)

		require.NotEqual(t, mediumPayloadPtrs, smallPayloadPtrs)
		require.Equal(t, mediumPayloadCap, smallPayloadCap)
	})
}
