package snpb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func getPointers[S ~[]E, E any](elems S) []uintptr {
	ptrs := make([]uintptr, len(elems))
	for i := range elems {
		p := uintptr(unsafe.Pointer(&elems[i]))
		ptrs[i] = p
	}
	return ptrs
}

func get2DPointers[S ~[][]E, E any](elems S) [][]uintptr {
	ptrs := make([][]uintptr, len(elems))
	for i := range elems {
		ptrs[i] = getPointers(elems[i])
	}
	return ptrs
}

func TestReplicateRequest(t *testing.T) {
	smallRequest := ReplicateRequest{
		LLSN: []types.LLSN{1},
		Data: [][]byte{[]byte("I")},
	}
	smallPayload, err := smallRequest.Marshal()
	require.NoError(t, err)

	mediumRequest := ReplicateRequest{
		LLSN: []types.LLSN{11, 12, 13, 14, 15},
		Data: [][]byte{[]byte("XI"), []byte("XII"), []byte("XIII"), []byte("XIV"), []byte("XV")},
	}
	mediumPayload, err := mediumRequest.Marshal()
	require.NoError(t, err)

	largeRequest := ReplicateRequest{
		LLSN: []types.LLSN{21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
		Data: [][]byte{[]byte("XXI"), []byte("XXII"), []byte("XXIII"), []byte("XXIV"), []byte("XXV"), []byte("XXVI"), []byte("XXVII"), []byte("XXVIII"), []byte("XXIX"), []byte("XXX")},
	}
	laregePayload, err := largeRequest.Marshal()
	require.NoError(t, err)

	t.Run("EqualLength", func(t *testing.T) {
		tcs := []struct {
			name    string
			req     ReplicateRequest
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
				var req ReplicateRequest

				err := req.Unmarshal(tc.payload)
				require.NoError(t, err)
				require.Equal(t, tc.req, req)
				wantLLSNPtrs := getPointers(req.LLSN)
				wantDataPtrs := get2DPointers(req.Data)

				req.ResetReuse()
				err = req.Unmarshal(tc.payload)
				require.NoError(t, err)
				require.Equal(t, tc.req, req)
				gotLLSNPtrs := getPointers(req.LLSN)
				gotDataPtrs := get2DPointers(req.Data)

				require.Equal(t, wantLLSNPtrs, gotLLSNPtrs)
				require.Equal(t, wantDataPtrs, gotDataPtrs)
			})
		}
	})

	t.Run("GrowingLengthGrowingCapacity", func(t *testing.T) {
		var req ReplicateRequest

		err := req.Unmarshal(smallPayload)
		require.NoError(t, err)
		require.Equal(t, smallRequest, req)
		smallLLSNPtrs := getPointers(req.LLSN)
		smallDataPtrs := get2DPointers(req.Data)
		smallLLSNCap := cap(req.LLSN)
		smallDataCap := cap(req.Data)

		req.ResetReuse()
		err = req.Unmarshal(mediumPayload)
		require.NoError(t, err)
		require.Equal(t, mediumRequest, req)
		mediumLLSNPtrs := getPointers(req.LLSN)
		mediumDataPtrs := get2DPointers(req.Data)
		mediumLLSNCap := cap(req.LLSN)
		mediumDataCap := cap(req.Data)

		require.NotEqual(t, smallLLSNPtrs, mediumLLSNPtrs)
		require.NotEqual(t, smallDataPtrs, mediumDataPtrs)
		require.Less(t, smallLLSNCap, mediumLLSNCap)
		require.Less(t, smallDataCap, mediumDataCap)

		req.ResetReuse()
		err = req.Unmarshal(laregePayload)
		require.NoError(t, err)
		require.Equal(t, largeRequest, req)
		largeLLSNPtrs := getPointers(req.LLSN)
		largeDataPtrs := get2DPointers(req.Data)
		largeLLSNCap := cap(req.LLSN)
		largeDataCap := cap(req.Data)

		require.NotEqual(t, mediumLLSNPtrs, largeLLSNPtrs)
		require.NotEqual(t, mediumDataPtrs, largeDataPtrs)
		require.Less(t, mediumLLSNCap, largeLLSNCap)
		require.Less(t, mediumDataCap, largeDataCap)
	})

	t.Run("ShrinkingLengthEqualCapacity", func(t *testing.T) {
		var req ReplicateRequest

		err := req.Unmarshal(laregePayload)
		require.NoError(t, err)
		require.Equal(t, largeRequest, req)
		largeLLSNPtrs := getPointers(req.LLSN)
		largeDataPtrs := get2DPointers(req.Data)
		largeLLSNCap := cap(req.LLSN)
		largeDataCap := cap(req.Data)

		req.ResetReuse()
		err = req.Unmarshal(mediumPayload)
		require.NoError(t, err)
		require.Equal(t, mediumRequest, req)
		mediumLLSNPtrs := getPointers(req.LLSN)
		mediumDataPtrs := get2DPointers(req.Data)
		mediumLLSNCap := cap(req.LLSN)
		mediumDataCap := cap(req.Data)

		require.NotEqual(t, largeLLSNPtrs, mediumLLSNPtrs)
		require.NotEqual(t, largeDataPtrs, mediumDataPtrs)
		require.Equal(t, largeLLSNCap, mediumLLSNCap)
		require.Equal(t, largeDataCap, mediumDataCap)

		req.ResetReuse()
		err = req.Unmarshal(smallPayload)
		require.NoError(t, err)
		require.Equal(t, smallRequest, req)
		smallLLSNPtrs := getPointers(req.LLSN)
		smallDataPtrs := get2DPointers(req.Data)
		smallLLSNCap := cap(req.LLSN)
		smallDataCap := cap(req.Data)

		require.NotEqual(t, mediumLLSNPtrs, smallLLSNPtrs)
		require.NotEqual(t, mediumDataPtrs, smallDataPtrs)
		require.Equal(t, mediumLLSNCap, smallLLSNCap)
		require.Equal(t, mediumDataCap, smallDataCap)
	})
}

func TestSyncPositionInvalid(t *testing.T) {
	tcs := []struct {
		input    SyncPosition
		expected bool
	}{
		{
			input:    SyncPosition{LLSN: 1, GLSN: 0},
			expected: true,
		},
		{
			input:    SyncPosition{LLSN: 0, GLSN: 1},
			expected: true,
		},
		{
			input:    SyncPosition{LLSN: 0, GLSN: 0},
			expected: true,
		},
		{
			input:    SyncPosition{LLSN: 1, GLSN: 1},
			expected: false,
		},
	}

	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.input.String(), func(t *testing.T) {
			if tc.input.Invalid() != tc.expected {
				t.Errorf("expected=%v, actual=%v", tc.expected, tc.input.Invalid())
			}
		})
	}
}

func TestSyncPositionInvalidConstructor(t *testing.T) {
	require.True(t, InvalidSyncPosition().Invalid())
}

func TestSyncPositionLessThan(t *testing.T) {
	tcs := []struct {
		left     SyncPosition
		right    SyncPosition
		expected bool
	}{
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 0, GLSN: 0},
			expected: false,
		},
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 0, GLSN: 1},
			expected: false,
		},
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 1, GLSN: 0},
			expected: false,
		},
		{
			left:     SyncPosition{LLSN: 0, GLSN: 0},
			right:    SyncPosition{LLSN: 1, GLSN: 1},
			expected: true,
		},
	}

	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.left.String()+"_"+tc.right.String(), func(t *testing.T) {
			if tc.left.LessThan(tc.right) != tc.expected {
				t.Errorf("expected=%v, actual=%v", tc.expected, tc.left.LessThan(tc.right))
			}
		})
	}
}
