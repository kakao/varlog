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
		Data:      [][]byte{[]byte("I")},
		BeginLLSN: types.LLSN(1),
	}
	smallPayload, err := smallRequest.Marshal()
	require.NoError(t, err)

	mediumRequest := ReplicateRequest{
		Data:      [][]byte{[]byte("XI"), []byte("XII"), []byte("XIII"), []byte("XIV"), []byte("XV")},
		BeginLLSN: types.LLSN(11),
	}
	mediumPayload, err := mediumRequest.Marshal()
	require.NoError(t, err)

	largeRequest := ReplicateRequest{
		Data:      [][]byte{[]byte("XXI"), []byte("XXII"), []byte("XXIII"), []byte("XXIV"), []byte("XXV"), []byte("XXVI"), []byte("XXVII"), []byte("XXVIII"), []byte("XXIX"), []byte("XXX")},
		BeginLLSN: types.LLSN(21),
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
				wantDataPtrs := get2DPointers(req.Data)

				req.ResetReuse()
				err = req.Unmarshal(tc.payload)
				require.NoError(t, err)
				require.Equal(t, tc.req, req)
				gotDataPtrs := get2DPointers(req.Data)

				require.Equal(t, wantDataPtrs, gotDataPtrs)
			})
		}
	})

	t.Run("GrowingLengthGrowingCapacity", func(t *testing.T) {
		var req ReplicateRequest

		err := req.Unmarshal(smallPayload)
		require.NoError(t, err)
		require.Equal(t, smallRequest, req)
		smallDataPtrs := get2DPointers(req.Data)
		smallDataCap := cap(req.Data)

		req.ResetReuse()
		err = req.Unmarshal(mediumPayload)
		require.NoError(t, err)
		require.Equal(t, mediumRequest, req)
		mediumDataPtrs := get2DPointers(req.Data)
		mediumDataCap := cap(req.Data)

		require.NotEqual(t, smallDataPtrs, mediumDataPtrs)
		require.Less(t, smallDataCap, mediumDataCap)

		req.ResetReuse()
		err = req.Unmarshal(laregePayload)
		require.NoError(t, err)
		require.Equal(t, largeRequest, req)
		largeDataPtrs := get2DPointers(req.Data)
		largeDataCap := cap(req.Data)

		require.NotEqual(t, mediumDataPtrs, largeDataPtrs)
		require.Less(t, mediumDataCap, largeDataCap)
	})

	t.Run("ShrinkingLengthEqualCapacity", func(t *testing.T) {
		var req ReplicateRequest

		err := req.Unmarshal(laregePayload)
		require.NoError(t, err)
		require.Equal(t, largeRequest, req)
		largeDataPtrs := get2DPointers(req.Data)
		largeDataCap := cap(req.Data)

		req.ResetReuse()
		err = req.Unmarshal(mediumPayload)
		require.NoError(t, err)
		require.Equal(t, mediumRequest, req)
		mediumDataPtrs := get2DPointers(req.Data)
		mediumDataCap := cap(req.Data)

		require.NotEqual(t, largeDataPtrs, mediumDataPtrs)
		require.Equal(t, largeDataCap, mediumDataCap)

		req.ResetReuse()
		err = req.Unmarshal(smallPayload)
		require.NoError(t, err)
		require.Equal(t, smallRequest, req)
		smallDataPtrs := get2DPointers(req.Data)
		smallDataCap := cap(req.Data)

		require.NotEqual(t, mediumDataPtrs, smallDataPtrs)
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

func TestInvalidSyncRange(t *testing.T) {
	sr := InvalidSyncRange()
	require.True(t, sr.Invalid())
}

func TestSyncRangeInvalid(t *testing.T) {
	tcs := []struct {
		sr       SyncRange
		expected bool
	}{
		{
			sr:       SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.InvalidLLSN},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.LLSN(1), LastLLSN: types.InvalidLLSN},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.LLSN(1)},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.LLSN(2), LastLLSN: types.LLSN(1)},
			expected: true,
		},
		{
			sr:       SyncRange{FirstLLSN: types.LLSN(1), LastLLSN: types.LLSN(2)},
			expected: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.sr.String(), func(t *testing.T) {
			require.Equal(t, tc.expected, tc.sr.Invalid())
		})
	}
}

func TestSyncRangeValidate(t *testing.T) {
	tcs := []struct {
		sr      SyncRange
		wantErr bool
	}{
		{
			sr:      SyncRange{FirstLLSN: types.LLSN(2), LastLLSN: types.LLSN(1)},
			wantErr: true,
		},
		{
			sr:      SyncRange{FirstLLSN: types.InvalidLLSN, LastLLSN: types.LLSN(1)},
			wantErr: true,
		},
		{
			sr:      SyncRange{FirstLLSN: types.LLSN(1), LastLLSN: types.LLSN(2)},
			wantErr: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.sr.String(), func(t *testing.T) {
			err := tc.sr.Validate()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
