package logstream

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestLogStreamContext(t *testing.T) {
	tcs := []struct {
		name  string
		testf func(t *testing.T, lsc *logStreamContext)
	}{
		{
			name: "InitialValue",
			testf: func(t *testing.T, lsc *logStreamContext) {
				version, highWatermark, uncommittedBegin, invalid := lsc.reportCommitBase()
				require.Equal(t, types.InvalidVersion, version)
				require.Equal(t, types.InvalidGLSN, highWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: types.MinLLSN,
					GLSN: types.MinGLSN,
				}, uncommittedBegin)
				require.False(t, invalid)

				require.Equal(t, types.MinLLSN, lsc.uncommittedLLSNEnd.Load())

				localLWM, localHWM, _ := lsc.localWatermarks()
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: types.InvalidLLSN,
					GLSN: types.InvalidGLSN,
				}, localLWM)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: types.InvalidLLSN,
					GLSN: types.InvalidGLSN,
				}, localHWM)
			},
		},
		{
			name: "MaybeTrimmed",
			testf: func(t *testing.T, lsc *logStreamContext) {
				lsc.storeReportCommitBase(types.Version(1), types.GLSN(10), varlogpb.LogSequenceNumber{
					LLSN: 10,
					GLSN: 10,
				}, true)

				localLWM, localHWM, _ := lsc.localWatermarks()
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: types.InvalidLLSN,
					GLSN: types.InvalidGLSN,
				}, localLWM)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: types.InvalidLLSN,
					GLSN: types.InvalidGLSN,
				}, localHWM)
			},
		},
		{
			name: "HasLogs",
			testf: func(t *testing.T, lsc *logStreamContext) {
				lsc.storeReportCommitBase(types.Version(1), types.GLSN(10), varlogpb.LogSequenceNumber{
					LLSN: 10,
					GLSN: 10,
				}, true)

				lsc.setLocalLowWatermark(varlogpb.LogSequenceNumber{
					LLSN: 1,
					GLSN: 1,
				})

				localLWM, localHWM, _ := lsc.localWatermarks()
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 1,
					GLSN: 1,
				}, localLWM)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 9,
					GLSN: 9,
				}, localHWM)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			lsc := newLogStreamContext()
			tc.testf(t, lsc)
		})
	}
}

func TestDecidableCondition(t *testing.T) {
	lsc := newLogStreamContext()
	dc := newDecidableCondition(lsc)

	// global high watermark is lower than 2, so it does not know whether there is a log at GLSN 1.
	assert.False(t, dc.decidable(1))

	dc.change(func() {
		lsc.storeReportCommitBase(1, 1, varlogpb.LogSequenceNumber{
			LLSN: 2,
			GLSN: 2,
		}, false)
	})

	assert.True(t, dc.decidable(1))
}

func TestDecidableCondition_InterruptWaiters(t *testing.T) {
	lsc := newLogStreamContext()
	dc := newDecidableCondition(lsc)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := dc.waitC(context.Background(), 2)
		assert.NoError(t, err)
	}()
	dc.change(func() {
		lsc.storeReportCommitBase(1, 1,
			varlogpb.LogSequenceNumber{
				LLSN: 2,
				GLSN: 2,
			},
			false)
	})
	runtime.Gosched()
	dc.change(func() {
		lsc.storeReportCommitBase(2, 2, varlogpb.LogSequenceNumber{
			LLSN: 3,
			GLSN: 3,
		}, false)
	})
	wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := dc.waitC(ctx, 3)
		assert.Error(t, err)
	}()
	dc.change(func() {
		cancel()
	})
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := dc.waitC(context.Background(), 3)
		assert.Error(t, err)
	}()
	dc.destroy()
	wg.Wait()

}
