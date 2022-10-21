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
	lsc := newLogStreamContext()

	version, highWatermark, uncommittedBegin, invalid := lsc.reportCommitBase()
	require.Equal(t, types.InvalidVersion, version)
	require.Equal(t, types.InvalidGLSN, highWatermark)
	require.Equal(t, types.MinLLSN, uncommittedBegin.LLSN)
	require.False(t, invalid)

	expected := varlogpb.LogSequenceNumber{
		LLSN: 3,
		GLSN: 4,
	}
	uncommittedBegin = expected
	uncommittedBegin.LLSN++
	uncommittedBegin.GLSN++
	lsc.storeReportCommitBase(version, highWatermark, uncommittedBegin, invalid)
	assert.Equal(t, expected, lsc.localHighWatermark())

	lsc.setLocalLowWatermark(expected)
	assert.Equal(t, expected, lsc.localLowWatermark())
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
