package logstream

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestLogStreamContext(t *testing.T) {
	defer goleak.VerifyNone(t)

	lsc := newLogStreamContext()

	version, highWatermark, uncommittedLLSNBegin := lsc.reportCommitBase()
	require.Equal(t, types.InvalidVersion, version)
	require.Equal(t, types.InvalidGLSN, highWatermark)
	require.Equal(t, types.MinLLSN, uncommittedLLSNBegin)

	expected := varlogpb.LogEntryMeta{
		TopicID:     1,
		LogStreamID: 2,
		LLSN:        3,
		GLSN:        4,
	}
	lsc.setLocalHighWatermark(expected)
	assert.Equal(t, expected, lsc.localHighWatermark())

	lsc.setLocalLowWatermark(expected)
	assert.Equal(t, expected, lsc.localLowWatermark())
}

func TestDecidableCondition(t *testing.T) {
	defer goleak.VerifyNone(t)

	lsc := newLogStreamContext()
	dc := newDecidableCondition(lsc)

	// global high watermark is lower than 2, so it does not know whether there is a log at GLSN 1.
	assert.False(t, dc.decidable(1))

	dc.change(func() {
		lsc.storeReportCommitBase(1, 1, 2)
	})

	assert.True(t, dc.decidable(1))
}

func TestDecidableCondition_InterruptWaiters(t *testing.T) {
	defer goleak.VerifyNone(t)

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
		lsc.storeReportCommitBase(1, 1, 2)
	})
	runtime.Gosched()
	dc.change(func() {
		lsc.storeReportCommitBase(2, 2, 3)
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
