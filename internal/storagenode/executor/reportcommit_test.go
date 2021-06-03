package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/storage"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func TestLogStreamReporter(t *testing.T) {
	const (
		snid  = types.StorageNodeID(1)
		lsid1 = types.LogStreamID(1)
		lsid2 = types.LogStreamID(2)
	)

	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	strg1, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse1, err := New(
		WithStorageNodeID(snid),
		WithLogStreamID(lsid1),
		WithStorage(strg1),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, lse1.Close()) }()

	status, sealedGLSN, err := lse1.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)
	require.NoError(t, lse1.Unseal(context.TODO(), []snpb.Replica{
		{
			StorageNodeID: lse1.storageNodeID,
			LogStreamID:   lse1.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse1.Metadata().Status)

	strg2, err := storage.NewStorage(storage.WithPath(t.TempDir()))
	require.NoError(t, err)
	lse2, err := New(
		WithStorageNodeID(snid),
		WithLogStreamID(lsid2),
		WithStorage(strg2),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, lse2.Close()) }()

	status, sealedGLSN, err = lse2.Seal(context.TODO(), types.InvalidGLSN)
	require.NoError(t, err)
	require.Equal(t, types.InvalidGLSN, sealedGLSN)
	require.Equal(t, varlogpb.LogStreamStatusSealed, status)
	require.NoError(t, lse2.Unseal(context.TODO(), []snpb.Replica{
		{
			StorageNodeID: lse2.storageNodeID,
			LogStreamID:   lse2.logStreamID,
		},
	}))
	require.Equal(t, varlogpb.LogStreamStatusRunning, lse2.Metadata().Status)

	rcg := reportcommitter.NewMockGetter(ctrl)
	rcg.EXPECT().ReportCommitter(gomock.Any()).DoAndReturn(
		func(lsid types.LogStreamID) (reportcommitter.ReportCommitter, bool) {
			switch lsid {
			case lsid1:
				return lse1, true
			case lsid2:
				return lse2, true
			default:
				return nil, false
			}
		},
	).AnyTimes()
	rcg.EXPECT().ReportCommitters().Return([]reportcommitter.ReportCommitter{lse1, lse2}).AnyTimes()

	_, err = New()
	require.Error(t, err)

	getter := id.NewMockStorageNodeIDGetter(ctrl)
	getter.EXPECT().StorageNodeID().Return(snid).AnyTimes()

	lsr := reportcommitter.New(
		reportcommitter.WithStorageNodeIDGetter(getter),
		reportcommitter.WithReportCommitterGetter(rcg),
	)

	err = lsr.Commit(context.TODO(), []*snpb.LogStreamCommitResult{})
	require.NoError(t, err)

	var (
		reports []*snpb.LogStreamUncommitReport
		wg      sync.WaitGroup
	)

	// Reports
	reports, err = lsr.GetReport(context.TODO())
	require.NoError(t, err)
	require.Len(t, reports, 2)
	for _, report := range reports {
		switch report.GetLogStreamID() {
		case lsid1:
			require.Equal(t, types.GLSN(0), report.GetHighWatermark())
			require.Equal(t, types.LLSN(1), report.GetUncommittedLLSNOffset())
			require.EqualValues(t, 0, report.GetUncommittedLLSNLength())
		case lsid2:
			require.Equal(t, types.GLSN(0), report.GetHighWatermark())
			require.Equal(t, types.LLSN(1), report.GetUncommittedLLSNOffset())
			require.EqualValues(t, 0, report.GetUncommittedLLSNLength())
		}
	}

	// Append
	// LSE1
	// LSE2
	wg.Add(3)
	go func() {
		defer wg.Done()

		glsn, err := lse1.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, types.GLSN(1), glsn)
	}()
	go func() {
		defer wg.Done()

		glsn, err := lse2.Append(context.TODO(), []byte("foo"))
		require.NoError(t, err)
		require.Equal(t, types.GLSN(2), glsn)
	}()
	go func() {
		defer wg.Done()

		require.Eventually(t, func() bool {
			reports, err := lsr.GetReport(context.TODO())
			require.NoError(t, err)
			return reports[0].UncommittedLLSNLength > 0 && reports[1].UncommittedLLSNLength > 0
		}, time.Second, time.Millisecond)

		err := lsr.Commit(context.TODO(), []*snpb.LogStreamCommitResult{
			{
				LogStreamID:         lsid1,
				HighWatermark:       2,
				PrevHighWatermark:   0,
				CommittedGLSNOffset: 1,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: 1,
			},
			{
				LogStreamID:         lsid2,
				HighWatermark:       2,
				PrevHighWatermark:   0,
				CommittedGLSNOffset: 2,
				CommittedGLSNLength: 1,
				CommittedLLSNOffset: 1,
			},
		})
		require.NoError(t, err)
	}()
	wg.Wait()

	require.Eventually(t, func() bool {
		reports, err := lsr.GetReport(context.TODO())
		require.NoError(t, err)
		return reports[0].UncommittedLLSNLength == 0 && reports[1].UncommittedLLSNLength == 0
	}, time.Second, time.Millisecond)

	require.NoError(t, lsr.Close())
	require.NoError(t, lsr.Close())

	_, err = lsr.GetReport(context.TODO())
	require.Error(t, err)

	err = lsr.Commit(context.TODO(), []*snpb.LogStreamCommitResult{
		{
			LogStreamID:         lsid1,
			HighWatermark:       4,
			PrevHighWatermark:   2,
			CommittedGLSNOffset: 3,
			CommittedGLSNLength: 1,
			CommittedLLSNOffset: 2,
		},
		{
			LogStreamID:         lsid2,
			HighWatermark:       4,
			PrevHighWatermark:   2,
			CommittedGLSNOffset: 4,
			CommittedGLSNLength: 1,
			CommittedLLSNOffset: 2,
		},
	})
	require.Error(t, err)
}
