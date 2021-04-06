package executor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kakao/varlog/pkg/verrors"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/kakao/varlog/internal/storagenode/replication"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
	"github.com/kakao/varlog/proto/snpb"
)

func TestReplicationProcessorFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	connector := replication.NewMockConnector(ctrl)
	state := NewMockStateProvider(ctrl)

	// queue size
	_, err := newReplicator(replicatorConfig{
		queueSize: 0,
		connector: connector,
		state:     state,
	})
	require.Error(t, err)

	// no connector
	_, err = newReplicator(replicatorConfig{
		queueSize: 1,
		connector: nil,
		state:     state,
	})
	require.Error(t, err)

	// no state
	_, err = newReplicator(replicatorConfig{
		queueSize: 1,
		connector: connector,
		state:     nil,
	})
	require.Error(t, err)
}

func TestReplicationProcessorNoClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	const queueSize = 10

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// connector
	connector := replication.NewMockConnector(ctrl)
	connector.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, errors.New("fake")).AnyTimes()

	// state provider
	var called atomicutil.AtomicBool
	called.Store(false)
	state := NewMockStateProvider(ctrl)
	state.EXPECT().mutableWithBarrier().Return(nil)
	state.EXPECT().releaseBarrier().Return()
	state.EXPECT().setSealing().DoAndReturn(func() {
		called.Store(true)
	}).MaxTimes(2)

	rp, err := newReplicator(replicatorConfig{
		queueSize: queueSize,
		connector: connector,
		state:     state,
	})
	require.NoError(t, err)
	defer rp.stop()

	rtb := newReplicateTask()
	rtb.llsn = types.LLSN(1)
	rtb.replicas = []snpb.Replica{
		{
			StorageNodeID: 1,
			LogStreamID:   1,
			Address:       "localhost:12345",
		},
	}

	err = rp.send(context.TODO(), rtb)
	require.NoError(t, err)
	require.Eventually(t, called.Load, time.Second, 10*time.Millisecond)
}

func TestReplicationProcessor(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		queueSize = 10
		numLogs   = 100
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var testCases []*struct {
		rp             *replicatorImpl
		expectedSealed bool
		actualSealed   atomicutil.AtomicBool
	}

	for _, cbErr := range []error{nil, errors.New("fake")} {
		cbErr := cbErr

		tc := &struct {
			rp             *replicatorImpl
			expectedSealed bool
			actualSealed   atomicutil.AtomicBool
		}{}

		client := replication.NewMockClient(ctrl)
		client.EXPECT().Replicate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ types.LLSN, _ []byte, f func(error)) {
				f(cbErr)
			},
		).AnyTimes()
		connector := replication.NewMockConnector(ctrl)
		connector.EXPECT().Get(gomock.Any(), gomock.Any()).Return(client, nil).AnyTimes()

		tc.actualSealed.Store(false)
		state := NewMockStateProvider(ctrl)
		state.EXPECT().mutableWithBarrier().DoAndReturn(func() error {
			if tc.actualSealed.Load() {
				return verrors.ErrSealed
			}
			return nil
		}).AnyTimes()
		state.EXPECT().releaseBarrier().Return().AnyTimes()
		state.EXPECT().setSealing().DoAndReturn(func() {
			tc.actualSealed.Store(true)
			t.Log("setSealing")
		}).AnyTimes()

		tc.expectedSealed = cbErr != nil

		rp, err := newReplicator(replicatorConfig{
			queueSize: queueSize,
			connector: connector,
			state:     state,
		})
		require.NoError(t, err)
		tc.rp = rp

		testCases = append(testCases, tc)
	}

	for i := range testCases {
		tc := testCases[i]
		name := fmt.Sprintf("sealed=%+v", tc.expectedSealed)
		t.Run(name, func(t *testing.T) {
			for i := 1; i <= numLogs; i++ {
				rtb := newReplicateTask()
				rtb.llsn = types.LLSN(i)
				rtb.replicas = []snpb.Replica{
					{
						StorageNodeID: 1,
						LogStreamID:   1,
						Address:       "localhost:12345",
					},
				}
				if err := tc.rp.send(context.TODO(), rtb); err != nil {
					break
				}
			}

			// TODO: check status
			require.Eventually(t, func() bool {
				t.Logf("expected=%v, actual=%v", tc.expectedSealed, tc.actualSealed.Load())
				return tc.expectedSealed == tc.actualSealed.Load()
			}, time.Second, 10*time.Millisecond)

			tc.rp.stop()
		})
	}
}
