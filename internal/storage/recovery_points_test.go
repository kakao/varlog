package storage

import (
	"math/rand/v2"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestRecoveryPoints_MarshalLogObject(t *testing.T) {
	getRandomRecoveryPoints := func() RecoveryPoints {
		rp := RecoveryPoints{}
		if rand.N(2) == 0 {
			rp.LastCommitContext = &CommitContext{
				Version:            types.Version(rand.Uint64()),
				HighWatermark:      types.GLSN(rand.Uint64()),
				CommittedGLSNBegin: types.GLSN(rand.Uint64()),
				CommittedGLSNEnd:   types.GLSN(rand.Uint64()),
				CommittedLLSNBegin: types.LLSN(rand.Uint64()),
			}
		}
		if rand.N(2) == 0 {
			rp.CommittedLogEntry.First = &varlogpb.LogSequenceNumber{
				LLSN: types.LLSN(rand.Uint64()),
				GLSN: types.GLSN(rand.Uint64()),
			}
		}
		if rand.N(2) == 0 {
			rp.CommittedLogEntry.Last = &varlogpb.LogSequenceNumber{
				LLSN: types.LLSN(rand.Uint64()),
				GLSN: types.GLSN(rand.Uint64()),
			}
		}
		rp.UncommittedLLSN.Begin = types.LLSN(rand.Uint64())
		rp.UncommittedLLSN.End = types.LLSN(rand.Uint64())
		return rp
	}

	tcs := []struct {
		name string
		rp   RecoveryPoints
	}{
		{
			name: "ZeroValue",
			rp:   RecoveryPoints{},
		},
		{
			name: "RandomValue",
			rp:   getRandomRecoveryPoints(),
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			logger := zaptest.NewLogger(t)
			t.Cleanup(func() {
				_ = logger.Sync()
			})

			logger.Info("test", zap.Object("recoveryPoints", tc.rp))
		})
	}
}
