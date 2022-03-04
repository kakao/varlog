package logstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestExecutor_SyncTracker(t *testing.T) {
	first := varlogpb.LogEntryMeta{LLSN: 1, GLSN: 1}
	last := varlogpb.LogEntryMeta{LLSN: 10, GLSN: 10}
	st := newSyncTracker(first, last)

	current := st.toSyncStatus().Current
	assert.Equal(t, snpb.SyncPosition{LLSN: 0, GLSN: 0}, current)

	st.setCursor(varlogpb.LogEntryMeta{LLSN: 1, GLSN: 1})
	current = st.toSyncStatus().Current
	assert.Equal(t, snpb.SyncPosition{LLSN: 1, GLSN: 1}, current)
}

func TestExecutor_SyncReplicateBuffer_InvalidSyncRange(t *testing.T) {
	srcReplica := varlogpb.Replica{}
	_, err := newSyncReplicateBuffer(srcReplica, snpb.SyncRange{
		FirstLLSN: types.InvalidLLSN,
		LastLLSN:  types.LLSN(10),
	})
	assert.Error(t, err)

	_, err = newSyncReplicateBuffer(srcReplica, snpb.SyncRange{
		FirstLLSN: types.LLSN(2),
		LastLLSN:  types.LLSN(1),
	})
	assert.Error(t, err)
}

func TestExecutor_SyncReplicateBuffer_Add(t *testing.T) {
	srcReplica := varlogpb.Replica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: types.StorageNodeID(1),
			Address:       "sn1",
		},
		TopicID:     types.TopicID(2),
		LogStreamID: types.LogStreamID(3),
	}
	srb, err := newSyncReplicateBuffer(srcReplica, snpb.SyncRange{
		FirstLLSN: types.LLSN(1),
		LastLLSN:  types.LLSN(2),
	})
	assert.NoError(t, err)

	now := time.Now()

	// bad source replica
	assert.Error(t, srb.add(varlogpb.Replica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: types.StorageNodeID(2),
			Address:       "sn1",
		},
		TopicID:     types.TopicID(2),
		LogStreamID: types.LogStreamID(3),
	}, snpb.SyncPayload{}, now))

	// neither commit context nor log entry
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{}, now))

	// no commit context
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		CommitContext: nil,
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				LLSN: types.LLSN(1),
				GLSN: types.GLSN(1),
			},
		},
	}, now))

	// invalid commit context
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            1,
			HighWatermark:      1,
			CommittedGLSNBegin: 2,
			CommittedGLSNEnd:   1,
			CommittedLLSNBegin: 1,
		},
	}, now))

	// valid commit context
	assert.NoError(t, srb.add(srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            1,
			HighWatermark:      2,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   3,
			CommittedLLSNBegin: 1,
		},
	}, now))
	// already assigned commit context
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            1,
			HighWatermark:      2,
			CommittedGLSNBegin: 1,
			CommittedGLSNEnd:   3,
			CommittedLLSNBegin: 1,
		},
	}, now))

	// unexpected log entry: bad LLSN
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 1,
				LLSN: 2,
			},
		},
	}, now))
	// subtle case: valid log entry
	// assert.NoError(t, srb.add(srcReplica, snpb.SyncPayload{
	//	LogEntry: &varlogpb.LogEntry{
	//		LogEntryMeta: varlogpb.LogEntryMeta{
	//			GLSN: 2,
	//			LLSN: 1,
	//		},
	//	},
	// }, now))

	// valid log entry
	assert.NoError(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 1,
				LLSN: 1,
			},
		},
	}, now))
	// unexpected log entry: bad LLSN
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 2,
				LLSN: 1,
			},
		},
	}, now))
	// unexpected log entry: bad LLSN
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 2,
				LLSN: 3,
			},
		},
	}, now))
	// unexpected log entry: bad GLSN
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 1,
				LLSN: 2,
			},
		},
	}, now))

	// not committable: need more log entries
	assert.False(t, srb.committable())

	// valid log entry
	assert.NoError(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 2,
				LLSN: 2,
			},
		},
	}, now))

	// valid commit context and log entries to commit
	assert.True(t, srb.committable())

	// fully replicated
	assert.True(t, srb.end())

	// prepare new commit
	srb.reset()

	// current buffer is empty, thus other commit context and log entries can be filled
	assert.False(t, srb.end())

	// next commit context: not sequential
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            2,
			HighWatermark:      4,
			CommittedGLSNBegin: 3,
			CommittedGLSNEnd:   4,
			CommittedLLSNBegin: 4,
		},
	}, now))

	// next commit context: empty and valid commit context
	assert.NoError(t, srb.add(srcReplica, snpb.SyncPayload{
		CommitContext: &varlogpb.CommitContext{
			Version:            2,
			HighWatermark:      4,
			CommittedGLSNBegin: 3,
			CommittedGLSNEnd:   3,
			CommittedLLSNBegin: 3,
		},
	}, now))
	// bad log entry
	assert.Error(t, srb.add(srcReplica, snpb.SyncPayload{
		LogEntry: &varlogpb.LogEntry{
			LogEntryMeta: varlogpb.LogEntryMeta{
				GLSN: 3,
				LLSN: 3,
			},
		},
	}, now))
	// committable: empty commit context and no log entries
	assert.True(t, srb.committable())
}
