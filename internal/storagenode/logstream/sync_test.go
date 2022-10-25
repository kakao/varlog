package logstream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestExecutor_SyncTracker(t *testing.T) {
	first := varlogpb.LogSequenceNumber{LLSN: 1, GLSN: 1}
	last := varlogpb.LogSequenceNumber{LLSN: 10, GLSN: 10}
	st := newSyncTracker(first, last)

	current := st.toSyncStatus().Current
	assert.Equal(t, snpb.SyncPosition{LLSN: 0, GLSN: 0}, current)

	st.setCursor(varlogpb.LogEntryMeta{LLSN: 1, GLSN: 1})
	current = st.toSyncStatus().Current
	assert.Equal(t, snpb.SyncPosition{LLSN: 1, GLSN: 1}, current)
}
