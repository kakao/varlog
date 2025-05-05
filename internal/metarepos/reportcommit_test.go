package metarepos_test

import (
	"context"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

// TestReportCommit verifies RPCs between the metadata repository and storage node.
// See [github.com/kakao/varlog/proto/snpb.LogStreamCommitResult].
func TestReportCommit(t *testing.T) {
	const (
		cid   = types.ClusterID(1)
		snid  = types.StorageNodeID(2)
		tpid  = types.TopicID(3)
		lsid1 = types.LogStreamID(4)
		lsid2 = types.LogStreamID(5)
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := zaptest.NewLogger(t)

	var knownVersions struct {
		vers map[types.LogStreamID]types.Version
		sync.Mutex
	}
	knownVersions.vers = make(map[types.LogStreamID]types.Version, 2)
	knownVersions.vers[lsid1] = types.InvalidVersion
	knownVersions.vers[lsid2] = types.InvalidVersion

	reportsCommits := map[types.LogStreamID][]struct {
		report         snpb.LogStreamUncommitReport
		expectedCommit snpb.LogStreamCommitResult
	}{
		lsid1: {
			// known version 0
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid1,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 0,
					Version:               0,
					HighWatermark:         0,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid1,
					Version:             1,
					HighWatermark:       10,
					CommittedLLSNOffset: 1,
					CommittedGLSNOffset: 1,
					CommittedGLSNLength: 0,
				},
			},
			// known version 1
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid1,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 0,
					Version:               1,
					HighWatermark:         10,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid1,
					Version:             2,
					HighWatermark:       20,
					CommittedLLSNOffset: 1,
					CommittedGLSNOffset: 1,
					CommittedGLSNLength: 0,
				},
			},
			// known version 2
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid1,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 10,
					Version:               2,
					HighWatermark:         20,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid1,
					Version:             3,
					HighWatermark:       40,
					CommittedLLSNOffset: 1,
					CommittedGLSNOffset: 21,
					CommittedGLSNLength: 10,
				},
			},
			// known version 3
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid1,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 10,
					Version:               3,
					HighWatermark:         40,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid1,
					Version:             4,
					HighWatermark:       50,
					CommittedLLSNOffset: 11,
					CommittedGLSNOffset: 41,
					CommittedGLSNLength: 10,
				},
			},
			// known version >= 4
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid1,
					UncommittedLLSNOffset: 21,
					UncommittedLLSNLength: 0,
					Version:               4,
					HighWatermark:         50,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid1,
					Version:             5,
					HighWatermark:       50,
					CommittedLLSNOffset: 21,
					CommittedGLSNOffset: 51,
					CommittedGLSNLength: 0,
				},
			},
		},
		lsid2: {
			// known version 0
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid2,
					UncommittedLLSNOffset: 1,
					UncommittedLLSNLength: 10,
					Version:               0,
					HighWatermark:         0,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid2,
					Version:             1,
					HighWatermark:       10,
					CommittedLLSNOffset: 1,
					CommittedGLSNOffset: 1,
					CommittedGLSNLength: 10,
				},
			},
			// known version 1
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid2,
					UncommittedLLSNOffset: 11,
					UncommittedLLSNLength: 10,
					Version:               1,
					HighWatermark:         10,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid2,
					Version:             2,
					HighWatermark:       20,
					CommittedLLSNOffset: 11,
					CommittedGLSNOffset: 11,
					CommittedGLSNLength: 10,
				},
			},
			// known version 2
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid2,
					UncommittedLLSNOffset: 21,
					UncommittedLLSNLength: 10,
					Version:               2,
					HighWatermark:         20,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid2,
					Version:             3,
					HighWatermark:       40,
					CommittedLLSNOffset: 21,
					CommittedGLSNOffset: 31,
					CommittedGLSNLength: 10,
				},
			},
			// known version 3
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid2,
					UncommittedLLSNOffset: 31,
					UncommittedLLSNLength: 0,
					Version:               3,
					HighWatermark:         40,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid2,
					Version:             4,
					HighWatermark:       50,
					CommittedLLSNOffset: 31,
					CommittedGLSNOffset: 41,
					CommittedGLSNLength: 0,
				},
			},
			// known version >= 4
			{
				report: snpb.LogStreamUncommitReport{
					LogStreamID:           lsid2,
					UncommittedLLSNOffset: 31,
					UncommittedLLSNLength: 0,
					Version:               4,
					HighWatermark:         50,
				},
				expectedCommit: snpb.LogStreamCommitResult{
					TopicID:             tpid,
					LogStreamID:         lsid2,
					Version:             5,
					HighWatermark:       50,
					CommittedLLSNOffset: 31,
					CommittedGLSNOffset: 41,
					CommittedGLSNLength: 0,
				},
			},
		},
	}

	var snClosed atomic.Bool
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	sn := storagenode.TestNewRPCServer(t, ctrl, snid)
	wg.Add(2)
	sn.MockLogStreamReporterServer.EXPECT().GetReport(gomock.Any()).DoAndReturn(
		func(stream snpb.LogStreamReporter_GetReportServer) (err error) {
			defer wg.Done()
			for !snClosed.Load() {
				_, err = stream.Recv()
				if err != nil {
					logger.Error("report", zap.Error(err))
					if err == io.EOF {
						err = nil
					}
					break
				}

				rsp := &snpb.GetReportResponse{StorageNodeID: snid}

				knownVersions.Lock()
				for lsid, ver := range knownVersions.vers {
					var report snpb.LogStreamUncommitReport
					if rcs := reportsCommits[lsid]; len(rcs) <= int(ver) {
						lastCommit := rcs[len(rcs)-1].expectedCommit
						report = snpb.LogStreamUncommitReport{
							LogStreamID:           lastCommit.LogStreamID,
							UncommittedLLSNOffset: lastCommit.CommittedLLSNOffset + types.LLSN(lastCommit.CommittedGLSNLength),
							UncommittedLLSNLength: 0,
							Version:               ver,
							HighWatermark:         lastCommit.HighWatermark,
						}
					} else {
						report = rcs[ver].report
					}
					rsp.UncommitReports = append(rsp.UncommitReports, report)
				}
				knownVersions.Unlock()
				sort.Slice(rsp.UncommitReports, func(i, j int) bool {
					return rsp.UncommitReports[i].LogStreamID < rsp.UncommitReports[j].LogStreamID
				})

				logger.Debug("report", zap.String("rsp", rsp.String()))

				err = stream.Send(rsp)
				if err != nil {
					logger.Error("report", zap.Error(err))
					break
				}
			}
			return err
		},
	).AnyTimes()
	sn.MockLogStreamReporterServer.EXPECT().CommitBatch(gomock.Any()).DoAndReturn(
		func(stream snpb.LogStreamReporter_CommitBatchServer) (err error) {
			defer wg.Done()
			var req *snpb.CommitBatchRequest
			for !snClosed.Load() {
				req, err = stream.Recv()
				if err != nil {
					logger.Error("commit batch", zap.Error(err))
					break
				}

				assert.Equal(t, snid, req.StorageNodeID)

				for _, cr := range req.CommitResults {
					lsid := cr.LogStreamID
					knownVersions.Lock()
					assert.Contains(t, knownVersions.vers, lsid)
					ver := knownVersions.vers[lsid]
					if ver >= cr.Version {
						knownVersions.Unlock()
						continue
					}
					assert.Equal(t, reportsCommits[lsid][ver].expectedCommit, cr)
					knownVersions.vers[lsid] = cr.Version
					knownVersions.Unlock()
				}

				logger.Debug("commit batch", zap.String("req", req.String()))
			}
			err = stream.SendAndClose(&snpb.CommitBatchResponse{})
			if err != nil {
				logger.Error("commit batch", zap.Error(err))
			}
			return err
		},
	).AnyTimes()

	sn.Run()
	defer func() {
		sn.Close()
		snClosed.Store(true)
	}()

	mr := metarepos.NewRaftMetadataRepository(
		metarepos.WithClusterID(cid),
		metarepos.WithRPCAddress(":0"),
		metarepos.WithRaftDirectory(filepath.Join(t.TempDir(), "raftdata")),
		metarepos.WithLogger(logger),
	)
	mr.Run()
	defer func() {
		err := mr.Close()
		require.NoError(t, err)
	}()

	err := mr.RegisterTopic(context.Background(), tpid)
	require.NoError(t, err)

	err = mr.RegisterStorageNode(context.Background(), &varlogpb.StorageNodeDescriptor{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: snid,
			Address:       sn.Address(),
		},
	})
	require.NoError(t, err)

	err = mr.RegisterLogStream(context.Background(), &varlogpb.LogStreamDescriptor{
		TopicID:     tpid,
		LogStreamID: lsid1,
		Status:      varlogpb.LogStreamStatusRunning,
		Replicas: []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID:   snid,
				StorageNodePath: "",
				DataPath:        "",
			},
		},
	})
	require.NoError(t, err)

	err = mr.RegisterLogStream(context.Background(), &varlogpb.LogStreamDescriptor{
		TopicID:     tpid,
		LogStreamID: lsid2,
		Status:      varlogpb.LogStreamStatusRunning,
		Replicas: []*varlogpb.ReplicaDescriptor{
			{
				StorageNodeID:   snid,
				StorageNodePath: "",
				DataPath:        "",
			},
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		knownVersions.Lock()
		defer knownVersions.Unlock()
		return knownVersions.vers[lsid1] >= types.Version(4) && knownVersions.vers[lsid2] >= types.Version(4)
	}, 10*time.Second, 100*time.Millisecond)
}
