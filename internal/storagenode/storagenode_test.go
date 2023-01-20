package storagenode

import (
	"context"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/internal/reportcommitter"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestStorageNode(t *testing.T) {
	// TODO: uncomment it
	// defer goleak.VerifyNone(t)
	const (
		cid       = types.ClusterID(1)
		snid1     = types.StorageNodeID(1)
		snid2     = types.StorageNodeID(2)
		tpid      = types.TopicID(1)
		lsid      = types.LogStreamID(1)
		numLogs   = 10
		commitLen = 5
		dataSize  = 32
	)

	var (
		path1 = t.TempDir()
		path2 = t.TempDir()
	)

	rng := rand.New(rand.NewSource(time.Now().Unix()))

	var wg sync.WaitGroup

	// run sn1 and sn2
	sn1 := TestNewSimpleStorageNode(t,
		WithClusterID(cid),
		WithStorageNodeID(snid1),
		WithVolumes(path1),
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = sn1.Serve()
	}()
	sn2 := TestNewSimpleStorageNode(t,
		WithClusterID(cid),
		WithStorageNodeID(snid2),
		WithVolumes(path2),
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = sn2.Serve()
	}()

	// wait for sn1 and sn2 to serve
	TestWaitForStartingOfServe(t, sn1)
	TestWaitForStartingOfServe(t, sn2)

	// replicas
	replicas := []varlogpb.LogStreamReplica{
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid1,
				Address:       sn1.advertise,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		},
		{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid2,
				Address:       sn2.advertise,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		},
	}

	// sn1: get path
	snmd1 := TestGetStorageNodeMetadataDescriptor(t, cid, sn1.snid, sn1.advertise)
	assert.Equal(t, snid1, snmd1.StorageNode.StorageNodeID)
	assert.NotEmpty(t, snmd1.Storages)
	assert.NotEmpty(t, snmd1.Storages[0].Path)
	// sn1: add ls
	TestAddLogStreamReplica(t, cid, sn1.snid, tpid, lsid, snmd1.Storages[0].Path, sn1.advertise)

	// sn2: get path
	snmd2 := TestGetStorageNodeMetadataDescriptor(t, cid, sn2.snid, sn2.advertise)
	assert.Equal(t, snid2, snmd2.StorageNode.StorageNodeID)
	assert.NotEmpty(t, snmd2.Storages)
	assert.NotEmpty(t, snmd2.Storages[0].Path)
	// sn2: add ls
	TestAddLogStreamReplica(t, cid, sn2.snid, tpid, lsid, snmd2.Storages[0].Path, sn2.advertise)

	// sn1: seal & unseal
	lss, lastCommittedGLSN := TestSealLogStreamReplica(t, cid, sn1.snid, tpid, lsid, types.InvalidGLSN, sn1.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss)
	assert.Equal(t, types.InvalidGLSN, lastCommittedGLSN)
	TestUnsealLogStreamReplica(t, cid, sn1.snid, tpid, lsid, replicas, sn1.advertise)
	// sn2: seal & unseal
	lss, lastCommittedGLSN = TestSealLogStreamReplica(t, cid, sn2.snid, tpid, lsid, types.InvalidGLSN, sn2.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss)
	assert.Equal(t, types.InvalidGLSN, lastCommittedGLSN)
	TestUnsealLogStreamReplica(t, cid, sn2.snid, tpid, lsid, replicas, sn2.advertise)

	var (
		lastGLSN    = types.InvalidGLSN
		lastLLSN    = types.InvalidLLSN
		lastVersion = types.InvalidVersion
	)
	// Append
	for i := 0; i < numLogs; i += commitLen {
		data := make([]byte, dataSize)
		_, _ = rng.Read(data)
		dataBatch := [][]byte{data}
		cr := snpb.LogStreamCommitResult{
			TopicID:             tpid,
			LogStreamID:         lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: commitLen,
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + commitLen,
		}
		var appendWg sync.WaitGroup
		for j := 0; j < commitLen; j++ {
			appendWg.Add(1)
			go func() {
				defer appendWg.Done()
				res := TestAppend(t, tpid, lsid, dataBatch, replicas)
				assert.Len(t, res, 1)
			}()
		}

		appendWg.Add(1)
		go func() {
			defer appendWg.Done()
			assert.Eventually(t, func() bool {
				reportcommitter.TestCommit(t, sn1.advertise, snpb.CommitRequest{
					StorageNodeID: snid1,
					CommitResult:  cr,
				})
				reports := reportcommitter.TestGetReport(t, sn1.advertise)
				assert.Len(t, reports, 1)
				return reports[0].Version == lastVersion+1
			}, time.Second, 10*time.Millisecond)
		}()
		appendWg.Add(1)
		go func() {
			defer appendWg.Done()
			assert.Eventually(t, func() bool {
				reportcommitter.TestCommit(t, sn2.advertise, snpb.CommitRequest{
					StorageNodeID: snid2,
					CommitResult:  cr,
				})
				reports := reportcommitter.TestGetReport(t, sn2.advertise)
				assert.Len(t, reports, 1)
				return reports[0].Version == lastVersion+1
			}, time.Second, 10*time.Millisecond)
		}()
		appendWg.Wait()
		lastLLSN += commitLen
		lastGLSN += commitLen
		lastVersion++
	}

	// CC  : +-- 1 --+ +-- 2 ---+
	// LLSN: 1 2 3 4 5 6 7 8 9 10
	// GLSN: 1 2 3 4 5 6 7 8 9 10

	// Subscribe: [1, 11)
	les1 := TestSubscribe(t, tpid, lsid, types.MinGLSN, lastGLSN+1, snid1, sn1.advertise)
	les2 := TestSubscribe(t, tpid, lsid, types.MinGLSN, lastGLSN+1, snid2, sn2.advertise)
	expectedLen := int(lastLLSN)
	assert.Equal(t, les1, les2)
	assert.Len(t, les1, expectedLen)
	assert.Equal(t, types.MinLLSN, les1[0].LLSN)
	assert.Equal(t, lastLLSN, les1[expectedLen-1].LLSN)
	assert.Equal(t, types.MinGLSN, les1[0].GLSN)
	assert.Equal(t, lastGLSN, les1[expectedLen-1].GLSN)
	assert.True(t, sort.SliceIsSorted(les1, func(i, j int) bool {
		return les1[i].LLSN < les1[j].LLSN && les1[i].GLSN < les1[j].GLSN
	}))

	// SubscribeTo: [1, 11)
	les1 = TestSubscribeTo(t, tpid, lsid, types.MinLLSN, lastLLSN+1, snid1, sn1.advertise)
	les2 = TestSubscribeTo(t, tpid, lsid, types.MinLLSN, lastLLSN+1, snid2, sn2.advertise)
	expectedLen = int(lastLLSN)
	assert.Equal(t, les1, les2)
	assert.Len(t, les1, expectedLen)
	assert.Equal(t, types.MinLLSN, les1[0].LLSN)
	assert.Equal(t, lastLLSN, les1[numLogs-1].LLSN)
	assert.True(t, sort.SliceIsSorted(les1, func(i, j int) bool {
		return les1[i].LLSN < les1[j].LLSN
	}))

	// Append to only one replica (snid1)
	for i := 0; i < numLogs; i += commitLen {
		data := make([]byte, dataSize)
		_, _ = rng.Read(data)
		dataBatch := [][]byte{data}
		cr := snpb.LogStreamCommitResult{
			TopicID:             tpid,
			LogStreamID:         lsid,
			CommittedLLSNOffset: lastLLSN + 1,
			CommittedGLSNOffset: lastGLSN + 1,
			CommittedGLSNLength: commitLen,
			Version:             lastVersion + 1,
			HighWatermark:       lastGLSN + commitLen,
		}
		var appendWg sync.WaitGroup
		for j := 0; j < commitLen; j++ {
			appendWg.Add(1)
			go func() {
				defer appendWg.Done()
				res := TestAppend(t, tpid, lsid, dataBatch, replicas)
				assert.Len(t, res, 1)
			}()
		}

		appendWg.Add(1)
		go func() {
			defer appendWg.Done()
			assert.Eventually(t, func() bool {
				reportcommitter.TestCommit(t, sn1.advertise, snpb.CommitRequest{
					StorageNodeID: snid1,
					CommitResult:  cr,
				})
				reports := reportcommitter.TestGetReport(t, sn1.advertise)
				assert.Len(t, reports, 1)
				return reports[0].Version == lastVersion+1
			}, time.Second, 10*time.Millisecond)
		}()
		appendWg.Wait()
		lastLLSN += commitLen
		lastGLSN += commitLen
		lastVersion++
	}

	// CC  : +-- 1 --+ +-- 2 ---+ +---- 3 -----+ +---- 4 -----+
	// LLSN: 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
	// GLSN: 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20

	// Subscribe: [1, 21)
	les1 = TestSubscribe(t, tpid, lsid, types.MinGLSN, lastGLSN+1, snid1, sn1.advertise)
	expectedLen = int(lastLLSN)
	assert.Len(t, les1, expectedLen)
	assert.Equal(t, types.MinLLSN, les1[0].LLSN)
	assert.Equal(t, lastLLSN, les1[expectedLen-1].LLSN)
	assert.Equal(t, types.MinGLSN, les1[0].GLSN)
	assert.Equal(t, lastGLSN, les1[expectedLen-1].GLSN)
	assert.True(t, sort.SliceIsSorted(les1, func(i, j int) bool {
		return les1[i].LLSN < les1[j].LLSN && les1[i].GLSN < les1[j].GLSN
	}))

	// seal
	lss, lastCommittedGLSN = TestSealLogStreamReplica(t, cid, sn1.snid, tpid, lsid, lastGLSN, sn1.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss)
	assert.Equal(t, lastGLSN, lastCommittedGLSN)
	lss, lastCommittedGLSN = TestSealLogStreamReplica(t, cid, sn2.snid, tpid, lsid, lastGLSN, sn2.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, lss)
	assert.Equal(t, types.GLSN(numLogs), lastCommittedGLSN)

	// sync
	assert.Eventually(t, func() bool {
		syncStatus := TestSync(t, cid, sn1.snid, tpid, lsid, lastGLSN, sn1.advertise, varlogpb.StorageNode{
			StorageNodeID: snid2,
			Address:       sn2.advertise,
		})
		return syncStatus.State == snpb.SyncStateComplete
	}, time.Second, 10*time.Millisecond)

	// Subscribe: [1, 21)
	les2 = TestSubscribe(t, tpid, lsid, types.MinGLSN, lastGLSN+1, snid2, sn2.advertise)
	expectedLen = int(lastLLSN)
	assert.Len(t, les2, expectedLen)
	assert.Equal(t, types.MinLLSN, les2[0].LLSN)
	assert.Equal(t, lastLLSN, les2[expectedLen-1].LLSN)
	assert.Equal(t, types.MinGLSN, les2[0].GLSN)
	assert.Equal(t, lastGLSN, les2[expectedLen-1].GLSN)
	assert.True(t, sort.SliceIsSorted(les2, func(i, j int) bool {
		return les2[i].LLSN < les2[j].LLSN && les2[i].GLSN < les2[j].GLSN
	}))

	// seal & unseal
	TestUnsealLogStreamReplica(t, cid, sn1.snid, tpid, lsid, replicas, sn1.advertise)
	TestSealLogStreamReplica(t, cid, sn2.snid, tpid, lsid, lastGLSN, sn2.advertise)
	TestUnsealLogStreamReplica(t, cid, sn2.snid, tpid, lsid, replicas, sn2.advertise)

	// trim
	TestTrim(t, cid, sn1.snid, tpid, 13, sn1.advertise)
	TestTrim(t, cid, sn2.snid, tpid, 13, sn2.advertise)

	// CC  : +-- 1 --+ +-- 2 ---+ +---- 3 -----+ +---- 4 -----+
	// LLSN: _ _ _ _ _ _ _ _ _ __ __ __ __ 14 15 16 17 18 19 20
	// GLSN: _ _ _ _ _ _ _ _ _ __ __ __ __ 14 15 16 17 18 19 20

	// Subscribe: [14, 21)
	les1 = TestSubscribe(t, tpid, lsid, 14, lastGLSN+1, snid1, sn1.advertise)
	les2 = TestSubscribe(t, tpid, lsid, 14, lastGLSN+1, snid2, sn2.advertise)
	expectedLen = int(lastLLSN - 14 + 1)
	assert.Equal(t, les1, les2)
	assert.Len(t, les1, expectedLen)
	assert.Equal(t, types.LLSN(14), les1[0].LLSN)
	assert.Equal(t, lastLLSN, les1[expectedLen-1].LLSN)
	assert.Equal(t, types.GLSN(14), les1[0].GLSN)
	assert.Equal(t, lastGLSN, les1[expectedLen-1].GLSN)
	assert.True(t, sort.SliceIsSorted(les1, func(i, j int) bool {
		return les1[i].LLSN < les1[j].LLSN && les1[i].GLSN < les1[j].GLSN
	}))

	// close sn1 and sn2
	assert.NoError(t, sn1.Close())
	assert.NoError(t, sn2.Close())
	wg.Wait()

	// rerun sn1
	sn1 = TestNewSimpleStorageNode(t,
		WithClusterID(cid),
		WithStorageNodeID(snid1),
		WithVolumes(path1),
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = sn1.Serve()
	}()
	// wait for sn1 to serve
	TestWaitForStartingOfServe(t, sn1)

	reports := reportcommitter.TestGetReport(t, sn1.advertise)
	assert.Len(t, reports, 1)
	assert.Equal(t, lastVersion, reports[0].Version)
	assert.Equal(t, lastLLSN, reports[0].UncommittedLLSNOffset-1)
	assert.Equal(t, lastGLSN, reports[0].HighWatermark)

	// close sn1
	assert.NoError(t, sn1.Close())
	wg.Wait()
}

func TestStorageNode_InvalidConfig(t *testing.T) {
	// bad id
	_, err := NewStorageNode(
		WithStorageNodeID(0),
		WithListenAddress("127.0.0.1:0"),
		WithVolumes(t.TempDir()),
	)
	assert.Error(t, err)

	// bad listen address
	_, err = NewStorageNode(
		WithListenAddress(""),
		WithVolumes(t.TempDir()),
	)
	assert.Error(t, err)

	// nil logger
	_, err = NewStorageNode(
		WithListenAddress("127.0.0.1:0"),
		WithLogger(nil),
		WithVolumes(t.TempDir()),
	)
	assert.Error(t, err)

	// bad volume: not dir
	fp, err := os.CreateTemp(t.TempDir(), "file")
	assert.NoError(t, err)
	badVolume := fp.Name()
	assert.NoError(t, fp.Close())
	_, err = NewStorageNode(
		WithListenAddress("127.0.0.1:0"),
		WithVolumes(badVolume),
	)
	assert.Error(t, err)

	// bad volume: unreadable
	badVolume = t.TempDir()
	assert.NoError(t, os.Chmod(badVolume, 0200))
	_, err = NewStorageNode(
		WithListenAddress("127.0.0.1:0"),
		WithVolumes(badVolume),
	)
	assert.Error(t, err)
	assert.NoError(t, os.Chmod(badVolume, 0700))

	// bad volume: duplicated settings
	badVolume = t.TempDir()
	_, err = NewStorageNode(
		WithListenAddress("127.0.0.1:0"),
		WithVolumes(badVolume, badVolume),
	)
	assert.Error(t, err)
}

func TestStorageNode_MakeVolumesAbsolute(t *testing.T) {
	sn, err := NewStorageNode(
		WithStorageNodeID(1),
		WithListenAddress("127.0.0.1:0"),
		WithVolumes("./testdata/relative_volume"),
	)
	assert.NoError(t, err)
	defer func() {
		ps, err := filepath.Glob("./testdata/relative_volume/*")
		assert.NoError(t, err)
		for _, p := range ps {
			if filepath.Base(p) == ".keep" {
				continue
			}
			_ = os.RemoveAll(p)
		}
	}()

	for _, volume := range sn.volumes {
		assert.True(t, filepath.IsAbs(volume))
	}
}

func TestStorageNode_Append(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)
	payload := [][]byte{{}}

	tcs := []struct {
		name  string
		testf func(t *testing.T, addr string, lc snpb.LogIOClient)
	}{
		{
			name: "InvalidTopicID",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				const invalidTopicID = types.TopicID(0)
				_, err := lc.Append(context.Background(), &snpb.AppendRequest{
					TopicID:     invalidTopicID,
					LogStreamID: lsid,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "InvalidLogStreamID",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				const invalidLogStreamID = types.LogStreamID(0)
				_, err := lc.Append(context.Background(), &snpb.AppendRequest{
					TopicID:     tpid,
					LogStreamID: invalidLogStreamID,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NoSuchTopic",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				_, err := lc.Append(context.Background(), &snpb.AppendRequest{
					TopicID:     tpid + 1,
					LogStreamID: lsid,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "NoSuchLogStream",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				_, err := lc.Append(context.Background(), &snpb.AppendRequest{
					TopicID:     tpid,
					LogStreamID: lsid + 1,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "NotPrimary",
			testf: func(t *testing.T, addr string, lc snpb.LogIOClient) {
				lss, lastGLSN := TestSealLogStreamReplica(t, cid, snid, tpid, lsid, types.InvalidGLSN, addr)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.True(t, lastGLSN.Invalid())

				TestUnsealLogStreamReplica(t, cid, snid, tpid, lsid, []varlogpb.LogStreamReplica{
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid + 1,
							Address:       addr,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
				}, addr)

				_, err := lc.Append(context.Background(), &snpb.AppendRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.Unavailable, status.Code(err))
			},
		},
		{
			name: "Sealed",
			testf: func(t *testing.T, addr string, lc snpb.LogIOClient) {
				lss, lastGLSN := TestSealLogStreamReplica(t, cid, snid, tpid, lsid, types.InvalidGLSN, addr)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.True(t, lastGLSN.Invalid())

				_, err := lc.Append(context.Background(), &snpb.AppendRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
			},
		},
		{
			name: "DeadlineExceeded",
			testf: func(t *testing.T, addr string, lc snpb.LogIOClient) {
				lss, lastGLSN := TestSealLogStreamReplica(t, cid, snid, tpid, lsid, types.InvalidGLSN, addr)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.True(t, lastGLSN.Invalid())

				TestUnsealLogStreamReplica(t, cid, snid, tpid, lsid, []varlogpb.LogStreamReplica{
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
				}, addr)

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				_, err := lc.Append(ctx, &snpb.AppendRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					Payload:     payload,
				})
				require.Error(t, err)
				require.Equal(t, codes.DeadlineExceeded, status.Code(err))
			},
		},
		{
			name: "Canceled",
			testf: func(t *testing.T, addr string, lc snpb.LogIOClient) {
				lss, lastGLSN := TestSealLogStreamReplica(t, cid, snid, tpid, lsid, types.InvalidGLSN, addr)
				require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
				require.True(t, lastGLSN.Invalid())

				TestUnsealLogStreamReplica(t, cid, snid, tpid, lsid, []varlogpb.LogStreamReplica{
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
				}, addr)

				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				defer func() {
					cancel()
					wg.Wait()
				}()
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := lc.Append(ctx, &snpb.AppendRequest{
						TopicID:     tpid,
						LogStreamID: lsid,
						Payload:     payload,
					})
					assert.Error(t, err)
					assert.Equal(t, codes.Canceled, status.Code(err))
				}()
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				err := sn.Close()
				require.NoError(t, err)
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)

			mc, mcClose := TestNewManagementClient(t, cid, snid, addr)
			defer mcClose()

			_, err := mc.AddLogStreamReplica(context.Background(), tpid, lsid, sn.snPaths[0])
			require.NoError(t, err)

			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rpcConn.Close())
			}()
			lc := snpb.NewLogIOClient(rpcConn.Conn)

			tc.testf(t, addr, lc)
		})
	}
}

func TestStorageNode_Subscribe(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)
	payload := [][]byte{[]byte("foo"), []byte("bar")}

	tcs := []struct {
		name  string
		testf func(t *testing.T, addr string, lc snpb.LogIOClient)
	}{
		{
			name: "InvalidTopicID",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				const invalidTopicID = types.TopicID(0)
				stream, err := lc.Subscribe(context.Background(), &snpb.SubscribeRequest{
					TopicID:     invalidTopicID,
					LogStreamID: lsid,
					GLSNBegin:   1,
					GLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "InvalidLogStreamID",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				const invalidLogStreamID = types.LogStreamID(0)
				stream, err := lc.Subscribe(context.Background(), &snpb.SubscribeRequest{
					TopicID:     tpid,
					LogStreamID: invalidLogStreamID,
					GLSNBegin:   1,
					GLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NoSuchTopic",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				stream, err := lc.Subscribe(context.Background(), &snpb.SubscribeRequest{
					TopicID:     tpid + 1,
					LogStreamID: lsid,
					GLSNBegin:   1,
					GLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "NoSuchLogStream",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				stream, err := lc.Subscribe(context.Background(), &snpb.SubscribeRequest{
					TopicID:     tpid,
					LogStreamID: lsid + 1,
					GLSNBegin:   1,
					GLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "InvalidRange",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				stream, err := lc.Subscribe(context.Background(), &snpb.SubscribeRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					GLSNBegin:   1,
					GLSNEnd:     1,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "Trimmed",
			testf: func(t *testing.T, addr string, lc snpb.LogIOClient) {
				ret := TestTrim(t, cid, snid, tpid, 1, addr)
				require.Len(t, ret, 1)
				require.Contains(t, ret, lsid)
				require.NoError(t, ret[lsid])

				stream, err := lc.Subscribe(context.Background(), &snpb.SubscribeRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					GLSNBegin:   1,
					GLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.OutOfRange, status.Code(err))
			},
		},
		{
			name: "DeadlineExceeded",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()

				stream, err := lc.Subscribe(ctx, &snpb.SubscribeRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					GLSNBegin:   1,
					GLSNEnd:     4,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				require.Eventually(t, func() bool {
					_, err := stream.Recv()
					return err != nil && status.Code(err) == codes.DeadlineExceeded
				}, time.Second, 10*time.Millisecond)
			},
		},
		{
			name: "Canceled",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)

				stream, err := lc.Subscribe(ctx, &snpb.SubscribeRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					GLSNBegin:   1,
					GLSNEnd:     4,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				var wg sync.WaitGroup
				defer wg.Wait()
				wg.Add(1)
				go func() {
					defer wg.Done()
					cancel()
				}()

				require.Eventually(t, func() bool {
					_, err := stream.Recv()
					return err != nil && status.Code(err) == codes.Canceled
				}, time.Second, 10*time.Millisecond)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				require.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)

			mc, mcClose := TestNewManagementClient(t, cid, snid, addr)
			defer mcClose()

			_, err := mc.AddLogStreamReplica(context.Background(), tpid, lsid, sn.snPaths[0])
			require.NoError(t, err)

			lss, lastGLSN := TestSealLogStreamReplica(t, cid, snid, tpid, lsid, types.InvalidGLSN, addr)
			require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
			require.True(t, lastGLSN.Invalid())

			TestUnsealLogStreamReplica(t, cid, snid, tpid, lsid, []varlogpb.LogStreamReplica{
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
						Address:       addr,
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
			}, addr)

			var appendWg sync.WaitGroup
			appendWg.Add(2)
			go func() {
				defer appendWg.Done()
				res := TestAppend(t, tpid, lsid, payload, []varlogpb.LogStreamReplica{
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
				})
				assert.Len(t, res, len(payload))
			}()
			go func() {
				defer appendWg.Done()
				assert.Eventually(t, func() bool {
					reportcommitter.TestCommit(t, addr, snpb.CommitRequest{
						StorageNodeID: snid,
						CommitResult: snpb.LogStreamCommitResult{
							TopicID:             tpid,
							LogStreamID:         lsid,
							CommittedLLSNOffset: 1,
							CommittedGLSNOffset: 1,
							CommittedGLSNLength: 2,
							Version:             1,
							HighWatermark:       2,
						},
					})
					reports := reportcommitter.TestGetReport(t, addr)
					assert.Len(t, reports, 1)
					return reports[0].Version == types.Version(1)
				}, time.Second, 10*time.Millisecond)
			}()
			appendWg.Wait()

			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rpcConn.Close())
			}()
			lc := snpb.NewLogIOClient(rpcConn.Conn)

			tc.testf(t, addr, lc)
		})
	}
}

func TestStorageNode_SubscribeTo(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)
	payload := [][]byte{[]byte("foo"), []byte("bar")}

	tcs := []struct {
		name  string
		testf func(t *testing.T, addr string, lc snpb.LogIOClient)
	}{
		{
			name: "InvalidTopicID",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				const invalidTopicID = types.TopicID(0)
				stream, err := lc.SubscribeTo(context.Background(), &snpb.SubscribeToRequest{
					TopicID:     invalidTopicID,
					LogStreamID: lsid,
					LLSNBegin:   1,
					LLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "InvalidLogStreamID",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				const invalidLogStreamID = types.LogStreamID(0)
				stream, err := lc.SubscribeTo(context.Background(), &snpb.SubscribeToRequest{
					TopicID:     tpid,
					LogStreamID: invalidLogStreamID,
					LLSNBegin:   1,
					LLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NoSuchTopic",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				stream, err := lc.SubscribeTo(context.Background(), &snpb.SubscribeToRequest{
					TopicID:     tpid + 1,
					LogStreamID: lsid,
					LLSNBegin:   1,
					LLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "NoSuchLogStream",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				stream, err := lc.SubscribeTo(context.Background(), &snpb.SubscribeToRequest{
					TopicID:     tpid,
					LogStreamID: lsid + 1,
					LLSNBegin:   1,
					LLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "InvalidRange",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				stream, err := lc.SubscribeTo(context.Background(), &snpb.SubscribeToRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					LLSNBegin:   1,
					LLSNEnd:     1,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "Trimmed",
			testf: func(t *testing.T, addr string, lc snpb.LogIOClient) {
				ret := TestTrim(t, cid, snid, tpid, 1, addr)
				require.Len(t, ret, 1)
				require.Contains(t, ret, lsid)
				require.NoError(t, ret[lsid])

				stream, err := lc.SubscribeTo(context.Background(), &snpb.SubscribeToRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					LLSNBegin:   1,
					LLSNEnd:     3,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				_, err = stream.Recv()
				require.Error(t, err)
				require.Equal(t, codes.OutOfRange, status.Code(err))
			},
		},
		{
			name: "DeadlineExceeded",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()

				stream, err := lc.SubscribeTo(ctx, &snpb.SubscribeToRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					LLSNBegin:   1,
					LLSNEnd:     4,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				require.Eventually(t, func() bool {
					_, err := stream.Recv()
					return err != nil && status.Code(err) == codes.DeadlineExceeded
				}, time.Second, 10*time.Millisecond)
			},
		},
		{
			name: "Canceled",
			testf: func(t *testing.T, _ string, lc snpb.LogIOClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)

				stream, err := lc.SubscribeTo(ctx, &snpb.SubscribeToRequest{
					TopicID:     tpid,
					LogStreamID: lsid,
					LLSNBegin:   1,
					LLSNEnd:     4,
				})
				require.NoError(t, err)
				defer func() {
					require.NoError(t, stream.CloseSend())
				}()

				var wg sync.WaitGroup
				defer wg.Wait()
				wg.Add(1)
				go func() {
					defer wg.Done()
					cancel()
				}()

				require.Eventually(t, func() bool {
					_, err := stream.Recv()
					return err != nil && status.Code(err) == codes.Canceled
				}, time.Second, 10*time.Millisecond)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				require.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)

			mc, mcClose := TestNewManagementClient(t, cid, snid, addr)
			defer mcClose()

			_, err := mc.AddLogStreamReplica(context.Background(), tpid, lsid, sn.snPaths[0])
			require.NoError(t, err)

			lss, lastGLSN := TestSealLogStreamReplica(t, cid, snid, tpid, lsid, types.InvalidGLSN, addr)
			require.Equal(t, varlogpb.LogStreamStatusSealed, lss)
			require.True(t, lastGLSN.Invalid())

			TestUnsealLogStreamReplica(t, cid, snid, tpid, lsid, []varlogpb.LogStreamReplica{
				{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snid,
						Address:       addr,
					},
					TopicLogStream: varlogpb.TopicLogStream{
						TopicID:     tpid,
						LogStreamID: lsid,
					},
				},
			}, addr)

			var appendWg sync.WaitGroup
			appendWg.Add(2)
			go func() {
				defer appendWg.Done()
				res := TestAppend(t, tpid, lsid, payload, []varlogpb.LogStreamReplica{
					{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
							Address:       addr,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
				})
				assert.Len(t, res, len(payload))
			}()
			go func() {
				defer appendWg.Done()
				assert.Eventually(t, func() bool {
					reportcommitter.TestCommit(t, addr, snpb.CommitRequest{
						StorageNodeID: snid,
						CommitResult: snpb.LogStreamCommitResult{
							TopicID:             tpid,
							LogStreamID:         lsid,
							CommittedLLSNOffset: 1,
							CommittedGLSNOffset: 1,
							CommittedGLSNLength: 2,
							Version:             1,
							HighWatermark:       2,
						},
					})
					reports := reportcommitter.TestGetReport(t, addr)
					assert.Len(t, reports, 1)
					return reports[0].Version == types.Version(1)
				}, time.Second, 10*time.Millisecond)
			}()
			appendWg.Wait()

			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rpcConn.Close())
			}()
			lc := snpb.NewLogIOClient(rpcConn.Conn)

			tc.testf(t, addr, lc)
		})
	}
}

func TestStorageNode_LogStreamReplicaMetadata(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)

	tcs := []struct {
		name  string
		testf func(t *testing.T, lc snpb.LogIOClient)
	}{
		{
			name: "InvalidTopicID",
			testf: func(t *testing.T, lc snpb.LogIOClient) {
				const invalidTopicID = types.TopicID(0)
				_, err := lc.LogStreamReplicaMetadata(context.Background(), &snpb.LogStreamReplicaMetadataRequest{
					TopicID:     invalidTopicID,
					LogStreamID: lsid,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "InvalidLogStreamID",
			testf: func(t *testing.T, lc snpb.LogIOClient) {
				const invalidLogStreamID = types.LogStreamID(0)
				_, err := lc.LogStreamReplicaMetadata(context.Background(), &snpb.LogStreamReplicaMetadataRequest{
					TopicID:     tpid,
					LogStreamID: invalidLogStreamID,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NoSuchTopic",
			testf: func(t *testing.T, lc snpb.LogIOClient) {
				_, err := lc.LogStreamReplicaMetadata(context.Background(), &snpb.LogStreamReplicaMetadataRequest{
					TopicID:     tpid + 1,
					LogStreamID: lsid,
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
		{
			name: "NoSuchLogStream",
			testf: func(t *testing.T, lc snpb.LogIOClient) {
				_, err := lc.LogStreamReplicaMetadata(context.Background(), &snpb.LogStreamReplicaMetadataRequest{
					TopicID:     tpid,
					LogStreamID: lsid + 1,
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				require.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)

			mc, mcClose := TestNewManagementClient(t, cid, snid, addr)
			defer mcClose()

			_, err := mc.AddLogStreamReplica(context.Background(), tpid, lsid, sn.snPaths[0])
			require.NoError(t, err)

			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rpcConn.Close())
			}()
			lc := snpb.NewLogIOClient(rpcConn.Conn)

			tc.testf(t, lc)
		})
	}
}

func TestStorageNode_GetMetadata(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)

	tcs := []struct {
		name  string
		testf func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient)
	}{
		{
			name: "NoLogStreamReplica",
			testf: func(t *testing.T, _ *StorageNode, mc snpb.ManagementClient) {
				rsp, err := mc.GetMetadata(context.Background(), &snpb.GetMetadataRequest{
					ClusterID: cid,
				})
				require.NoError(t, err)

				snmd := rsp.StorageNodeMetadata
				require.Equal(t, cid, snmd.ClusterID)
				require.Equal(t, snid, snmd.StorageNodeID)
				require.NotEmpty(t, snmd.Storages)
				require.Empty(t, snmd.LogStreamReplicas)
				require.NotZero(t, snmd.StartTime)
			},
		},
		{
			name: "LogStreamReplica",
			testf: func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient) {
				ctx := context.Background()
				_, err := mc.AddLogStreamReplica(ctx, &snpb.AddLogStreamReplicaRequest{
					ClusterID:       cid,
					StorageNodeID:   snid,
					TopicID:         tpid,
					LogStreamID:     lsid,
					StorageNodePath: sn.snPaths[0],
				})
				require.NoError(t, err)

				rsp, err := mc.GetMetadata(ctx, &snpb.GetMetadataRequest{
					ClusterID: cid,
				})
				require.NoError(t, err)
				snmd := rsp.StorageNodeMetadata
				require.Len(t, snmd.LogStreamReplicas, 1)
				require.Equal(t, tpid, snmd.LogStreamReplicas[0].TopicID)
				require.Equal(t, lsid, snmd.LogStreamReplicas[0].LogStreamID)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				require.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)
			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				err := rpcConn.Close()
				require.NoError(t, err)
			}()
			mc := snpb.NewManagementClient(rpcConn.Conn)

			tc.testf(t, sn, mc)
		})
	}
}

func TestStorageNode_RemoveLogStreamReplica(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)

	tcs := []struct {
		name  string
		testf func(t *testing.T, snpath string, mc snpb.ManagementClient)
	}{
		{
			name: "Succeed",
			testf: func(t *testing.T, snpath string, mc snpb.ManagementClient) {
				ctx := context.Background()

				rsp, err := mc.AddLogStreamReplica(ctx, &snpb.AddLogStreamReplicaRequest{
					ClusterID:       cid,
					StorageNodeID:   snid,
					TopicID:         tpid,
					LogStreamID:     lsid,
					StorageNodePath: snpath,
				})
				require.NoError(t, err)

				_, err = os.ReadDir(rsp.LogStreamReplica.Path)
				require.NoError(t, err)

				_, err = mc.RemoveLogStream(ctx, &snpb.RemoveLogStreamRequest{
					ClusterID:     cid,
					StorageNodeID: snid,
					TopicID:       tpid,
					LogStreamID:   lsid,
				})
				require.NoError(t, err)
				_, err = os.ReadDir(rsp.LogStreamReplica.Path)
				require.ErrorIs(t, err, fs.ErrNotExist)
			},
		},
		{
			name: "NotFound",
			testf: func(t *testing.T, _ string, mc snpb.ManagementClient) {
				_, err := mc.RemoveLogStream(context.Background(), &snpb.RemoveLogStreamRequest{
					ClusterID:     cid,
					StorageNodeID: snid,
					TopicID:       tpid,
					LogStreamID:   lsid,
				})
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				assert.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)
			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				err := rpcConn.Close()
				require.NoError(t, err)
			}()
			mc := snpb.NewManagementClient(rpcConn.Conn)

			tc.testf(t, sn.snPaths[0], mc)
		})
	}
}

func TestStorageNode_Report(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)
		snid = types.StorageNodeID(1)
	)

	tcs := []struct {
		name  string
		testf func(t *testing.T, addr string)
	}{
		{
			name: "Succeed",
			testf: func(t *testing.T, addr string) {
				reports := reportcommitter.TestGetReport(t, addr)
				require.Len(t, reports, 1)
				require.Equal(t, lsid, reports[0].LogStreamID)
				require.EqualValues(t, 1, reports[0].UncommittedLLSNOffset)
				require.Zero(t, reports[0].UncommittedLLSNLength)
			},
		},
		{
			name: "Learning",
			testf: func(t *testing.T, addr string) {
				rc, rcClose := logstream.TestNewReplicatorClient(t, addr)
				defer rcClose()

				_, err := rc.SyncInit(context.Background(), &snpb.SyncInitRequest{
					ClusterID: cid,
					Source: varlogpb.LogStreamReplica{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid + 1,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
					Destination: varlogpb.LogStreamReplica{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snid,
						},
						TopicLogStream: varlogpb.TopicLogStream{
							TopicID:     tpid,
							LogStreamID: lsid,
						},
					},
					Range: snpb.SyncRange{
						FirstLLSN: 1,
						LastLLSN:  10,
					},
				})
				require.NoError(t, err)

				reports := reportcommitter.TestGetReport(t, addr)
				require.Empty(t, reports)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t,
				WithDefaultLogStreamExecutorOptions(
					logstream.WithSyncTimeout(time.Minute),
				),
			)

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()

			addr := TestGetAdvertiseAddress(t, sn)

			mc, mcClose := TestNewManagementClient(t, sn.cid, sn.snid, addr)
			defer mcClose()

			_, err := mc.AddLogStreamReplica(context.Background(), tpid, lsid, sn.snPaths[0])
			require.NoError(t, err)
		})
	}
}

func TestStorageNode_Sync(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		tpid = types.TopicID(1)
		lsid = types.LogStreamID(1)

		syncTimeout = time.Minute
	)

	makeReplicas := func(sn *StorageNode) []varlogpb.LogStreamReplica {
		return []varlogpb.LogStreamReplica{
			{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: sn.snid,
					Address:       sn.advertise,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     tpid,
					LogStreamID: lsid,
				},
			},
		}
	}

	// ver: +-1-+ +-2-+ +-3-+ ...
	// lsn:  1 2   3 4   5 6  ...
	lastGLSN := func(v types.Version) types.GLSN {
		return types.GLSN(v * 2)
	}
	put := func(t *testing.T, sn *StorageNode, targetVer types.Version) {
		var wg sync.WaitGroup
		defer wg.Wait()

		replicas := makeReplicas(sn)

		for v := 1; types.Version(v) <= targetVer; v++ {
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res := TestAppend(t, tpid, lsid, [][]byte{[]byte("foo")}, replicas)
					assert.Len(t, res, 1)
				}()
			}
			require.Eventually(t, func() bool {
				reportcommitter.TestCommit(t, sn.advertise, snpb.CommitRequest{
					StorageNodeID: sn.snid,
					CommitResult: snpb.LogStreamCommitResult{
						TopicID:             tpid,
						LogStreamID:         lsid,
						CommittedLLSNOffset: types.LLSN(2*v - 1),
						CommittedGLSNOffset: types.GLSN(2*v - 1),
						CommittedGLSNLength: 2,
						Version:             types.Version(v),
						HighWatermark:       lastGLSN(types.Version(v)),
					},
				})
				reports := reportcommitter.TestGetReport(t, sn.advertise)
				assert.Len(t, reports, 1)
				return reports[0].Version == types.Version(v)
			}, time.Second, 10*time.Millisecond)
		}
	}
	trim := func(t *testing.T, sn *StorageNode, trimGLSN types.GLSN) {
		ret := TestTrim(t, cid, sn.snid, tpid, trimGLSN, sn.advertise)
		require.Len(t, ret, 1)
		require.Contains(t, ret, lsid)
		require.NoError(t, ret[lsid])
	}

	tcs := []struct {
		name  string
		testf func(t *testing.T, src, dst *StorageNode)
	}{
		{
			// ver: +-1-+
			// src:  1 2  <no commit context>
			// dst:
			name: "NoCommitContext",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(1)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)
				lse, ok := src.executors.Load(tpid, lsid)
				require.True(t, ok)
				stg := logstream.TestGetStorage(t, lse)
				storage.TestDeleteCommitContext(t, stg)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, types.InvalidGLSN, localHWM)

				snmc, closer := TestNewManagementClient(t, cid, src.snid, src.advertise)
				defer closer()

				st, err := snmc.Sync(context.Background(), tpid, lsid, dst.snid, dst.advertise, types.InvalidGLSN /*unused*/)
				require.NoError(t, err)
				require.Equal(t, snpb.SyncStateStart, st.State)

				require.Never(t, func() bool {
					st, err := snmc.Sync(context.Background(), tpid, lsid, dst.snid, dst.advertise, types.InvalidGLSN /*unused*/)
					return err == nil && st.State == snpb.SyncStateComplete
				}, 1500*time.Millisecond, 100*time.Millisecond)
			},
		},
		{
			// ver: +-1-+
			// src:  1 2  <invalid commit context>
			// dst:
			name: "InvalidCommitContext",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(1)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)
				lse, ok := src.executors.Load(tpid, lsid)
				require.True(t, ok)
				stg := logstream.TestGetStorage(t, lse)
				storage.TestSetCommitContext(t, stg, storage.CommitContext{
					Version:            1,
					HighWatermark:      3,
					CommittedGLSNBegin: 1,
					CommittedGLSNEnd:   4,
					CommittedLLSNBegin: 1,
				})

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, types.InvalidGLSN, localHWM)

				snmc, closer := TestNewManagementClient(t, cid, src.snid, src.advertise)
				defer closer()

				st, err := snmc.Sync(context.Background(), tpid, lsid, dst.snid, dst.advertise, types.InvalidGLSN /*unused*/)
				require.NoError(t, err)
				require.Equal(t, snpb.SyncStateStart, st.State)

				require.Never(t, func() bool {
					st, err := snmc.Sync(context.Background(), tpid, lsid, dst.snid, dst.advertise, types.InvalidGLSN /*unused*/)
					return err == nil && st.State == snpb.SyncStateComplete
				}, 1500*time.Millisecond, 100*time.Millisecond)
			},
		},
		{
			// ver: +-1-+ +-2-+
			// lsn:  1 2     4
			// dst:
			name: "IncorrectLogEntry",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(2)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)
				lse, ok := src.executors.Load(tpid, lsid)
				require.True(t, ok)
				stg := logstream.TestGetStorage(t, lse)
				storage.TestDeleteLogEntry(t, stg, varlogpb.LogSequenceNumber{
					LLSN: 3,
					GLSN: 3,
				})

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, types.InvalidGLSN, localHWM)

				snmc, closer := TestNewManagementClient(t, cid, src.snid, src.advertise)
				defer closer()

				st, err := snmc.Sync(context.Background(), tpid, lsid, dst.snid, dst.advertise, types.InvalidGLSN /*unused*/)
				require.NoError(t, err)
				require.Equal(t, snpb.SyncStateStart, st.State)

				require.Never(t, func() bool {
					st, err := snmc.Sync(context.Background(), tpid, lsid, dst.snid, dst.advertise, types.InvalidGLSN /*unused*/)
					return err == nil && st.State == snpb.SyncStateComplete
				}, 1500*time.Millisecond, 100*time.Millisecond)
			},
		},
		{
			// ver: +-1-+
			// src:  1 2
			// dst:
			name: "CopyAllFromStart",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(1)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, types.InvalidGLSN, localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 10*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 1, GLSN: 1,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 2, GLSN: 2,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
		{
			// ver: +-1-+ +-2-+
			// src:        3 4
			// dst:
			name: "CopyAllFromMiddle",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(2)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				trim(t, src, 2)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, types.InvalidGLSN, localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 10*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 3, GLSN: 3,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 4, GLSN: 4,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
		{
			// ver: +-1-+
			// src:  1 2
			// dst:  1 2  <no commit context>
			name: "CopyOnlyCommitContext",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(1)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				var wg sync.WaitGroup
				defer func() {
					wg.Wait()
				}()
				vol := t.TempDir()
				dst = TestNewSimpleStorageNode(t,
					WithClusterID(cid),
					WithStorageNodeID(dst.snid+1),
					WithVolumes(vol),
				)
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = dst.Serve()
				}()
				TestWaitForStartingOfServe(t, dst)
				TestAddLogStreamReplica(t, cid, dst.snid, tpid, lsid, dst.snPaths[0], dst.advertise)
				lse, ok := dst.executors.Load(tpid, lsid)
				require.True(t, ok)
				stg := logstream.TestGetStorage(t, lse)
				storage.TestAppendLogEntryWithoutCommitContext(t, stg, 1, 1, []byte("foo"))
				storage.TestAppendLogEntryWithoutCommitContext(t, stg, 2, 2, []byte("foo"))
				_ = dst.Close()

				dst = TestNewSimpleStorageNode(t,
					WithClusterID(cid),
					WithStorageNodeID(dst.snid),
					WithVolumes(vol),
				)
				defer func() {
					_ = dst.Close()
				}()
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = dst.Serve()
				}()
				TestWaitForStartingOfServe(t, dst)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 3*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 1, GLSN: 1,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 2, GLSN: 2,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
		{
			// ver: +-1-+ +-2-+
			// src:        3 4
			// dst:  1 2
			name: "TrimAllAndCopy",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(2)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				trim(t, src, 2)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				put(t, dst, 1)
				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, lastGLSN(1), localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 10*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 3, GLSN: 3,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 4, GLSN: 4,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
		{
			// ver: +-1-+ +-2-+
			// src:          4
			// dst:  1 2
			name: "TrimAllAndCopy",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(2)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				trim(t, src, 3)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				put(t, dst, 1)
				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, lastGLSN(1), localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 10*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 4, GLSN: 4,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 4, GLSN: 4,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
		{
			// ver: +-1-+ +-2-+ +-3-+
			// src:        3 4   5 6
			// dst:  1 2   3 4
			// https://github.com/kakao/varlog/pull/199#discussion_r1011908030
			name: "TrimSomeAndCopy",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(3)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				trim(t, src, 2)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				put(t, dst, 2)
				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, lastGLSN(2), localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 10*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 3, GLSN: 3,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 6, GLSN: 6,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
		{
			// ver: +-1-+ +-2-+ +-3-+
			// src:    2   3 4   5 6
			// dst:        3 4
			name: "FixTrimAllAndCopy",
			testf: func(t *testing.T, src, dst *StorageNode) {
				const ver = types.Version(3)
				lastCommittedGLSN := lastGLSN(ver)

				put(t, src, ver)
				trim(t, src, 1)
				status, localHWM := TestSealLogStreamReplica(t, cid, src.snid, tpid, lsid, lastCommittedGLSN, src.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				put(t, dst, 2)
				trim(t, dst, 2)
				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealing, status)
				require.Equal(t, lastGLSN(2), localHWM)

				syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
					StorageNodeID: dst.snid,
					Address:       dst.advertise,
				})
				require.Equal(t, snpb.SyncStateStart, syncStatus.State)

				require.Eventually(t, func() bool {
					syncStatus := TestSync(t, cid, src.snid, tpid, lsid, 0 /*unused*/, src.advertise, varlogpb.StorageNode{
						StorageNodeID: dst.snid,
						Address:       dst.advertise,
					})
					return syncStatus.State == snpb.SyncStateComplete
				}, 10*time.Second, 100*time.Millisecond)

				status, localHWM = TestSealLogStreamReplica(t, cid, dst.snid, tpid, lsid, lastCommittedGLSN, dst.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, lastCommittedGLSN, localHWM)

				snmd, err := dst.getMetadata(context.Background())
				require.NoError(t, err)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 2, GLSN: 2,
				}, snmd.LogStreamReplicas[0].LocalLowWatermark)
				require.Equal(t, varlogpb.LogSequenceNumber{
					LLSN: 6, GLSN: 6,
				}, snmd.LogStreamReplicas[0].LocalHighWatermark)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			defer func() {
				wg.Wait()
			}()
			nodes := make([]*StorageNode, 2)
			for i := range nodes {
				sn := TestNewSimpleStorageNode(t,
					WithClusterID(cid),
					WithStorageNodeID(types.StorageNodeID(i+1)),
					WithDefaultLogStreamExecutorOptions(
						logstream.WithSyncTimeout(syncTimeout),
					),
				)
				nodes[i] = sn
			}
			defer func() {
				for _, sn := range nodes {
					_ = sn.Close()
				}
			}()
			for i := range nodes {
				wg.Add(1)
				sn := nodes[i]
				go func() {
					defer wg.Done()
					_ = sn.Serve()
				}()
				TestWaitForStartingOfServe(t, sn)

				TestAddLogStreamReplica(t, cid, sn.snid, tpid, lsid, sn.snPaths[0], sn.advertise)

				status, localHWM := TestSealLogStreamReplica(t, cid, sn.snid, tpid, lsid, types.InvalidGLSN, sn.advertise)
				require.Equal(t, varlogpb.LogStreamStatusSealed, status)
				require.Equal(t, types.InvalidGLSN, localHWM)

				TestUnsealLogStreamReplica(t, cid, sn.snid, tpid, lsid, makeReplicas(sn), sn.advertise)
			}

			tc.testf(t, nodes[0], nodes[1])
		})
	}
}

func TestStorageNode_AddLogStreamReplica(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name                      string
		maxLogStreamReplicasCount int32
		testf                     func(t *testing.T, snpath string, mc *client.ManagementClient)
	}{
		{
			name:                      "LimitOne",
			maxLogStreamReplicasCount: 1,
			testf: func(t *testing.T, snpath string, mc *client.ManagementClient) {
				_, err := mc.AddLogStreamReplica(ctx, 1, 1, snpath)
				require.NoError(t, err)

				_, err = mc.AddLogStreamReplica(ctx, 1, 2, snpath)
				require.Error(t, err)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))

				err = mc.RemoveLogStream(ctx, 1, 1)
				require.NoError(t, err)

				_, err = mc.AddLogStreamReplica(ctx, 1, 2, snpath)
				require.NoError(t, err)
			},
		},
		{
			name:                      "LimitZero",
			maxLogStreamReplicasCount: 0,
			testf: func(t *testing.T, snpath string, mc *client.ManagementClient) {
				_, err := mc.AddLogStreamReplica(ctx, 1, 1, snpath)
				require.Error(t, err)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithMaxLogStreamReplicasCount(tc.maxLogStreamReplicasCount))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				assert.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)
			mc, mcClose := TestNewManagementClient(t, sn.cid, sn.snid, addr)
			defer mcClose()

			tc.testf(t, sn.snPaths[0], mc)
		})
	}
}

func TestStorageNode_Seal(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)

	tcs := []struct {
		testf func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient)
		name  string
	}{
		{
			name: "InvalidTopicID",
			testf: func(t *testing.T, _ *StorageNode, mc snpb.ManagementClient) {
				const invalidTopicID = types.TopicID(0)

				_, err := mc.Seal(context.Background(), &snpb.SealRequest{
					ClusterID:         cid,
					StorageNodeID:     snid,
					TopicID:           invalidTopicID,
					LogStreamID:       lsid,
					LastCommittedGLSN: types.InvalidGLSN,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "InvalidLogStreamID",
			testf: func(t *testing.T, _ *StorageNode, mc snpb.ManagementClient) {
				const invalidLogStreamID = types.LogStreamID(0)

				_, err := mc.Seal(context.Background(), &snpb.SealRequest{
					ClusterID:         cid,
					StorageNodeID:     snid,
					TopicID:           tpid,
					LogStreamID:       invalidLogStreamID,
					LastCommittedGLSN: types.InvalidGLSN,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NotFound",
			testf: func(t *testing.T, _ *StorageNode, mc snpb.ManagementClient) {
				_, err := mc.Seal(context.Background(), &snpb.SealRequest{
					ClusterID:         cid,
					StorageNodeID:     snid,
					TopicID:           tpid,
					LogStreamID:       lsid,
					LastCommittedGLSN: types.InvalidGLSN,
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				assert.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)
			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				err := rpcConn.Close()
				require.NoError(t, err)
			}()
			mc := snpb.NewManagementClient(rpcConn.Conn)

			tc.testf(t, sn, mc)
		})
	}
}

func TestStorageNode_Unseal(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
		tpid = types.TopicID(3)
		lsid = types.LogStreamID(4)
	)

	tcs := []struct {
		testf func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient)
		name  string
	}{
		{
			name: "InvalidTopicID",
			testf: func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient) {
				const invalidTopicID = types.TopicID(0)

				_, err := mc.Unseal(context.Background(), &snpb.UnsealRequest{
					ClusterID:     cid,
					StorageNodeID: snid,
					TopicID:       invalidTopicID,
					LogStreamID:   lsid,
					Replicas: []varlogpb.LogStreamReplica{
						{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
								Address:       sn.advertise,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     invalidTopicID,
								LogStreamID: lsid,
							},
						},
					},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "InvalidLogStreamID",
			testf: func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient) {
				const invalidLogStreamID = types.LogStreamID(0)

				_, err := mc.Unseal(context.Background(), &snpb.UnsealRequest{
					ClusterID:     cid,
					StorageNodeID: snid,
					TopicID:       tpid,
					LogStreamID:   invalidLogStreamID,
					Replicas: []varlogpb.LogStreamReplica{
						{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
								Address:       sn.advertise,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     tpid,
								LogStreamID: invalidLogStreamID,
							},
						},
					},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NotFound",
			testf: func(t *testing.T, sn *StorageNode, mc snpb.ManagementClient) {
				_, err := mc.Unseal(context.Background(), &snpb.UnsealRequest{
					ClusterID:     cid,
					StorageNodeID: snid,
					TopicID:       tpid,
					LogStreamID:   lsid,
					Replicas: []varlogpb.LogStreamReplica{
						{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snid,
								Address:       sn.advertise,
							},
							TopicLogStream: varlogpb.TopicLogStream{
								TopicID:     tpid,
								LogStreamID: lsid,
							},
						},
					},
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			sn := TestNewSimpleStorageNode(t, WithClusterID(cid), WithStorageNodeID(snid))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = sn.Serve()
			}()
			defer func() {
				assert.NoError(t, sn.Close())
				wg.Wait()
			}()

			addr := TestGetAdvertiseAddress(t, sn)
			rpcConn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			defer func() {
				err := rpcConn.Close()
				require.NoError(t, err)
			}()
			mc := snpb.NewManagementClient(rpcConn.Conn)

			tc.testf(t, sn, mc)
		})
	}
}
