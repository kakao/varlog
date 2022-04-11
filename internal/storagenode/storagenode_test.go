package storagenode

import (
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.daumkakao.com/varlog/varlog/internal/reportcommitter"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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
	snmd1 := TestGetStorageNodeMetadataDescriptor(t, cid, sn1.advertise)
	assert.Equal(t, snid1, snmd1.StorageNode.StorageNodeID)
	assert.NotEmpty(t, snmd1.StorageNode.Storages)
	assert.NotEmpty(t, snmd1.StorageNode.Storages[0].Path)
	// sn1: add ls
	TestAddLogStreamReplica(t, cid, tpid, lsid, snmd1.StorageNode.Storages[0].Path, sn1.advertise)

	// sn2: get path
	snmd2 := TestGetStorageNodeMetadataDescriptor(t, cid, sn2.advertise)
	assert.Equal(t, snid2, snmd2.StorageNode.StorageNodeID)
	assert.NotEmpty(t, snmd2.StorageNode.Storages)
	assert.NotEmpty(t, snmd2.StorageNode.Storages[0].Path)
	// sn2: add ls
	TestAddLogStreamReplica(t, cid, tpid, lsid, snmd2.StorageNode.Storages[0].Path, sn2.advertise)

	// sn1: seal & unseal
	lss, lastCommittedGLSN := TestSealLogStreamReplica(t, cid, tpid, lsid, types.InvalidGLSN, sn1.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss)
	assert.Equal(t, types.InvalidGLSN, lastCommittedGLSN)
	TestUnsealLogStreamReplica(t, cid, tpid, lsid, replicas, sn1.advertise)
	// sn2: seal & unseal
	lss, lastCommittedGLSN = TestSealLogStreamReplica(t, cid, tpid, lsid, types.InvalidGLSN, sn2.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss)
	assert.Equal(t, types.InvalidGLSN, lastCommittedGLSN)
	TestUnsealLogStreamReplica(t, cid, tpid, lsid, replicas, sn2.advertise)

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
	les1 := TestSubscribe(t, tpid, lsid, types.MinGLSN, types.GLSN(lastGLSN)+1, sn1.advertise)
	les2 := TestSubscribe(t, tpid, lsid, types.MinGLSN, types.GLSN(lastGLSN)+1, sn2.advertise)
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
	les1 = TestSubscribeTo(t, tpid, lsid, types.MinLLSN, types.LLSN(lastLLSN)+1, sn1.advertise)
	les2 = TestSubscribeTo(t, tpid, lsid, types.MinLLSN, types.LLSN(lastLLSN)+1, sn2.advertise)
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
	les1 = TestSubscribe(t, tpid, lsid, types.MinGLSN, types.GLSN(lastGLSN)+1, sn1.advertise)
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
	lss, lastCommittedGLSN = TestSealLogStreamReplica(t, cid, tpid, lsid, lastGLSN, sn1.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealed, lss)
	assert.Equal(t, lastGLSN, lastCommittedGLSN)
	lss, lastCommittedGLSN = TestSealLogStreamReplica(t, cid, tpid, lsid, lastGLSN, sn2.advertise)
	assert.Equal(t, varlogpb.LogStreamStatusSealing, lss)
	assert.Equal(t, types.GLSN(numLogs), lastCommittedGLSN)

	// sync
	assert.Eventually(t, func() bool {
		syncStatus := TestSync(t, cid, tpid, lsid, lastGLSN, sn1.advertise, varlogpb.StorageNode{
			StorageNodeID: snid2,
			Address:       sn2.advertise,
		})
		return syncStatus.State == snpb.SyncStateComplete
	}, time.Second, 10*time.Millisecond)

	// Subscribe: [1, 21)
	les2 = TestSubscribe(t, tpid, lsid, types.MinGLSN, types.GLSN(lastGLSN)+1, sn2.advertise)
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
	TestUnsealLogStreamReplica(t, cid, tpid, lsid, replicas, sn1.advertise)
	TestSealLogStreamReplica(t, cid, tpid, lsid, lastGLSN, sn2.advertise)
	TestUnsealLogStreamReplica(t, cid, tpid, lsid, replicas, sn2.advertise)

	// trim
	TestTrim(t, cid, tpid, 13, sn1.advertise)
	TestTrim(t, cid, tpid, 13, sn2.advertise)

	// CC  : +-- 1 --+ +-- 2 ---+ +---- 3 -----+ +---- 4 -----+
	// LLSN: _ _ _ _ _ _ _ _ _ __ __ __ __ 14 15 16 17 18 19 20
	// GLSN: _ _ _ _ _ _ _ _ _ __ __ __ __ 14 15 16 17 18 19 20

	// Subscribe: [14, 21)
	les1 = TestSubscribe(t, tpid, lsid, 14, types.GLSN(lastGLSN)+1, sn1.advertise)
	les2 = TestSubscribe(t, tpid, lsid, 14, types.GLSN(lastGLSN)+1, sn2.advertise)
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
	// TODO: uncomment it
	// defer goleak.VerifyNone(t)

	// bad listen address
	_, err := NewStorageNode(
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
