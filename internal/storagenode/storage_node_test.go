package storagenode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	bindAddress = "127.0.0.1:0"
	clusterID   = types.ClusterID(1)
	numSN       = 3
	logStreamID = types.LogStreamID(1)
)

// FIXME (jun): This function is good to unittest, but not good for integration test.
// Use LogStreamReporterClient!
func commitAndWait(lse LogStreamExecutor, highWatermark, prevHighWatermark, committedGLSNOffset types.GLSN, committedGLSNLength uint64) <-chan error {
	c := make(chan error, 1)
	go func() {
		defer close(c)
		for {
			status := lse.GetReport()

			// commit ok
			if status.KnownHighWatermark == highWatermark {
				c <- nil
				return
			}

			// bad lse
			if status.KnownHighWatermark > highWatermark {
				c <- errors.New("bad LSE status")
				return
			}

			// no written entry
			if status.UncommittedLLSNLength < 1 {
				time.Sleep(time.Millisecond)
				continue
			}

			// send commit
			lse.Commit(context.TODO(), CommittedLogStreamStatus{
				LogStreamID:         logStreamID,
				HighWatermark:       highWatermark,
				PrevHighWatermark:   prevHighWatermark,
				CommittedGLSNOffset: committedGLSNOffset,
				CommittedGLSNLength: committedGLSNLength,
			})
		}
	}()
	return c

}

func TestStorageNode(t *testing.T) {
	Convey("StorageNode Integration Test", t, func() {
		var snList []*StorageNode
		for i := 0; i < numSN; i++ {
			volume, err := NewVolume(t.TempDir())
			So(err, ShouldBeNil)

			opts := &Options{
				RPCOptions:               RPCOptions{RPCBindAddress: bindAddress},
				LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
				LogStreamReporterOptions: DefaultLogStreamReporterOptions,
				ClusterID:                clusterID,
				StorageNodeID:            types.StorageNodeID(i),
				StorageName:              DefaultStorageName,
				Volumes:                  map[Volume]struct{}{volume: {}},
				Verbose:                  true,
				Logger:                   zap.L(),
			}
			sn, err := NewStorageNode(opts)
			if err != nil {
				t.Fatal(err)
			}
			snList = append(snList, sn)
		}

		for _, sn := range snList {
			if err := sn.Run(); err != nil {
				t.Fatal(err)
			}
		}

		var mclList []snc.StorageNodeManagementClient
		for _, sn := range snList {
			mcl, err := snc.NewManagementClient(context.TODO(), clusterID, sn.serverAddr, zap.L())
			if err != nil {
				t.Fatal(err)
			}
			mclList = append(mclList, mcl)
		}
		defer func() {
			for _, mcl := range mclList {
				mcl.Close()
			}
		}()

		for i := range mclList {
			sn := snList[i]
			mcl := mclList[i]

			meta, err := mcl.GetMetadata(context.TODO(), snpb.MetadataTypeLogStreams)
			So(err, ShouldBeNil)
			So(meta.GetClusterID(), ShouldEqual, clusterID)
			So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
			So(meta.GetLogStreams(), ShouldHaveLength, 0)

			storages := meta.GetStorageNode().GetStorages()
			So(len(storages), ShouldBeGreaterThan, 0)

			err = mcl.AddLogStream(context.TODO(), logStreamID, storages[0].GetPath())
			So(err, ShouldBeNil)

			meta, err = mcl.GetMetadata(context.TODO(), snpb.MetadataTypeLogStreams)
			So(err, ShouldBeNil)
			So(meta.GetClusterID(), ShouldEqual, clusterID)
			So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
			So(meta.GetLogStreams(), ShouldHaveLength, 1)
			So(meta.GetLogStreams()[0].GetLogStreamID(), ShouldEqual, logStreamID)
		}

		for _, sn := range snList {
			sn.Close()
			sn.Wait()
		}
	})
}

func TestSync(t *testing.T) {
	Convey("Sync", t, func(c C) {
		const (
			clusterID         = types.ClusterID(1)
			logStreamID       = types.LogStreamID(1)
			lastCommittedGLSN = types.GLSN(10)
		)

		var snList []*StorageNode
		var storagePathList []string
		for i := 1; i <= 2; i++ {
			volume, err := NewVolume(t.TempDir())
			So(err, ShouldBeNil)

			sn, err := NewStorageNode(&Options{
				RPCOptions:               RPCOptions{RPCBindAddress: bindAddress},
				LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
				LogStreamReporterOptions: DefaultLogStreamReporterOptions,
				ClusterID:                clusterID,
				StorageNodeID:            types.StorageNodeID(i),
				StorageName:              DefaultStorageName,
				Volumes:                  map[Volume]struct{}{volume: {}},
				Verbose:                  true,
				Logger:                   zap.L(),
			})
			So(err, ShouldBeNil)
			snList = append(snList, sn)

			for snPath := range sn.storageNodePaths {
				storagePathList = append(storagePathList, snPath)
				break
			}
		}

		for _, sn := range snList {
			err := sn.Run()
			So(err, ShouldBeNil)
		}
		defer func() {
			for _, sn := range snList {
				sn.Close()
				sn.Wait()
			}
		}()

		var mclList []snc.StorageNodeManagementClient
		for i, sn := range snList {
			mcl, err := snc.NewManagementClient(context.TODO(), clusterID, sn.serverAddr, zap.L())
			So(err, ShouldBeNil)

			mclList = append(mclList, mcl)

			err = mcl.AddLogStream(context.TODO(), logStreamID, storagePathList[i])
			So(err, ShouldBeNil)
		}
		defer func() {
			for _, mcl := range mclList {
				mcl.Close()
			}
		}()

		var logclList []logc.LogIOClient
		for _, sn := range snList {
			logcl, err := logc.NewLogIOClient(sn.serverAddr)
			So(err, ShouldBeNil)

			logclList = append(logclList, logcl)
		}
		defer func() {
			for _, logcl := range logclList {
				logcl.Close()
			}
		}()

		oldSN, newSN := snList[0], snList[1]
		oldMCL, newMCL := mclList[0], mclList[1]
		oldCL, newCL := logclList[0], logclList[1]

		So(oldSN.storageNodeID, ShouldNotEqual, newSN.storageNodeID)
		So(oldSN.serverAddr, ShouldNotEqual, newSN.serverAddr)

		// Append some logs to oldSN
		for hwm := types.GLSN(1); hwm <= lastCommittedGLSN; hwm++ {
			lse, ok := oldSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)

			// Append & Commit
			expectedData := []byte(fmt.Sprintf("log-%03d", hwm))
			errC := commitAndWait(lse, hwm, hwm-1, hwm, 1)
			glsn, err := oldCL.Append(context.TODO(), logStreamID, expectedData)
			So(glsn, ShouldNotEqual, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(<-errC, ShouldBeNil)

			// Read
			ent, err := oldCL.Read(context.TODO(), logStreamID, hwm)
			So(err, ShouldBeNil)
			So(expectedData, ShouldResemble, ent.Data)
			zap.L().Sugar().Infof("READ OK - %v", hwm)
		}

		// Subscribe
		zap.L().Sugar().Infof("STEP-1")
		subC, err := oldCL.Subscribe(context.TODO(), logStreamID, types.MinGLSN, lastCommittedGLSN/2)
		So(err, ShouldBeNil)
		zap.L().Sugar().Infof("STEP-2")
		for expectedGLSN := types.MinGLSN; expectedGLSN < lastCommittedGLSN/2; expectedGLSN++ {
			sub := <-subC
			zap.L().Sugar().Infof("STEP-3")
			So(sub.Error, ShouldBeNil)
			So(sub.GLSN, ShouldEqual, expectedGLSN)
			So(sub.LLSN, ShouldEqual, types.LLSN(expectedGLSN))
		}
		So((<-subC).Error, ShouldEqual, io.EOF)
		_, more := <-subC
		So(more, ShouldBeFalse)

		subC, err = oldCL.Subscribe(context.TODO(), logStreamID, types.MinGLSN, lastCommittedGLSN+1)
		So(err, ShouldBeNil)
		for expectedGLSN := types.MinGLSN; expectedGLSN < lastCommittedGLSN+1; expectedGLSN++ {
			sub := <-subC
			So(sub.Error, ShouldBeNil)
			So(sub.GLSN, ShouldEqual, expectedGLSN)
			So(sub.LLSN, ShouldEqual, types.LLSN(expectedGLSN))
		}
		So((<-subC).Error, ShouldEqual, io.EOF)
		_, more = <-subC
		So(more, ShouldBeFalse)

		// seal oldSN => oldSN: SEALED
		status, sealedGLSN, err := oldMCL.Seal(context.TODO(), logStreamID, lastCommittedGLSN)
		So(err, ShouldBeNil)
		So(status, ShouldEqual, varlogpb.LogStreamStatusSealed)
		So(sealedGLSN, ShouldEqual, lastCommittedGLSN)

		// ERROR: oldSN (SEALED) ---> newSN (RUNNING)
		Convey("ERROR: SEALED LS -> RUNNING LS", func() {
			// check if the newSN (= destination SN) is running
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, varlogpb.LogStreamStatusRunning)

			// start sync,
			status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(status.GetState(), ShouldEqual, snpb.SyncStateInProgress)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return status.GetState() != snpb.SyncStateInProgress
			}, time.Minute)

			// eventually, it fails
			_, err = oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
		})

		// ERROR: oldSN (SEALED) ---> newSN (SEALED)
		Convey("ERROR: SEALED LS -> SEALED LS", func() {
			// check if the newSN (= destination SN) is running
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, varlogpb.LogStreamStatusRunning)

			// make newSN SEALED (seal newSN with InvalidGLSN)
			sealStatus, sealedGLSN, err := newMCL.Seal(context.TODO(), logStreamID, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(sealStatus, ShouldEqual, varlogpb.LogStreamStatusSealed)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// start sync,
			syncStatus, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(syncStatus.GetState(), ShouldEqual, snpb.SyncStateInProgress)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return status.GetState() != snpb.SyncStateInProgress
			}, time.Minute)

			// eventually, it fails
			syncStatus, err = oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(syncStatus.GetState(), ShouldEqual, snpb.SyncStateError)
		})

		// ERROR: newSN (NOT SEALED) ---> oldSN
		Convey("ERROR: NOT SEALED LS -> ANY LS", func() {
			// Syncing SN (= source SN) is LogStreamStatusRunning
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, varlogpb.LogStreamStatusRunning)

			// Sync Error
			_, err := newMCL.Sync(context.TODO(), logStreamID, oldSN.storageNodeID, oldSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldNotBeNil)

			// Syncing SN (= source SN) is LogStreamStatusSealing
			status, sealedGLSN, err := newMCL.Seal(context.TODO(), logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(status, ShouldEqual, varlogpb.LogStreamStatusSealing)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// Sync Error
			_, err = newMCL.Sync(context.TODO(), logStreamID, oldSN.storageNodeID, oldSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldNotBeNil)
		})

		// OK: oldSN (SEALED) ---> newSN (SEALING)
		Convey("OK: SEALED LS -> SEALING LS", func() {
			// newSN (SEALING)
			sealStatus, sealedGLSN, err := newMCL.Seal(context.TODO(), logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(sealStatus, ShouldEqual, varlogpb.LogStreamStatusSealing)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// sync
			_, err = oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return status.GetState() != snpb.SyncStateInProgress
			}, time.Minute)

			// check: complete
			syncStatus, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(syncStatus.GetState(), ShouldEqual, snpb.SyncStateComplete)

			// TODO (jun): To update status, we seal newSN
			// TODO (jun): In source LS, when does the status delete?
			//

			// Read the last log from OLD
			oldLogEntry, err := oldCL.Read(context.TODO(), logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)

			// Read the last log from NEW
			newLogEntry, err := newCL.Read(context.TODO(), logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)

			// check
			So(oldLogEntry, ShouldResemble, newLogEntry)
			So(oldLogEntry.GLSN, ShouldEqual, lastCommittedGLSN)
			So(oldLogEntry.LLSN, ShouldEqual, types.LLSN(lastCommittedGLSN))
		})

	})
}
