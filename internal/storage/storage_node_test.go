package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"
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
			logger, _ := zap.NewDevelopment()
			opts := &StorageNodeOptions{
				RPCOptions:               RPCOptions{RPCBindAddress: bindAddress},
				LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
				LogStreamReporterOptions: DefaultLogStreamReporterOptions,
				ClusterID:                clusterID,
				StorageNodeID:            types.StorageNodeID(i),
				Verbose:                  true,
				Logger:                   logger,
			}
			var err error
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

		var mclList []varlog.ManagementClient
		for _, sn := range snList {
			mcl, err := varlog.NewManagementClient(sn.serverAddr)
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

			meta, err := mcl.GetMetadata(context.TODO(), clusterID, snpb.MetadataTypeLogStreams)
			So(err, ShouldBeNil)
			So(meta.GetClusterID(), ShouldEqual, clusterID)
			So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
			So(meta.GetLogStreams(), ShouldHaveLength, 0)

			err = mcl.AddLogStream(context.TODO(), clusterID, sn.storageNodeID, logStreamID, "/tmp")
			So(err, ShouldBeNil)

			meta, err = mcl.GetMetadata(context.TODO(), clusterID, snpb.MetadataTypeLogStreams)
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

		logger, err := zap.NewDevelopment()
		So(err, ShouldBeNil)

		var snList []*StorageNode
		for i := 1; i <= 2; i++ {
			sn, err := NewStorageNode(&StorageNodeOptions{
				RPCOptions:               RPCOptions{RPCBindAddress: bindAddress},
				LogStreamExecutorOptions: DefaultLogStreamExecutorOptions,
				LogStreamReporterOptions: DefaultLogStreamReporterOptions,
				ClusterID:                clusterID,
				StorageNodeID:            types.StorageNodeID(i),
				Verbose:                  true,
				Logger:                   logger,
			})
			So(err, ShouldBeNil)
			snList = append(snList, sn)
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

		var mclList []varlog.ManagementClient
		for _, sn := range snList {
			mcl, err := varlog.NewManagementClient(sn.serverAddr)
			So(err, ShouldBeNil)

			mclList = append(mclList, mcl)

			err = mcl.AddLogStream(context.TODO(), clusterID, sn.storageNodeID, logStreamID, "/tmp")
			So(err, ShouldBeNil)
		}
		defer func() {
			for _, mcl := range mclList {
				mcl.Close()
			}
		}()

		var logclList []varlog.LogIOClient
		for _, sn := range snList {
			logcl, err := varlog.NewLogIOClient(sn.serverAddr)
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
		}

		// Subscribe
		subC, err := oldCL.Subscribe(context.TODO(), logStreamID, types.MinGLSN, lastCommittedGLSN/2)
		So(err, ShouldBeNil)
		for expectedGLSN := types.MinGLSN; expectedGLSN < lastCommittedGLSN/2; expectedGLSN++ {
			sub := <-subC
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
		status, sealedGLSN, err := oldMCL.Seal(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, lastCommittedGLSN)
		So(err, ShouldBeNil)
		So(status, ShouldEqual, vpb.LogStreamStatusSealed)
		So(sealedGLSN, ShouldEqual, lastCommittedGLSN)

		// FAIL: oldSN (SEALED) ---> newSN (RUNNING)
		Convey("FAIL: SEALED LS -> RUNNING LS", func() {
			// check if the newSN (= destination SN) is running
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, vpb.LogStreamStatusRunning)

			// start sync,
			state, err := oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(state, ShouldEqual, snpb.SyncStateInProgress)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				state, err := oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return state != snpb.SyncStateInProgress
			}, time.Minute)

			// eventually, it fails
			state, err = oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(state, ShouldEqual, snpb.SyncStateError)
		})

		// FAIL: oldSN (SEALED) ---> newSN (SEALED)
		Convey("FAIL: SEALED LS -> SEALED LS", func() {
			// check if the newSN (= destination SN) is running
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, vpb.LogStreamStatusRunning)

			// make newSN SEALED (seal newSN with InvalidGLSN)
			status, sealedGLSN, err := newMCL.Seal(context.TODO(), clusterID, newSN.storageNodeID, logStreamID, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(status, ShouldEqual, vpb.LogStreamStatusSealed)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// start sync,
			state, err := oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(state, ShouldEqual, snpb.SyncStateInProgress)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				state, err := oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return state != snpb.SyncStateInProgress
			}, time.Minute)

			// eventually, it fails
			state, err = oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(state, ShouldEqual, snpb.SyncStateError)
		})

		// FAIL: newSN (NOT SEALED) ---> oldSN
		Convey("FAIL: NOT SEALED LS -> ANY LS", func() {
			// Syncing SN (= source SN) is LogStreamStatusRunning
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, vpb.LogStreamStatusRunning)

			// Sync Error
			_, err := newMCL.Sync(context.TODO(), clusterID, newSN.storageNodeID, logStreamID, oldSN.storageNodeID, oldSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldNotBeNil)

			// Syncing SN (= source SN) is LogStreamStatusSealing
			status, sealedGLSN, err := newMCL.Seal(context.TODO(), clusterID, newSN.storageNodeID, logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(status, ShouldEqual, vpb.LogStreamStatusSealing)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// Sync Error
			_, err = newMCL.Sync(context.TODO(), clusterID, newSN.storageNodeID, logStreamID, oldSN.storageNodeID, oldSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldNotBeNil)
		})

		// OK: oldSN (SEALED) ---> newSN (SEALING)
		Convey("OK: SEALED LS -> SEALING LS", func() {
			// newSN (SEALING)
			status, sealedGLSN, err := newMCL.Seal(context.TODO(), clusterID, newSN.storageNodeID, logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(status, ShouldEqual, vpb.LogStreamStatusSealing)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// sync
			_, err = oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				state, err := oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return state != snpb.SyncStateInProgress
			}, time.Minute)

			// check: complete
			state, err := oldMCL.Sync(context.TODO(), clusterID, oldSN.storageNodeID, logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(state, ShouldEqual, snpb.SyncStateComplete)

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
