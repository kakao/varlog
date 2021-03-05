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

	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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

			opts := DefaultOptions()
			opts.RPCOptions.ListenAddress = bindAddress
			opts.ClusterID = clusterID
			opts.StorageNodeID = types.StorageNodeID(i)
			opts.Volumes = map[Volume]struct{}{volume: {}}
			opts.Logger = zap.L()

			sn, err := NewStorageNode(context.TODO(), &opts)
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
			mcl, err := snc.NewManagementClient(context.TODO(), clusterID, sn.advertiseAddr, zap.L())
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

			meta, err := mcl.GetMetadata(context.TODO())
			So(err, ShouldBeNil)
			So(meta.GetClusterID(), ShouldEqual, clusterID)
			So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
			So(meta.GetLogStreams(), ShouldHaveLength, 0)

			storages := meta.GetStorageNode().GetStorages()
			So(len(storages), ShouldBeGreaterThan, 0)

			err = mcl.AddLogStream(context.TODO(), logStreamID, storages[0].GetPath())
			So(err, ShouldBeNil)

			meta, err = mcl.GetMetadata(context.TODO())
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

			opts := DefaultOptions()
			opts.RPCOptions.ListenAddress = bindAddress
			opts.ClusterID = clusterID
			opts.StorageNodeID = types.StorageNodeID(i)
			opts.Volumes = map[Volume]struct{}{volume: {}}
			opts.Logger = zap.L()

			sn, err := NewStorageNode(context.TODO(), &opts)
			So(err, ShouldBeNil)
			snList = append(snList, sn)

			for snPath := range sn.storageNodePaths {
				storagePathList = append(storagePathList, snPath.(string))
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
			mcl, err := snc.NewManagementClient(context.TODO(), clusterID, sn.advertiseAddr, zap.L())
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
			logcl, err := logc.NewLogIOClient(context.TODO(), sn.advertiseAddr)
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
		So(oldSN.advertiseAddr, ShouldNotEqual, newSN.advertiseAddr)

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
			status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(status.GetState(), ShouldEqual, snpb.SyncStateInProgress)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return status.GetState() != snpb.SyncStateInProgress
			}, time.Minute)

			// eventually, it fails
			_, err = oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
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
			syncStatus, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(syncStatus.GetState(), ShouldEqual, snpb.SyncStateInProgress)

			// wait for changing syncstate
			var lastStatus *snpb.SyncStatus
			testutil.CompareWait(func() bool {
				status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				lastStatus = status
				return status.GetState() != snpb.SyncStateInProgress
			}, time.Minute)

			// eventually, it fails
			So(lastStatus.GetState(), ShouldEqual, snpb.SyncStateError)
			/*
				syncStatus, err = oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.serverAddr, lastCommittedGLSN)
				So(err, ShouldBeNil)
				So(syncStatus.GetState(), ShouldEqual, snpb.SyncStateError)
			*/
		})

		// ERROR: newSN (NOT SEALED) ---> oldSN
		Convey("ERROR: NOT SEALED LS -> ANY LS", func() {
			// Syncing SN (= source SN) is LogStreamStatusRunning
			lse, ok := newSN.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)
			So(lse.Status(), ShouldEqual, varlogpb.LogStreamStatusRunning)

			// Sync Error
			_, err := newMCL.Sync(context.TODO(), logStreamID, oldSN.storageNodeID, oldSN.advertiseAddr, lastCommittedGLSN)
			So(err, ShouldNotBeNil)

			// Syncing SN (= source SN) is LogStreamStatusSealing
			status, sealedGLSN, err := newMCL.Seal(context.TODO(), logStreamID, lastCommittedGLSN)
			So(err, ShouldBeNil)
			So(status, ShouldEqual, varlogpb.LogStreamStatusSealing)
			So(sealedGLSN, ShouldEqual, types.InvalidGLSN)

			// Sync Error
			_, err = newMCL.Sync(context.TODO(), logStreamID, oldSN.storageNodeID, oldSN.advertiseAddr, lastCommittedGLSN)
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
			_, err = oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
			So(err, ShouldBeNil)

			// wait for changing syncstate
			testutil.CompareWait(func() bool {
				status, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
				c.So(err, ShouldBeNil)
				return status.GetState() != snpb.SyncStateInProgress
			}, time.Minute)

			// check: complete
			syncStatus, err := oldMCL.Sync(context.TODO(), logStreamID, newSN.storageNodeID, newSN.advertiseAddr, lastCommittedGLSN)
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

func TestStorageNodeRestart(t *testing.T) {
	Convey("StorageNode Restart", t, func() {
		const (
			numLogs     = 10
			moreLogs    = 10
			logStreamID = types.LogStreamID(1)
		)

		volume, err := NewVolume(t.TempDir())
		So(err, ShouldBeNil)

		opts := DefaultOptions()
		opts.RPCOptions.ListenAddress = bindAddress
		opts.ClusterID = clusterID
		opts.StorageNodeID = types.StorageNodeID(1)
		opts.Volumes = map[Volume]struct{}{volume: {}}
		opts.Logger = zap.L()

		sn, err := NewStorageNode(context.TODO(), &opts)
		if err != nil {
			t.Fatal(err)
		}

		if err := sn.Run(); err != nil {
			t.Fatal(err)
		}

		// snmcl: connect
		mcl, err := snc.NewManagementClient(context.TODO(), clusterID, sn.advertiseAddr, zap.L())
		if err != nil {
			t.Fatal(err)
		}

		meta, err := mcl.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta.GetClusterID(), ShouldEqual, clusterID)
		So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
		So(meta.GetLogStreams(), ShouldHaveLength, 0)

		storages := meta.GetStorageNode().GetStorages()
		So(len(storages), ShouldBeGreaterThan, 0)

		err = mcl.AddLogStream(context.TODO(), logStreamID, storages[0].GetPath())
		So(err, ShouldBeNil)

		// logcl: connect
		logCL, err := logc.NewLogIOClient(context.TODO(), sn.advertiseAddr)
		if err != nil {
			t.Fatal(err)
		}

		for hwm := types.GLSN(1); hwm < types.GLSN(1+numLogs); hwm++ {
			lse, ok := sn.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)

			data := []byte(fmt.Sprintf("data-%02d", int(hwm)))
			errC := commitAndWait(lse, hwm, hwm-1, hwm, 1)
			glsn, err := logCL.Append(context.TODO(), logStreamID, data)

			So(glsn, ShouldNotEqual, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(<-errC, ShouldBeNil)

			// Read
			ent, err := logCL.Read(context.TODO(), logStreamID, hwm)
			So(err, ShouldBeNil)
			So(data, ShouldResemble, ent.Data)
		}

		// Subscribe
		subC, err := logCL.Subscribe(context.TODO(), logStreamID, 1, 11)
		So(err, ShouldBeNil)
		for expected := types.GLSN(1); expected < types.GLSN(1+numLogs); expected++ {
			sub := <-subC
			So(sub.Error, ShouldBeNil)
			So(sub.GLSN, ShouldEqual, expected)
		}
		So((<-subC).Error, ShouldEqual, io.EOF)
		_, more := <-subC
		So(more, ShouldBeFalse)

		sn.Close()
		sn.Wait()
		So(mcl.Close(), ShouldBeNil)
		So(logCL.Close(), ShouldBeNil)

		// restart
		sn, err = NewStorageNode(context.TODO(), &opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := sn.Run(); err != nil {
			t.Fatal(err)
		}

		// mcl: reconnect
		mcl, err = snc.NewManagementClient(context.TODO(), clusterID, sn.advertiseAddr, zap.L())
		if err != nil {
			t.Fatal(err)
		}
		meta, err = mcl.GetMetadata(context.TODO())
		So(err, ShouldBeNil)
		So(meta.GetClusterID(), ShouldEqual, clusterID)
		So(meta.GetStorageNode().GetStorageNodeID(), ShouldEqual, sn.storageNodeID)
		So(meta.GetLogStreams(), ShouldHaveLength, 1)
		So(meta.GetLogStreams()[0].GetLogStreamID(), ShouldEqual, logStreamID)

		// logcl: connect
		logCL, err = logc.NewLogIOClient(context.TODO(), sn.advertiseAddr)
		if err != nil {
			t.Fatal(err)
		}

		// Subscribe
		subC, err = logCL.Subscribe(context.TODO(), logStreamID, 1, 11)
		So(err, ShouldBeNil)
		for expected := types.GLSN(1); expected < types.GLSN(1+numLogs); expected++ {
			sub := <-subC
			So(sub.Error, ShouldBeNil)
			So(sub.GLSN, ShouldEqual, expected)
		}
		So((<-subC).Error, ShouldEqual, io.EOF)
		_, more = <-subC
		So(more, ShouldBeFalse)

		// append
		for hwm := types.GLSN(1 + numLogs); hwm < types.GLSN(1+numLogs+moreLogs); hwm++ {
			lse, ok := sn.GetLogStreamExecutor(logStreamID)
			So(ok, ShouldBeTrue)

			data := []byte(fmt.Sprintf("data-%02d", int(hwm)))
			errC := commitAndWait(lse, hwm, hwm-1, hwm, 1)
			glsn, err := logCL.Append(context.TODO(), logStreamID, data)

			So(glsn, ShouldNotEqual, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(<-errC, ShouldBeNil)

			// Read
			ent, err := logCL.Read(context.TODO(), logStreamID, hwm)
			So(err, ShouldBeNil)
			So(data, ShouldResemble, ent.Data)
		}

		sn.Close()
		sn.Wait()
		So(mcl.Close(), ShouldBeNil)
		So(logCL.Close(), ShouldBeNil)
	})
}

func TestRPCAddress(t *testing.T) {
	Convey("Given that listen address of StorageNode is not specified", t, func() {
		opts := DefaultOptions()
		opts.ListenAddress = "0.0.0.0:0"

		Convey("When the advertise address is specified", func() {
			opts.AdvertiseAddress = "127.0.0.1:9999"

			Convey("Then advertise address of the StorageNode should be equal to opts.AdvertiseAddress", func() {
				sn, err := NewStorageNode(context.TODO(), &opts)
				So(err, ShouldBeNil)
				So(sn.Run(), ShouldBeNil)
				Reset(func() {
					sn.Close()
				})

				snmd, err := sn.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				So(snmd.GetStorageNode().GetAddress(), ShouldEqual, opts.AdvertiseAddress)
			})
		})

		Convey("When the advertise address is not specified", func() {
			Convey("Then advertise address of StorageNode should be set", func() {
				sn, err := NewStorageNode(context.TODO(), &opts)
				So(err, ShouldBeNil)
				So(sn.Run(), ShouldBeNil)
				Reset(func() {
					sn.Close()
				})

				snmd, err := sn.GetMetadata(context.TODO())
				So(err, ShouldBeNil)

				So(snmd.GetStorageNode().GetAddress(), ShouldNotBeEmpty)
			})
		})
	})

	Convey("Given that listen address of StorageNode is specified", t, func() {
		// TODO: Binding specified address can result in bind error when parallel testing
		t.Skip()
	})
}
