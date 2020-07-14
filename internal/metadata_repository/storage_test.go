package metadata_repository

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCopyOnWrite(t *testing.T) {
	Convey("storage should returns different stateMachine while copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		pre, cur := ms.getStateMachine()
		So(pre == cur, ShouldBeTrue)

		ms.setCopyOnWrite()

		pre, cur = ms.getStateMachine()
		So(pre == cur, ShouldBeFalse)
	})

	Convey("update matadata should make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)
	})

	Convey("copyOnWrite storage should give the same response for registerStorageNode", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		pre, cur := ms.getStateMachine()
		So(pre.Metadata.GetStorageNode(snID), ShouldNotBeNil)
		So(cur.Metadata.GetStorageNode(snID), ShouldBeNil)

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldNotBeNil)

		snID2 := snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID2,
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		So(pre.Metadata.GetStorageNode(snID2), ShouldBeNil)
		So(cur.Metadata.GetStorageNode(snID2), ShouldNotBeNil)
	})

	Convey("copyOnWrite storage should give the same response for createLogStream", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		ls := &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID,
		}

		err := ms.CreateLogStream(ls, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		pre, cur := ms.getStateMachine()
		So(pre.Metadata.GetLogStream(lsID), ShouldNotBeNil)
		So(cur.Metadata.GetLogStream(lsID), ShouldBeNil)

		err = ms.CreateLogStream(ls, 0, 0)
		So(err, ShouldNotBeNil)

		lsID2 := lsID + types.LogStreamID(1)
		ls = &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID2,
		}

		err = ms.CreateLogStream(ls, 0, 0)
		So(err, ShouldBeNil)

		So(pre.Metadata.GetLogStream(lsID2), ShouldBeNil)
		So(cur.Metadata.GetLogStream(lsID2), ShouldNotBeNil)
	})

	Convey("update LocalLogStream does not make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		snID := types.StorageNodeID(time.Now().UnixNano())
		r := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
			BeginLLSN:     types.LLSN(0),
			EndLLSN:       types.LLSN(5),
			KnownNextGLSN: types.GLSN(10),
		}
		ms.UpdateLocalLogStreamReplica(lsID, snID, r)
		So(ms.isCopyOnWrite(), ShouldBeFalse)
		So(ms.LookupLocalLogStreamReplica(lsID, snID), ShouldNotBeNil)

		Convey("lookup LocalLogStream with copyOnWrite should give merged response", func(ctx C) {
			ms.setCopyOnWrite()

			snID2 := snID + types.StorageNodeID(1)
			r := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
				BeginLLSN:     types.LLSN(0),
				EndLLSN:       types.LLSN(5),
				KnownNextGLSN: types.GLSN(10),
			}
			ms.UpdateLocalLogStreamReplica(lsID, snID2, r)
			So(ms.LookupLocalLogStreamReplica(lsID, snID), ShouldNotBeNil)
			So(ms.LookupLocalLogStreamReplica(lsID, snID2), ShouldNotBeNil)

			replicas := ms.LookupLocalLogStream(lsID)
			So(replicas, ShouldNotBeNil)
			_, ok := replicas.Replicas[snID]
			So(ok, ShouldBeTrue)
			_, ok = replicas.Replicas[snID2]
			So(ok, ShouldBeTrue)
		})
	})

	Convey("update GlobalLogStream does not make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		gls := &snpb.GlobalLogStreamDescriptor{
			PrevNextGLSN: types.GLSN(5),
			NextGLSN:     types.GLSN(10),
		}

		lsID := types.LogStreamID(time.Now().UnixNano())
		commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
			LogStreamID:        lsID,
			CommittedGLSNBegin: types.GLSN(5),
			CommittedGLSNEnd:   types.GLSN(10),
		}
		gls.CommitResult = append(gls.CommitResult, commit)

		ms.AppendGlobalLogStream(gls)
		So(ms.isCopyOnWrite(), ShouldBeFalse)
		So(ms.LookupGlobalLogStreamByPrev(types.GLSN(5)), ShouldNotBeNil)
		So(ms.GetNextGLSN(), ShouldEqual, types.GLSN(10))

		Convey("lookup GlobalLogStream with copyOnWrite should give merged response", func(ctx C) {
			ms.setCopyOnWrite()

			gls := &snpb.GlobalLogStreamDescriptor{
				PrevNextGLSN: types.GLSN(10),
				NextGLSN:     types.GLSN(15),
			}

			commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				LogStreamID:        lsID,
				CommittedGLSNBegin: types.GLSN(10),
				CommittedGLSNEnd:   types.GLSN(15),
			}
			gls.CommitResult = append(gls.CommitResult, commit)

			ms.AppendGlobalLogStream(gls)
			So(ms.LookupGlobalLogStreamByPrev(types.GLSN(5)), ShouldNotBeNil)
			So(ms.LookupGlobalLogStreamByPrev(types.GLSN(10)), ShouldNotBeNil)
			So(ms.GetNextGLSN(), ShouldEqual, types.GLSN(15))
		})
	})
}

func TestMetadataCache(t *testing.T) {
	Convey("cacheCompleteCB should return after make cache", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb)
		ms.Run()

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		timeout := false
		select {
		case <-ch:
		case <-time.After(time.Second):
			timeout = true
		}

		So(timeout, ShouldBeFalse)

		meta := ms.GetMetadata()
		So(meta, ShouldNotBeNil)
		So(meta.GetStorageNode(snID), ShouldNotBeNil)
		So(testutil.CompareWait(func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}, time.Second), ShouldBeTrue)

		Reset(func() {
			ms.Close()
		})
	})

	Convey("createMetadataCache should dedup", t, func(ctx C) {
		ch := make(chan struct{}, 2)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		snID2 := snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID2,
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		ms.Run()

		<-ch
		meta := ms.GetMetadata()
		So(meta, ShouldNotBeNil)

		So(meta.GetStorageNode(snID), ShouldNotBeNil)
		So(meta.GetStorageNode(snID2), ShouldNotBeNil)

		<-ch
		meta2 := ms.GetMetadata()
		So(meta2, ShouldEqual, meta)

		So(testutil.CompareWait(func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}, time.Second), ShouldBeTrue)

		Reset(func() {
			ms.Close()
		})
	})
}

func TestStateMachineMerge(t *testing.T) {
	Convey("merge stateMachine should not operate while job running", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		ms.mergeStateMachine()
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		Convey("merge stateMachine should operate if no more job", func(ctx C) {
			snID = snID + types.StorageNodeID(1)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID,
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			pre, cur := ms.getStateMachine()
			So(pre == cur, ShouldBeFalse)
			So(pre.Metadata.GetStorageNode(snID), ShouldBeNil)
			So(cur.Metadata.GetStorageNode(snID), ShouldNotBeNil)

			ms.Run()

			<-ch

			So(testutil.CompareWait(func() bool {
				return atomic.LoadInt64(&ms.nrRunning) == 0
			}, time.Second), ShouldBeTrue)

			ms.mergeStateMachine()
			So(ms.isCopyOnWrite(), ShouldBeFalse)

			pre, cur = ms.getStateMachine()
			So(pre == cur, ShouldBeTrue)
			So(pre.Metadata.GetStorageNode(snID), ShouldNotBeNil)
			So(cur.Metadata.GetStorageNode(snID), ShouldNotBeNil)

			Reset(func() {
				ms.Close()
			})
		})
	})

	Convey("merge performance:: # of LocalLogStreams:1024. RepFactor:3:: ", t, func(ctx C) {
		ms := NewMetadataStorage(nil)

		for i := 0; i < 1024; i++ {
			lsID := types.LogStreamID(i)

			for j := 0; j < 3; j++ {
				snID := types.StorageNodeID(j)

				s := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					BeginLLSN:     types.LLSN(i * 3),
					EndLLSN:       types.LLSN(i*3 + 1),
					KnownNextGLSN: types.GLSN(0),
				}

				ms.UpdateLocalLogStreamReplica(lsID, snID, s)
			}
		}

		ms.setCopyOnWrite()

		for i := 0; i < 1024; i++ {
			lsID := types.LogStreamID(i)

			for j := 0; j < 3; j++ {
				snID := types.StorageNodeID(j)

				s := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					BeginLLSN:     types.LLSN(1 + i*3),
					EndLLSN:       types.LLSN(1 + i*3 + 1),
					KnownNextGLSN: types.GLSN(1024),
				}

				ms.UpdateLocalLogStreamReplica(lsID, snID, s)
			}
		}

		ms.releaseCopyOnWrite()

		st := time.Now()
		ms.mergeStateMachine()
		fmt.Println(time.Now().Sub(st))
	})
}

func TestSnapshot(t *testing.T) {
	Convey("create snapshot should not operate while job running", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		appliedIndex := uint64(0)

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		appliedIndex++
		ms.UpdateAppliedIndex(appliedIndex)
		ms.triggerSnapshot(appliedIndex)

		ms.Run()

		<-ch

		snID = snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		<-ch

		snap, _ := ms.GetSnapshot()
		So(snap, ShouldBeNil)
		So(testutil.CompareWait(func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}, time.Second), ShouldBeTrue)

		Convey("create snapshot should operate if no more job", func(ctx C) {
			appliedIndex++
			ms.UpdateAppliedIndex(appliedIndex)
			ms.triggerSnapshot(appliedIndex)

			snID2 := snID + types.StorageNodeID(1)
			sn = &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID2,
			}

			err = ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			<-ch

			snap, snapIndex := ms.GetSnapshot()
			So(snap, ShouldNotBeNil)
			So(snapIndex, ShouldEqual, appliedIndex)

			u := &pb.MetadataRepositoryDescriptor{}
			err = u.Unmarshal(snap)
			So(err, ShouldBeNil)

			So(u.Metadata.GetStorageNode(snID), ShouldNotBeNil)
			So(u.Metadata.GetStorageNode(snID2), ShouldBeNil)
		})

		Reset(func() {
			ms.Close()
		})
	})
}

func TestSnapshotRace(t *testing.T) {
	Convey("create snapshot", t, func(ctx C) {
		defaultSnapshotCount = uint64(100 + rand.Int31n(64))

		ms := NewMetadataStorage(nil)
		ms.Run()

		n := 10000
		numLS := 128
		numRep := 3

		appliedIndex := uint64(0)
		checkGLS := 0
		checkLS := 0

		for i := 0; i < n; i++ {
			preGLSN := types.GLSN(i * numLS)
			newGLSN := types.GLSN((i + 1) * numLS)
			gls := &snpb.GlobalLogStreamDescriptor{
				PrevNextGLSN: preGLSN,
				NextGLSN:     newGLSN,
			}

			for j := 0; j < numLS; j++ {
				lsID := types.LogStreamID(j)

				for k := 0; k < numRep; k++ {
					snID := types.StorageNodeID(j*numRep + k)

					r := &pb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
						BeginLLSN:     types.LLSN(i),
						EndLLSN:       types.LLSN(i + 1),
						KnownNextGLSN: preGLSN,
					}

					ms.UpdateLocalLogStreamReplica(lsID, snID, r)

					appliedIndex++
					ms.UpdateAppliedIndex(appliedIndex)
				}

				commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
					LogStreamID:        lsID,
					CommittedGLSNBegin: preGLSN,
					CommittedGLSNEnd:   newGLSN,
				}
				gls.CommitResult = append(gls.CommitResult, commit)
			}

			ms.AppendGlobalLogStream(gls)

			appliedIndex++
			ms.UpdateAppliedIndex(appliedIndex)

			gls = ms.GetNextGLSFrom(preGLSN)
			if gls != nil &&
				gls.NextGLSN == newGLSN &&
				ms.GetNextGLSN() == newGLSN {
				checkGLS++
			}

		CHECKLS:
			for j := 0; j < numLS; j++ {
				lsID := types.LogStreamID(j)
				ls := ms.LookupLocalLogStream(lsID)
				if ls == nil {
					continue CHECKLS
				}

				for k := 0; k < numRep; k++ {
					snID := types.StorageNodeID(j*numRep + k)

					r, ok := ls.Replicas[snID]
					if ok &&
						r.KnownNextGLSN == preGLSN {
						checkLS++
					}
				}
			}
		}

		So(checkGLS, ShouldEqual, n)
		So(checkLS, ShouldEqual, n*numLS*numRep)

		So(testutil.CompareWait(func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}, 10*time.Second), ShouldBeTrue)

		ms.mergeStateMachine()
		ms.triggerSnapshot(appliedIndex)

		So(testutil.CompareWait(func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}, 10*time.Second), ShouldBeTrue)

		_, recv := ms.GetSnapshot()
		So(recv, ShouldEqual, appliedIndex)

		Reset(func() {
			ms.Close()
		})
	})
}
