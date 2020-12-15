package metadata_repository

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/vtesting"
)

func TestStorageRegisterSN(t *testing.T) {
	Convey("SN should be registered", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)
		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
			Address:       "mt_addr",
		}

		err := ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		Convey("SN should not be registered if arleady exist", func(ctx C) {
			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)

			dup_sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID,
				Address:       "diff_addr",
			}

			err = ms.registerStorageNode(dup_sn)
			So(err, ShouldResemble, verrors.ErrAlreadyExists)
		})
	})
}

func TestStoragUnregisterSN(t *testing.T) {
	Convey("Given a MetadataStorage", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)
		snID := types.StorageNodeID(time.Now().UnixNano())

		Convey("When SN is not exist", func(ctx C) {
			Convey("Then it should not be registered", func(ctx C) {
				err := ms.unregisterStorageNode(snID)
				So(err, ShouldResemble, verrors.ErrNotExist)
			})
		})

		Convey("Wnen SN is exist", func(ctx C) {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID,
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			So(ms.isCopyOnWrite(), ShouldBeTrue)

			Convey("Then it should be unregistered", func(ctx C) {
				err = ms.unregisterStorageNode(snID)
				So(err, ShouldBeNil)

				So(ms.lookupStorageNode(snID), ShouldBeNil)

				Convey("unregistered SN should not be found after merge", func(ctx C) {
					ms.mergeMetadata()
					ms.mergeLogStream()

					ms.releaseCopyOnWrite()

					So(ms.lookupStorageNode(snID), ShouldBeNil)
				})
			})

			Convey("And LS which have the SN as replica is exist", func(ctx C) {
				rep := 1
				lsID := types.LogStreamID(time.Now().UnixNano())
				snIDs := make([]types.StorageNodeID, rep)
				for i := 0; i < rep; i++ {
					snIDs[i] = snID + types.StorageNodeID(i)
				}
				ls := makeLogStream(lsID, snIDs)

				err := ms.registerLogStream(ls)
				So(err, ShouldBeNil)

				Convey("Then SN should not be unregistered", func(ctx C) {
					err := ms.unregisterStorageNode(snID)
					So(err, ShouldResemble, verrors.ErrInvalidArgument)
				})
			})
		})
	})
}

func TestStoragGetAllSN(t *testing.T) {
	Convey("Given a MetadataStorage", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		Convey("When SN is not exist", func(ctx C) {
			Convey("Then it returns nil", func(ctx C) {
				So(ms.GetAllStorageNodes(), ShouldBeNil)
			})
		})

		Convey("Wnen SN register", func(ctx C) {
			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID,
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			Convey("Then it should return 1 SN", func(ctx C) {
				sns := ms.GetAllStorageNodes()
				So(len(sns), ShouldEqual, 1)

				Convey("Wnen one more SN register to diff", func(ctx C) {
					So(ms.isCopyOnWrite(), ShouldBeTrue)
					snID2 := types.StorageNodeID(time.Now().UnixNano())
					sn := &varlogpb.StorageNodeDescriptor{
						StorageNodeID: snID2,
					}

					err := ms.RegisterStorageNode(sn, 0, 0)
					So(err, ShouldBeNil)

					Convey("Then it should return 2 SNs", func(ctx C) {
						sns := ms.GetAllStorageNodes()
						So(len(sns), ShouldEqual, 2)

						Convey("When unregister SN which registered orig", func(ctx C) {
							err = ms.unregisterStorageNode(snID)
							So(err, ShouldBeNil)

							So(ms.lookupStorageNode(snID), ShouldBeNil)

							Convey("Then it should returns 1 SNs", func(ctx C) {
								sns := ms.GetAllStorageNodes()
								So(len(sns), ShouldEqual, 1)

								Convey("When merge Metadata", func(ctx C) {
									ms.mergeMetadata()

									ms.releaseCopyOnWrite()

									Convey("Then it should returns 1 SNs", func(ctx C) {
										sns := ms.GetAllStorageNodes()
										So(len(sns), ShouldEqual, 1)
									})
								})
							})
						})
					})
				})
			})
		})
	})
}

func TestStorageRegisterLS(t *testing.T) {
	Convey("LS which has no SN should not be registerd", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		ls := makeLogStream(lsID, nil)

		err := ms.registerLogStream(ls)
		So(err, ShouldResemble, verrors.ErrInvalidArgument)
	})

	Convey("LS should not be registerd if not exist proper SN", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		tmp := types.StorageNodeID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = tmp + types.StorageNodeID(i)
		}
		ls := makeLogStream(lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(err, ShouldResemble, verrors.ErrInvalidArgument)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snIDs[0],
		}

		err = ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		err = ms.registerLogStream(ls)
		So(err, ShouldResemble, verrors.ErrInvalidArgument)

		Convey("LS should be registerd if exist all SN", func(ctx C) {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[1],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)

			err = ms.registerLogStream(ls)
			So(err, ShouldBeNil)

			Convey("registered LS should be lookuped", func(ctx C) {
				for i := 0; i < rep; i++ {
					So(ms.LookupLocalLogStreamReplica(lsID, snIDs[i]), ShouldNotBeNil)
				}
			})
		})
	})
}

func TestStoragUnregisterLS(t *testing.T) {
	Convey("LS which is not exist should not be unregistered", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		lsID := types.LogStreamID(time.Now().UnixNano())

		err := ms.unregisterLogStream(lsID)
		So(err, ShouldResemble, verrors.ErrNotExist)

		Convey("LS which is exist should be unregistered", func(ctx C) {
			rep := 1
			snIDs := make([]types.StorageNodeID, rep)
			tmp := types.StorageNodeID(time.Now().UnixNano())
			for i := 0; i < rep; i++ {
				snIDs[i] = tmp + types.StorageNodeID(i)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i],
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
			ls := makeLogStream(lsID, snIDs)

			err := ms.RegisterLogStream(ls, 0, 0)
			So(err, ShouldBeNil)
			So(len(ms.GetLocalLogStreamIDs()), ShouldEqual, 1)

			So(ms.isCopyOnWrite(), ShouldBeTrue)

			err = ms.unregisterLogStream(lsID)
			So(err, ShouldBeNil)

			So(ms.lookupLogStream(lsID), ShouldBeNil)
			So(ms.LookupLocalLogStream(lsID), ShouldBeNil)
			So(len(ms.GetLocalLogStreamIDs()), ShouldEqual, 0)

			Convey("unregistered SN should not be found after merge", func(ctx C) {
				ms.mergeMetadata()
				ms.mergeLogStream()

				ms.releaseCopyOnWrite()

				So(ms.lookupLogStream(lsID), ShouldBeNil)
				So(ms.LookupLocalLogStream(lsID), ShouldBeNil)
				So(len(ms.GetLocalLogStreamIDs()), ShouldEqual, 0)
			})
		})
	})
}

func TestStorageUpdateLS(t *testing.T) {
	Convey("LS should not be updated if not exist proper SN", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		updateSnIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			updateSnIDs[i] = snIDs[i] + types.StorageNodeID(rep)
		}
		updateLS := makeLogStream(lsID, updateSnIDs)

		err = ms.UpdateLogStream(updateLS, 0, 0)
		So(err, ShouldResemble, verrors.ErrInvalidArgument)

		Convey("LS should be updated if exist all SN", func(ctx C) {
			for i := 0; i < rep; i++ {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: updateSnIDs[i],
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}

			err = ms.UpdateLogStream(updateLS, 0, 0)
			So(err, ShouldBeNil)
			Convey("updated LS should be lookuped", func(ctx C) {
				for i := 0; i < rep; i++ {
					So(ms.LookupLocalLogStreamReplica(lsID, snIDs[i]), ShouldBeNil)
					So(ms.LookupLocalLogStreamReplica(lsID, updateSnIDs[i]), ShouldNotBeNil)
				}

				So(testutil.CompareWaitN(10, func() bool {
					return atomic.LoadInt64(&ms.nrRunning) == 0
				}), ShouldBeTrue)

				meta := ms.GetMetadata()
				So(meta, ShouldNotBeNil)

				ls := meta.GetLogStream(lsID)
				So(ls, ShouldNotBeNil)

				for _, r := range ls.Replicas {
					exist := false
					for i := 0; i < rep; i++ {
						So(r.StorageNodeID, ShouldNotEqual, snIDs[i])
						if !exist {
							exist = r.StorageNodeID == updateSnIDs[i]
						}
					}
					So(exist, ShouldBeTrue)
				}
			})
		})
	})
}

func TestStorageUpdateLSUnderCOW(t *testing.T) {
	Convey("update LS to COW storage should applyed after merge", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		rep := 2

		// register LS
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		// set COW
		ms.setCopyOnWrite()

		// update LS
		updateSnIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			updateSnIDs[i] = snIDs[i] + types.StorageNodeID(rep)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: updateSnIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		updateLS := makeLogStream(lsID, updateSnIDs)

		err = ms.UpdateLogStream(updateLS, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		// compare
		diffls := ms.diffStateMachine.Metadata.GetLogStream(lsID)
		difflls, _ := ms.diffStateMachine.LogStream.LocalLogStreams[lsID]

		origls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		origlls, _ := ms.origStateMachine.LogStream.LocalLogStreams[lsID]

		So(diffls.Equal(origls), ShouldBeFalse)
		So(difflls.Equal(origlls), ShouldBeFalse)

		// merge
		ms.mergeMetadata()
		ms.mergeLogStream()

		ms.releaseCopyOnWrite()

		// compare
		mergedls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		mergedlls, _ := ms.origStateMachine.LogStream.LocalLogStreams[lsID]

		So(diffls.Equal(mergedls), ShouldBeTrue)
		So(difflls.Equal(mergedlls), ShouldBeTrue)
	})
}

func TestStorageSealLS(t *testing.T) {
	Convey("LS should not be sealed if not exist", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		err := ms.SealLogStream(lsID, 0, 0)
		So(err, ShouldResemble, verrors.ErrNotExist)
	})

	Convey("For resigtered LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("Seal should be success", func(ctx C) {
			err = ms.SealLogStream(lsID, 0, 0)
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(10, func() bool {
				return atomic.LoadInt64(&ms.nrRunning) == 0
			}), ShouldBeTrue)

			Convey("Sealed LS should have LogStreamStatusSealed", func(ctx C) {
				meta := ms.GetMetadata()
				So(meta, ShouldNotBeNil)

				ls := meta.GetLogStream(lsID)
				So(ls, ShouldNotBeNil)
				So(ls.Status, ShouldEqual, varlogpb.LogStreamStatusSealed)
			})

			Convey("Seal to LS which is already Sealed should return nil", func(ctx C) {
				err := ms.SealLogStream(lsID, 0, 0)
				So(err, ShouldBeNil)
			})
		})

		Convey("Sealed LocalLogStreamReplica should have same EndLLSN", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				return atomic.LoadInt64(&ms.nrRunning) == 0
			}), ShouldBeTrue)

			for i := 0; i < rep; i++ {
				r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					UncommittedLLSNOffset: types.MinLLSN,
					UncommittedLLSNLength: uint64(i),
					KnownHighWatermark:    types.InvalidGLSN,
				}

				ms.UpdateLocalLogStreamReplica(lsID, snIDs[i], r)
				rr := ms.LookupLocalLogStreamReplica(lsID, snIDs[i])
				So(rr, ShouldNotBeNil)
				So(r.UncommittedLLSNEnd(), ShouldEqual, r.UncommittedLLSNEnd())
			}

			err = ms.SealLogStream(lsID, 0, 0)
			So(err, ShouldBeNil)

			for i := 0; i < rep; i++ {
				r := ms.LookupLocalLogStreamReplica(lsID, snIDs[i])
				So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN)
			}

			Convey("Sealed LocalLogStreamReplica should ignore report", func(ctx C) {
				for i := 0; i < rep; i++ {
					r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: uint64(i + 1),
						KnownHighWatermark:    types.InvalidGLSN,
					}

					ms.UpdateLocalLogStreamReplica(lsID, snIDs[i], r)
					rr := ms.LookupLocalLogStreamReplica(lsID, snIDs[i])
					So(rr, ShouldNotBeNil)
					So(rr.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN)
				}
			})
		})
	})
}

func TestStorageSealLSUnderCOW(t *testing.T) {
	Convey("seal LS to COW storage should applyed after merge", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		rep := 2

		// register LS
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		// set COW
		ms.setCopyOnWrite()

		// seal
		err = ms.SealLogStream(lsID, 0, 0)
		So(err, ShouldBeNil)

		// compare
		diffls := ms.diffStateMachine.Metadata.GetLogStream(lsID)
		difflls, _ := ms.diffStateMachine.LogStream.LocalLogStreams[lsID]

		origls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		origlls, _ := ms.origStateMachine.LogStream.LocalLogStreams[lsID]

		So(diffls.Equal(origls), ShouldBeFalse)
		So(difflls.Equal(origlls), ShouldBeFalse)

		// merge
		ms.mergeMetadata()
		ms.mergeLogStream()

		ms.releaseCopyOnWrite()

		// compare
		mergedls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		mergedlls, _ := ms.origStateMachine.LogStream.LocalLogStreams[lsID]

		So(diffls.Equal(mergedls), ShouldBeTrue)
		So(difflls.Equal(mergedlls), ShouldBeTrue)

		So(mergedls.Status, ShouldEqual, varlogpb.LogStreamStatusSealed)
		So(mergedlls.Status, ShouldEqual, varlogpb.LogStreamStatusSealed)
	})
}

func TestStorageUnsealLS(t *testing.T) {
	Convey("Storage should return ErrNotExsit if Unseal to not exist LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		err := ms.UnsealLogStream(lsID, 0, 0)
		So(err, ShouldResemble, verrors.ErrNotExist)
	})

	Convey("For resigtered LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("Unseal to LS which is already Unsealed should return nil", func(ctx C) {
			err := ms.UnsealLogStream(lsID, 0, 0)
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(10, func() bool {
				return atomic.LoadInt64(&ms.nrRunning) == 0
			}), ShouldBeTrue)

			Convey("Unsealed to sealed LS should be success", func(ctx C) {
				err := ms.SealLogStream(lsID, 0, 0)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(10, func() bool {
					return atomic.LoadInt64(&ms.nrRunning) == 0
				}), ShouldBeTrue)

				err = ms.UnsealLogStream(lsID, 0, 0)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(10, func() bool {
					return atomic.LoadInt64(&ms.nrRunning) == 0
				}), ShouldBeTrue)

				meta := ms.GetMetadata()
				So(meta, ShouldNotBeNil)

				ls := meta.GetLogStream(lsID)
				So(ls, ShouldNotBeNil)
				So(ls.Status, ShouldEqual, varlogpb.LogStreamStatusRunning)

				Convey("Unsealed LS should update report", func(ctx C) {
					for i := 0; i < rep; i++ {
						r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
							UncommittedLLSNOffset: types.MinLLSN,
							UncommittedLLSNLength: uint64(i),
							KnownHighWatermark:    types.InvalidGLSN,
						}

						ms.UpdateLocalLogStreamReplica(lsID, snIDs[i], r)
						rr := ms.LookupLocalLogStreamReplica(lsID, snIDs[i])
						So(rr, ShouldNotBeNil)
						So(rr.UncommittedLLSNEnd(), ShouldEqual, r.UncommittedLLSNEnd())
					}
				})
			})
		})
	})
}

func TestStorageTrim(t *testing.T) {
	Convey("Given a GlobalLogStreams", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		for hwm := types.MinGLSN; hwm < types.GLSN(1024); hwm++ {
			gls := &snpb.GlobalLogStreamDescriptor{
				HighWatermark:     hwm,
				PrevHighWatermark: hwm - types.GLSN(1),
			}

			gls.CommitResult = append(gls.CommitResult, &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				CommittedGLSNOffset: hwm,
				CommittedGLSNLength: 1,
			})

			ms.AppendGlobalLogStream(gls)
		}

		Convey("When operate trim, trimmed gls should not be found", func(ctx C) {
			for trim := types.InvalidGLSN; trim < types.GLSN(1024); trim++ {
				ms.TrimGlobalLogStream(trim)
				ms.trimGlobalLogStream()

				if trim != types.InvalidGLSN {
					So(ms.lookupNextGLSNoLock(trim-types.GLSN(1)), ShouldNotBeNil)
				}

				if trim > types.MinGLSN {
					So(ms.lookupNextGLSNoLock(trim-types.GLSN(2)), ShouldBeNil)
				}
			}
		})
	})
}

func TestStorageReport(t *testing.T) {
	Convey("storage should not apply report if not registered LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		tmp := types.StorageNodeID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = tmp + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		notExistSnID := tmp + types.StorageNodeID(rep)

		r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
			UncommittedLLSNOffset: types.MinLLSN,
			UncommittedLLSNLength: 5,
			KnownHighWatermark:    types.InvalidGLSN,
		}

		for i := 0; i < rep; i++ {
			ms.UpdateLocalLogStreamReplica(lsID, snIDs[i], r)
			So(ms.LookupLocalLogStreamReplica(lsID, snIDs[i]), ShouldBeNil)
		}

		Convey("storage should not apply report if snID is not exist in LS", func(ctx C) {
			ls := makeLogStream(lsID, snIDs)
			ms.registerLogStream(ls)

			r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
				UncommittedLLSNOffset: types.MinLLSN,
				UncommittedLLSNLength: 5,
				KnownHighWatermark:    types.InvalidGLSN,
			}

			ms.UpdateLocalLogStreamReplica(lsID, notExistSnID, r)
			So(ms.LookupLocalLogStreamReplica(lsID, notExistSnID), ShouldBeNil)

			Convey("storage should apply report if snID is exist in LS", func(ctx C) {
				r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					UncommittedLLSNOffset: types.MinLLSN,
					UncommittedLLSNLength: 5,
					KnownHighWatermark:    types.InvalidGLSN,
				}

				for i := 0; i < rep; i++ {
					ms.UpdateLocalLogStreamReplica(lsID, snIDs[i], r)
					So(ms.LookupLocalLogStreamReplica(lsID, snIDs[i]), ShouldNotBeNil)
				}
			})
		})
	})
}

func TestStorageCopyOnWrite(t *testing.T) {
	Convey("storage should returns different stateMachine while copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		pre, cur := ms.getStateMachine()
		So(pre == cur, ShouldBeTrue)

		ms.setCopyOnWrite()

		pre, cur = ms.getStateMachine()
		So(pre == cur, ShouldBeFalse)
	})

	Convey("update matadata should make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)
	})

	Convey("copyOnWrite storage should give the same response for registerStorageNode", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
			Address:       "my_addr",
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		pre, cur := ms.getStateMachine()
		So(pre.Metadata.GetStorageNode(snID), ShouldNotBeNil)
		So(cur.Metadata.GetStorageNode(snID), ShouldBeNil)

		sn = &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
			Address:       "diff_addr",
		}
		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldResemble, verrors.ErrAlreadyExists)

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
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		lsID2 := lsID + types.LogStreamID(1)

		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)
		ls2 := makeLogStream(lsID2, snIDs)

		err := ms.RegisterLogStream(ls, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		pre, cur := ms.getStateMachine()
		So(pre.Metadata.GetLogStream(lsID), ShouldNotBeNil)
		So(cur.Metadata.GetLogStream(lsID), ShouldBeNil)

		conflict := makeLogStream(lsID, snIDs)
		ls.Replicas[0].StorageNodeID = ls.Replicas[0].StorageNodeID + 100
		err = ms.RegisterLogStream(conflict, 0, 0)
		So(err, ShouldResemble, verrors.ErrAlreadyExists)

		err = ms.RegisterLogStream(ls2, 0, 0)
		So(err, ShouldBeNil)

		So(pre.Metadata.GetLogStream(lsID2), ShouldBeNil)
		So(cur.Metadata.GetLogStream(lsID2), ShouldNotBeNil)
	})

	Convey("update LocalLogStream does not make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		ls := makeLogStream(lsID, snIDs)
		ms.registerLogStream(ls)

		r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
			UncommittedLLSNOffset: types.MinLLSN,
			UncommittedLLSNLength: 5,
			KnownHighWatermark:    types.GLSN(10),
		}
		ms.UpdateLocalLogStreamReplica(lsID, snIDs[0], r)
		So(ms.isCopyOnWrite(), ShouldBeFalse)

		for i := 0; i < rep; i++ {
			So(ms.LookupLocalLogStreamReplica(lsID, snIDs[i]), ShouldNotBeNil)
		}

		Convey("lookup LocalLogStream with copyOnWrite should give merged response", func(ctx C) {
			ms.setCopyOnWrite()

			r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
				UncommittedLLSNOffset: types.MinLLSN,
				UncommittedLLSNLength: 5,
				KnownHighWatermark:    types.GLSN(10),
			}
			ms.UpdateLocalLogStreamReplica(lsID, snIDs[1], r)

			for i := 0; i < rep; i++ {
				So(ms.LookupLocalLogStreamReplica(lsID, snIDs[i]), ShouldNotBeNil)
			}
		})
	})

	Convey("update GlobalLogStream does not make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		gls := &snpb.GlobalLogStreamDescriptor{
			PrevHighWatermark: types.GLSN(5),
			HighWatermark:     types.GLSN(10),
		}

		lsID := types.LogStreamID(time.Now().UnixNano())
		commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
			LogStreamID:         lsID,
			CommittedGLSNOffset: types.GLSN(6),
			CommittedGLSNLength: 5,
		}
		gls.CommitResult = append(gls.CommitResult, commit)

		ms.AppendGlobalLogStream(gls)
		So(ms.isCopyOnWrite(), ShouldBeFalse)
		So(ms.LookupNextGLS(types.GLSN(5)), ShouldNotBeNil)
		So(ms.GetHighWatermark(), ShouldEqual, types.GLSN(10))

		Convey("lookup GlobalLogStream with copyOnWrite should give merged response", func(ctx C) {
			ms.setCopyOnWrite()

			gls := &snpb.GlobalLogStreamDescriptor{
				PrevHighWatermark: types.GLSN(10),
				HighWatermark:     types.GLSN(15),
			}

			commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				LogStreamID:         lsID,
				CommittedGLSNOffset: types.GLSN(11),
				CommittedGLSNLength: 5,
			}
			gls.CommitResult = append(gls.CommitResult, commit)

			ms.AppendGlobalLogStream(gls)
			So(ms.LookupNextGLS(types.GLSN(5)), ShouldNotBeNil)
			So(ms.LookupNextGLS(types.GLSN(10)), ShouldNotBeNil)
			So(ms.GetHighWatermark(), ShouldEqual, types.GLSN(15))
		})
	})
}

func TestStorageMetadataCache(t *testing.T) {
	Convey("cacheCompleteCB should return after make cache", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, nil)
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		timeout := false
		select {
		case <-ch:
		case <-time.After(vtesting.TimeoutUnitTimesFactor(10)):
			timeout = true
		}

		So(timeout, ShouldBeFalse)

		meta := ms.GetMetadata()
		So(meta, ShouldNotBeNil)
		So(meta.GetStorageNode(snID), ShouldNotBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)
	})

	Convey("createMetadataCache should dedup", t, func(ctx C) {
		ch := make(chan struct{}, 2)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, nil)

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
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		<-ch
		meta := ms.GetMetadata()
		So(meta, ShouldNotBeNil)

		So(meta.GetStorageNode(snID), ShouldNotBeNil)
		So(meta.GetStorageNode(snID2), ShouldNotBeNil)

		<-ch
		meta2 := ms.GetMetadata()
		So(meta2, ShouldEqual, meta)

		So(testutil.CompareWaitN(10, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)
	})
}

func TestStorageStateMachineMerge(t *testing.T) {
	Convey("merge stateMachine should not operate while job running", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, nil)

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
			Reset(func() {
				ms.Close()
				testutil.GC()
			})

			<-ch

			So(testutil.CompareWaitN(10, func() bool {
				return atomic.LoadInt64(&ms.nrRunning) == 0
			}), ShouldBeTrue)

			ms.mergeStateMachine()
			So(ms.isCopyOnWrite(), ShouldBeFalse)

			pre, cur = ms.getStateMachine()
			So(pre == cur, ShouldBeTrue)
			So(pre.Metadata.GetStorageNode(snID), ShouldNotBeNil)
			So(cur.Metadata.GetStorageNode(snID), ShouldNotBeNil)
		})
	})

	Convey("merge performance:: # of LocalLogStreams:1024. RepFactor:3:: ", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		for i := 0; i < 1024; i++ {
			lsID := types.LogStreamID(i)

			for j := 0; j < 3; j++ {
				snID := types.StorageNodeID(j)

				s := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i*3),
					UncommittedLLSNLength: 1,
					KnownHighWatermark:    types.InvalidGLSN,
				}

				ms.UpdateLocalLogStreamReplica(lsID, snID, s)
			}
		}

		ms.setCopyOnWrite()

		for i := 0; i < 1024; i++ {
			lsID := types.LogStreamID(i)

			for j := 0; j < 3; j++ {
				snID := types.StorageNodeID(j)

				s := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(1+i*3),
					UncommittedLLSNLength: 1,
					KnownHighWatermark:    types.GLSN(1024),
				}

				ms.UpdateLocalLogStreamReplica(lsID, snID, s)
			}
		}

		ms.releaseCopyOnWrite()

		st := time.Now()
		ms.mergeStateMachine()
		t.Log(time.Now().Sub(st))
	})
}

func TestStorageSnapshot(t *testing.T) {
	Convey("create snapshot should not operate while job running", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, 0, nil)

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
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		<-ch

		snID = snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		<-ch

		snap, _, _ := ms.GetSnapshot()
		So(snap, ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)

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

			snap, _, snapIndex := ms.GetSnapshot()
			So(snap, ShouldNotBeNil)
			So(snapIndex, ShouldEqual, appliedIndex)

			u := &mrpb.MetadataRepositoryDescriptor{}
			err = u.Unmarshal(snap)
			So(err, ShouldBeNil)

			So(u.Metadata.GetStorageNode(snID), ShouldNotBeNil)
			So(u.Metadata.GetStorageNode(snID2), ShouldBeNil)
		})
	})
}

func TestStorageApplySnapshot(t *testing.T) {
	Convey("Given MetadataStorage Snapshop", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, 0, nil)
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		appliedIndex := uint64(0)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		appliedIndex++
		ms.UpdateAppliedIndex(appliedIndex)

		<-ch

		snID = snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		appliedIndex++
		ms.UpdateAppliedIndex(appliedIndex)

		<-ch

		So(testutil.CompareWaitN(10, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)

		ms.mergeStateMachine()
		ms.triggerSnapshot(appliedIndex)

		So(testutil.CompareWaitN(10, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)

		snap, confState, snapIndex := ms.GetSnapshot()
		So(snap, ShouldNotBeNil)
		So(snapIndex, ShouldEqual, appliedIndex)

		Convey("When new MetadataStorage which load snapshot", func(ctx C) {
			loaded := NewMetadataStorage(cb, DefaultSnapshotCount, nil)
			So(loaded.ApplySnapshot(snap, confState, snapIndex), ShouldBeNil)
			Reset(func() {
				loaded.Close()
			})

			Convey("Then MetadataStorage should have same data", func(ctx C) {
				lsnap, _, lsnapIndex := loaded.GetSnapshot()
				So(snapIndex, ShouldEqual, lsnapIndex)
				So(bytes.Compare(snap, lsnap), ShouldEqual, 0)

				meta := ms.GetMetadata()
				lmeta := loaded.GetMetadata()
				So(meta.Equal(lmeta), ShouldBeTrue)
			})
		})
	})
}

func TestStorageSnapshotRace(t *testing.T) {
	Convey("create snapshot", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)
		ms.snapCount = uint64(100 + rand.Int31n(64))

		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		n := 10000
		numLS := 128
		numRep := 3

		lsIDs := make([]types.LogStreamID, numLS)
		snIDs := make([][]types.StorageNodeID, numLS)
		for i := 0; i < numLS; i++ {
			lsIDs[i] = types.LogStreamID(i)
			snIDs[i] = make([]types.StorageNodeID, numRep)
			for j := 0; j < numRep; j++ {
				snIDs[i][j] = types.StorageNodeID(i*numRep + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i][j],
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}

			ls := makeLogStream(lsIDs[i], snIDs[i])
			err := ms.RegisterLogStream(ls, 0, 0)
			So(err, ShouldBeNil)
		}

		appliedIndex := uint64(0)
		checkGLS := 0
		checkLS := 0

		for i := 0; i < n; i++ {
			preGLSN := types.GLSN(i * numLS)
			newGLSN := types.GLSN((i + 1) * numLS)
			gls := &snpb.GlobalLogStreamDescriptor{
				PrevHighWatermark: preGLSN,
				HighWatermark:     newGLSN,
			}

			for j := 0; j < numLS; j++ {
				lsID := types.LogStreamID(j)

				for k := 0; k < numRep; k++ {
					snID := types.StorageNodeID(j*numRep + k)

					r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
						UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i),
						UncommittedLLSNLength: 1,
						KnownHighWatermark:    preGLSN,
					}

					ms.UpdateLocalLogStreamReplica(lsID, snID, r)

					appliedIndex++
					ms.UpdateAppliedIndex(appliedIndex)
				}

				commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
					LogStreamID:         lsID,
					CommittedGLSNOffset: preGLSN + types.GLSN(1),
					CommittedGLSNLength: uint64(numLS),
				}
				gls.CommitResult = append(gls.CommitResult, commit)
			}

			ms.AppendGlobalLogStream(gls)

			appliedIndex++
			ms.UpdateAppliedIndex(appliedIndex)

			gls = ms.LookupNextGLS(preGLSN)
			if gls != nil &&
				gls.HighWatermark == newGLSN &&
				ms.GetHighWatermark() == newGLSN {
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
						r.KnownHighWatermark == preGLSN {
						checkLS++
					}
				}
			}
		}

		So(checkGLS, ShouldEqual, n)
		So(checkLS, ShouldEqual, n*numLS*numRep)

		So(testutil.CompareWaitN(100, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)

		ms.mergeStateMachine()
		ms.triggerSnapshot(appliedIndex)

		So(testutil.CompareWaitN(100, func() bool {
			return atomic.LoadInt64(&ms.nrRunning) == 0
		}), ShouldBeTrue)

		_, _, recv := ms.GetSnapshot()
		So(recv, ShouldEqual, appliedIndex)
	})
}

func TestStorageVerifyReport(t *testing.T) {
	Convey("Given MetadataStorage which has GlobalLogSteams with HWM [10,15,20]", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, nil)

		lsID := types.LogStreamID(time.Now().UnixNano())

		snID := types.StorageNodeID(lsID)
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		ls := makeLogStream(lsID, []types.StorageNodeID{snID})

		err = ms.RegisterLogStream(ls, 0, 0)
		So(err, ShouldBeNil)

		for i := 0; i < 3; i++ {
			gls := &snpb.GlobalLogStreamDescriptor{
				PrevHighWatermark: types.GLSN(i*5 + 5),
				HighWatermark:     types.GLSN(i*5 + 10),
			}

			commit := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
				LogStreamID:         lsID,
				CommittedGLSNOffset: types.GLSN(6 + i*5),
				CommittedGLSNLength: 5,
			}
			gls.CommitResult = append(gls.CommitResult, commit)

			ms.AppendGlobalLogStream(gls)
		}

		Convey("When update report with valid hwm, then it should be succeed", func(ctx C) {
			for i := 0; i < 4; i++ {
				r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i*5),
					UncommittedLLSNLength: 5,
					KnownHighWatermark:    types.GLSN(i*5 + 5),
				}
				So(ms.verifyLocalLogStream(r), ShouldBeTrue)
			}
		})

		Convey("When update report with invalid hwm, then it should be succeed", func(ctx C) {
			for i := 0; i < 5; i++ {
				r := &mrpb.MetadataRepositoryDescriptor_LocalLogStreamReplica{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i*5),
					UncommittedLLSNLength: 5,
					KnownHighWatermark:    types.GLSN(i*5 + 5 - 1),
				}
				So(ms.verifyLocalLogStream(r), ShouldBeFalse)
			}
		})
	})
}
