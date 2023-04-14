package metarepos

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/vtesting"
)

func TestStorageRegisterSN(t *testing.T) {
	Convey("SN should be registered", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
				Address:       "mt_addr",
			},
		}

		err := ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		Convey("SN should not be registered if arleady exist", func(ctx C) {
			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)

			dupSN := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
					Address:       "diff_addr",
				},
			}

			err = ms.registerStorageNode(dupSN)
			So(status.Code(err), ShouldEqual, codes.AlreadyExists)
		})
	})
}

func TestStoragUnregisterSN(t *testing.T) {
	Convey("Given a MetadataStorage", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
		snID := types.StorageNodeID(time.Now().UnixNano())

		Convey("When SN is not exist", func(ctx C) {
			Convey("Then it should not be registered", func(ctx C) {
				err := ms.unregisterStorageNode(snID)
				So(status.Code(err), ShouldEqual, codes.NotFound)
			})
		})

		Convey("Wnen SN is exist", func(ctx C) {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			// simulate copy on write
			ms.setCopyOnWrite()

			Convey("Then it should be unregistered", func(ctx C) {
				err = ms.unregisterStorageNode(snID)
				So(err, ShouldBeNil)
				So(ms.lookupStorageNode(snID), ShouldBeNil)

				ms.createMetadataCache(&jobMetadataCache{})
				meta := ms.GetMetadata()
				So(len(meta.GetStorageNodes()), ShouldEqual, 0)
				So(len(ms.diffStateMachine.Metadata.StorageNodes), ShouldEqual, 1)

				Convey("unregistered SN should not be found after merge", func(ctx C) {
					ms.mergeMetadata()
					ms.mergeLogStream()

					ms.releaseCopyOnWrite()

					So(ms.lookupStorageNode(snID), ShouldBeNil)
					So(len(ms.diffStateMachine.Metadata.StorageNodes), ShouldEqual, 0)
				})
			})

			Convey("And LS which have the SN as replica is exist", func(ctx C) {
				rep := 1

				err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
				So(err, ShouldBeNil)

				lsID := types.LogStreamID(time.Now().UnixNano())
				snIDs := make([]types.StorageNodeID, rep)
				for i := 0; i < rep; i++ {
					snIDs[i] = snID + types.StorageNodeID(i)
				}
				ls := makeLogStream(types.TopicID(1), lsID, snIDs)

				err = ms.registerLogStream(ls)
				So(err, ShouldBeNil)

				Convey("Then SN should not be unregistered", func(ctx C) {
					err := ms.unregisterStorageNode(snID)
					So(status.Code(err), ShouldEqual, codes.FailedPrecondition)
				})
			})
		})
	})
}

func TestStoragGetAllSN(t *testing.T) {
	Convey("Given a MetadataStorage", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		Convey("When SN is not exist", func(ctx C) {
			Convey("Then it returns nil", func(ctx C) {
				So(len(ms.GetStorageNodes()), ShouldEqual, 0)
			})
		})

		Convey("Wnen SN register", func(ctx C) {
			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			ms.setCopyOnWrite()

			Convey("Then it should return 1 SN", func(ctx C) {
				sns := ms.GetStorageNodes()
				So(len(sns), ShouldEqual, 1)

				Convey("Wnen one more SN register to diff", func(ctx C) {
					snID2 := types.StorageNodeID(time.Now().UnixNano())
					sn := &varlogpb.StorageNodeDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snID2,
						},
					}

					err := ms.RegisterStorageNode(sn, 0, 0)
					So(err, ShouldBeNil)

					Convey("Then it should return 2 SNs", func(ctx C) {
						sns := ms.GetStorageNodes()
						So(len(sns), ShouldEqual, 2)

						Convey("When unregister SN which registered orig", func(ctx C) {
							err = ms.unregisterStorageNode(snID)
							So(err, ShouldBeNil)

							So(ms.lookupStorageNode(snID), ShouldBeNil)

							Convey("Then it should returns 1 SNs", func(ctx C) {
								sns := ms.GetStorageNodes()
								So(len(sns), ShouldEqual, 1)

								Convey("When merge Metadata", func(ctx C) {
									ms.mergeMetadata()

									ms.releaseCopyOnWrite()

									Convey("Then it should returns 1 SNs", func(ctx C) {
										sns := ms.GetStorageNodes()
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

func TestStoragGetAllLS(t *testing.T) {
	Convey("Given a MetadataStorage", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		Convey("When LS is not exist", func(ctx C) {
			Convey("Then it returns nil", func(ctx C) {
				So(len(ms.GetLogStreams()), ShouldEqual, 0)
			})
		})

		Convey("Wnen LS register", func(ctx C) {
			snID := types.StorageNodeID(time.Now().UnixNano())
			lsID := types.LogStreamID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			err = ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)

			ls := &varlogpb.LogStreamDescriptor{
				TopicID:     types.TopicID(1),
				LogStreamID: lsID,
			}

			ls.Replicas = append(ls.Replicas, &varlogpb.ReplicaDescriptor{StorageNodeID: snID})

			err = ms.RegisterLogStream(ls, 0, 0)
			So(err, ShouldBeNil)

			ms.setCopyOnWrite()

			Convey("Then it should return 1 LS", func(ctx C) {
				lss := ms.GetLogStreams()
				So(len(lss), ShouldEqual, 1)

				Convey("When unregister LS", func(ctx C) {
					err := ms.unregisterLogStream(lsID)
					So(err, ShouldBeNil)

					Convey("Then it should return nil", func(ctx C) {
						lss := ms.GetLogStreams()
						So(len(lss), ShouldEqual, 0)

						Convey("When merge Metadata", func(ctx C) {
							ms.mergeMetadata()

							ms.releaseCopyOnWrite()

							Convey("Then it should returns nil", func(ctx C) {
								lss := ms.GetLogStreams()
								So(len(lss), ShouldEqual, 0)
							})
						})
					})
				})
			})
		})
	})
}

func TestStorageRegisterLS(t *testing.T) {
	Convey("LS which has no SN should not be registered", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		ls := makeLogStream(types.TopicID(1), lsID, nil)

		err = ms.registerLogStream(ls)
		So(status.Code(err), ShouldEqual, codes.InvalidArgument)
	})

	Convey("LS should not be registered if not exist proper SN", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		rep := 2

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		lsID := types.LogStreamID(time.Now().UnixNano())
		tmp := types.StorageNodeID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = tmp + types.StorageNodeID(i)
		}
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err = ms.registerLogStream(ls)
		So(status.Code(err), ShouldEqual, codes.NotFound)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snIDs[0],
			},
		}

		err = ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		err = ms.registerLogStream(ls)
		So(status.Code(err), ShouldEqual, codes.NotFound)

		Convey("LS should be registered if exist all SN", func(ctx C) {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[1],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)

			err = ms.registerLogStream(ls)
			So(err, ShouldBeNil)

			Convey("registered LS should be lookuped", func(ctx C) {
				for i := 0; i < rep; i++ {
					_, ok := ms.LookupUncommitReport(lsID, snIDs[i])
					So(ok, ShouldBeTrue)
				}
			})
		})
	})
}

func TestStoragUnregisterLS(t *testing.T) {
	Convey("LS which is not exist should not be unregistered", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		lsID := types.LogStreamID(time.Now().UnixNano())

		err := ms.unregisterLogStream(lsID)
		So(status.Code(err), ShouldEqual, codes.NotFound)

		Convey("LS which is exist should be unregistered", func(ctx C) {
			rep := 1

			err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)

			snIDs := make([]types.StorageNodeID, rep)
			tmp := types.StorageNodeID(time.Now().UnixNano())
			for i := 0; i < rep; i++ {
				snIDs[i] = tmp + types.StorageNodeID(i)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i],
					},
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
			ls := makeLogStream(types.TopicID(1), lsID, snIDs)

			err = ms.RegisterLogStream(ls, 0, 0)
			So(err, ShouldBeNil)

			ms.setCopyOnWrite()

			So(len(ms.GetSortedTopicLogStreamIDs()), ShouldEqual, 1)

			err = ms.unregisterLogStream(lsID)
			So(err, ShouldBeNil)

			So(ms.lookupLogStream(lsID), ShouldBeNil)
			So(ms.LookupUncommitReports(lsID), ShouldBeNil)
			So(len(ms.GetSortedTopicLogStreamIDs()), ShouldEqual, 0)

			Convey("unregistered SN should not be found after merge", func(ctx C) {
				ms.mergeMetadata()
				ms.mergeLogStream()

				ms.releaseCopyOnWrite()

				So(ms.lookupLogStream(lsID), ShouldBeNil)
				So(ms.LookupUncommitReports(lsID), ShouldBeNil)
				So(len(ms.GetSortedTopicLogStreamIDs()), ShouldEqual, 0)
			})
		})
	})
}

func TestStorageUpdateLS(t *testing.T) {
	Convey("LS should not be updated if not exist proper SN", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
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
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err = ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		err = ms.SealLogStream(lsID, 0, 0)
		So(err, ShouldBeNil)

		victimSNID := snIDs[0]
		updateSnIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			updateSnIDs[i] = snIDs[i] + types.StorageNodeID(1)
		}

		updateLS := makeLogStream(types.TopicID(1), lsID, updateSnIDs)

		err = ms.UpdateLogStream(updateLS, 0, 0)
		So(status.Code(err), ShouldEqual, codes.NotFound)

		Convey("LS should be updated if exist all SN", func(ctx C) {
			for i := 1; i < rep; i++ {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: updateSnIDs[i],
					},
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}

			err = ms.UpdateLogStream(updateLS, 0, 0)
			So(err, ShouldBeNil)
			Convey("updated LS should be lookuped", func(ctx C) {
				_, ok := ms.LookupUncommitReport(lsID, victimSNID)
				So(ok, ShouldBeFalse)

				for i := 0; i < rep; i++ {
					_, ok = ms.LookupUncommitReport(lsID, updateSnIDs[i])
					So(ok, ShouldBeTrue)
				}

				So(testutil.CompareWaitN(10, func() bool {
					return ms.nrRunning.Load() == 0
				}), ShouldBeTrue)

				meta := ms.GetMetadata()
				So(meta, ShouldNotBeNil)

				ls := meta.GetLogStream(lsID)
				So(ls, ShouldNotBeNil)

				for _, r := range ls.Replicas {
					So(r.StorageNodeID, ShouldNotEqual, victimSNID)
				}
			})
		})
	})
}

func TestStorageUpdateLSUnderCOW(t *testing.T) {
	Convey("update LS to COW storage should applyed after merge", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		rep := 2

		// register LS
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err = ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		err = ms.SealLogStream(lsID, 0, 0)
		So(err, ShouldBeNil)

		// set COW
		ms.setCopyOnWrite()

		victimSNID := snIDs[0]
		// update LS
		updateSnIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			updateSnIDs[i] = snIDs[i] + types.StorageNodeID(1)

			if updateSnIDs[i] != victimSNID {
				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: updateSnIDs[i],
					},
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		updateLS := makeLogStream(types.TopicID(1), lsID, updateSnIDs)

		err = ms.UpdateLogStream(updateLS, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		// compare
		diffls := ms.diffStateMachine.Metadata.GetLogStream(lsID)
		difflls := ms.diffStateMachine.LogStream.UncommitReports[lsID]

		origls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		origlls := ms.origStateMachine.LogStream.UncommitReports[lsID]

		So(diffls.Equal(origls), ShouldBeFalse)
		So(difflls.Equal(origlls), ShouldBeFalse)

		// merge
		ms.mergeMetadata()
		ms.mergeLogStream()

		ms.releaseCopyOnWrite()

		// compare
		mergedls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		mergedlls := ms.origStateMachine.LogStream.UncommitReports[lsID]

		So(diffls.Equal(mergedls), ShouldBeTrue)
		So(difflls.Equal(mergedlls), ShouldBeTrue)
	})
}

func TestStorageSealLS(t *testing.T) {
	Convey("LS should not be sealed if not exist", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		lsID := types.LogStreamID(time.Now().UnixNano())
		err := ms.SealLogStream(lsID, 0, 0)
		So(status.Code(err), ShouldEqual, codes.NotFound)
	})

	Convey("For resigtered LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
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
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err = ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("Seal should be success", func(ctx C) {
			err = ms.SealLogStream(lsID, 0, 0)
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(10, func() bool {
				return ms.nrRunning.Load() == 0
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

		Convey("Sealed UncommitReportReplica should have same EndLLSN", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				return ms.nrRunning.Load() == 0
			}), ShouldBeTrue)

			for i := 0; i < rep; i++ {
				r := snpb.LogStreamUncommitReport{
					UncommittedLLSNOffset: types.MinLLSN,
					UncommittedLLSNLength: uint64(i),
					Version:               types.InvalidVersion,
				}

				ms.UpdateUncommitReport(lsID, snIDs[i], r)
				_, ok := ms.LookupUncommitReport(lsID, snIDs[i])
				So(ok, ShouldBeTrue)
				So(r.UncommittedLLSNEnd(), ShouldEqual, r.UncommittedLLSNEnd())
			}

			err = ms.SealLogStream(lsID, 0, 0)
			So(err, ShouldBeNil)

			for i := 0; i < rep; i++ {
				r, ok := ms.LookupUncommitReport(lsID, snIDs[i])
				So(ok, ShouldBeTrue)
				So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN)
			}

			Convey("Sealed UncommitReportReplica should ignore report", func(ctx C) {
				for i := 0; i < rep; i++ {
					r := snpb.LogStreamUncommitReport{
						UncommittedLLSNOffset: types.MinLLSN,
						UncommittedLLSNLength: uint64(i + 1),
						Version:               types.InvalidVersion,
					}

					ms.UpdateUncommitReport(lsID, snIDs[i], r)
					rr, ok := ms.LookupUncommitReport(lsID, snIDs[i])
					So(ok, ShouldBeTrue)
					So(rr.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN)
				}
			})
		})
	})
}

func TestStorageSealLSUnderCOW(t *testing.T) {
	Convey("seal LS to COW storage should applyed after merge", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		rep := 2

		// register LS
		lsID := types.LogStreamID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err = ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		// set COW
		ms.setCopyOnWrite()

		// seal
		err = ms.SealLogStream(lsID, 0, 0)
		So(err, ShouldBeNil)

		// compare
		diffls := ms.diffStateMachine.Metadata.GetLogStream(lsID)
		difflls := ms.diffStateMachine.LogStream.UncommitReports[lsID]

		origls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		origlls := ms.origStateMachine.LogStream.UncommitReports[lsID]

		So(diffls.Equal(origls), ShouldBeFalse)
		So(difflls.Equal(origlls), ShouldBeFalse)

		// merge
		ms.mergeMetadata()
		ms.mergeLogStream()

		ms.releaseCopyOnWrite()

		// compare
		mergedls := ms.origStateMachine.Metadata.GetLogStream(lsID)
		mergedlls := ms.origStateMachine.LogStream.UncommitReports[lsID]

		So(diffls.Equal(mergedls), ShouldBeTrue)
		So(difflls.Equal(mergedlls), ShouldBeTrue)

		So(mergedls.Status, ShouldEqual, varlogpb.LogStreamStatusSealed)
		So(mergedlls.Status, ShouldEqual, varlogpb.LogStreamStatusSealed)
	})
}

func TestStorageUnsealLS(t *testing.T) {
	Convey("Storage should return ErrNotExsit if Unseal to not exist LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		lsID := types.LogStreamID(time.Now().UnixNano())
		err := ms.UnsealLogStream(lsID, 0, 0)
		So(status.Code(err), ShouldEqual, codes.NotFound)
	})

	Convey("For resigtered LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
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
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err = ms.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("Unseal to LS which is already Unsealed should return nil", func(ctx C) {
			err := ms.UnsealLogStream(lsID, 0, 0)
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(10, func() bool {
				return ms.nrRunning.Load() == 0
			}), ShouldBeTrue)

			Convey("Unsealed to sealed LS should be success", func(ctx C) {
				err := ms.SealLogStream(lsID, 0, 0)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(10, func() bool {
					return ms.nrRunning.Load() == 0
				}), ShouldBeTrue)

				err = ms.UnsealLogStream(lsID, 0, 0)
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(10, func() bool {
					return ms.nrRunning.Load() == 0
				}), ShouldBeTrue)

				meta := ms.GetMetadata()
				So(meta, ShouldNotBeNil)

				ls := meta.GetLogStream(lsID)
				So(ls, ShouldNotBeNil)
				So(ls.Status, ShouldEqual, varlogpb.LogStreamStatusRunning)

				Convey("Unsealed LS should update report", func(ctx C) {
					for i := 0; i < rep; i++ {
						r := snpb.LogStreamUncommitReport{
							UncommittedLLSNOffset: types.MinLLSN,
							UncommittedLLSNLength: uint64(i),
							Version:               types.InvalidVersion,
						}

						ms.UpdateUncommitReport(lsID, snIDs[i], r)
						rr, ok := ms.LookupUncommitReport(lsID, snIDs[i])
						So(ok, ShouldBeTrue)
						So(rr.UncommittedLLSNEnd(), ShouldEqual, r.UncommittedLLSNEnd())
					}
				})
			})
		})
	})
}

func TestStorageTrim(t *testing.T) {
	Convey("Given a GlobalLogStreams", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		for ver := types.MinVersion; ver < types.Version(1024); ver++ {
			gls := &mrpb.LogStreamCommitResults{
				Version: ver,
			}

			gls.CommitResults = append(gls.CommitResults, snpb.LogStreamCommitResult{
				CommittedGLSNOffset: types.GLSN(ver),
				CommittedGLSNLength: 1,
			})

			ms.AppendLogStreamCommitHistory(gls)
		}

		Convey("When operate trim, trimmed gls should not be found", func(ctx C) {
			for trim := types.InvalidVersion; trim < types.Version(1024); trim++ {
				ms.TrimLogStreamCommitHistory(trim) //nolint:errcheck,revive // TODO:: Handle an error returned.
				ms.trimLogStreamCommitHistory()

				if trim > types.MinVersion {
					So(ms.getFirstCommitResultsNoLock().GetVersion(), ShouldEqual, trim-types.MinVersion)
				}
			}
		})
	})
}

func TestStorageReport(t *testing.T) {
	Convey("storage should not apply report if not registered LS", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		tmp := types.StorageNodeID(time.Now().UnixNano())
		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = tmp + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		notExistSnID := tmp + types.StorageNodeID(rep)

		r := snpb.LogStreamUncommitReport{
			UncommittedLLSNOffset: types.MinLLSN,
			UncommittedLLSNLength: 5,
			Version:               types.InvalidVersion,
		}

		for i := 0; i < rep; i++ {
			ms.UpdateUncommitReport(lsID, snIDs[i], r)
			_, ok := ms.LookupUncommitReport(lsID, snIDs[i])
			So(ok, ShouldBeFalse)
		}

		Convey("storage should not apply report if snID is not exist in LS", func(ctx C) {
			err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)

			ls := makeLogStream(types.TopicID(1), lsID, snIDs)
			ms.registerLogStream(ls) //nolint:errcheck,revive // TODO:: Handle an error returned.

			r := snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.MinLLSN,
				UncommittedLLSNLength: 5,
				Version:               types.InvalidVersion,
			}

			ms.UpdateUncommitReport(lsID, notExistSnID, r)
			_, ok := ms.LookupUncommitReport(lsID, notExistSnID)
			So(ok, ShouldBeFalse)

			Convey("storage should apply report if snID is exist in LS", func(ctx C) {
				r := snpb.LogStreamUncommitReport{
					UncommittedLLSNOffset: types.MinLLSN,
					UncommittedLLSNLength: 5,
					Version:               types.InvalidVersion,
				}

				for i := 0; i < rep; i++ {
					ms.UpdateUncommitReport(lsID, snIDs[i], r)
					_, ok := ms.LookupUncommitReport(lsID, snIDs[i])
					So(ok, ShouldBeTrue)
				}
			})
		})
	})
}

func TestStorageCopyOnWrite(t *testing.T) {
	Convey("storage should returns different stateMachine while copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		pre, cur := ms.getStateMachine()
		So(pre == cur, ShouldBeTrue)

		ms.setCopyOnWrite()

		pre, cur = ms.getStateMachine()
		So(pre == cur, ShouldBeFalse)
	})

	Convey("update matadata should make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)
	})

	Convey("copyOnWrite storage should give the same response for registerStorageNode", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
				Address:       "my_addr",
			},
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		pre, cur := ms.getStateMachine()
		So(pre.Metadata.GetStorageNode(snID), ShouldNotBeNil)
		So(cur.Metadata.GetStorageNode(snID), ShouldBeNil)

		sn = &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
				Address:       "diff_addr",
			},
		}
		err = ms.RegisterStorageNode(sn, 0, 0)
		So(status.Code(err), ShouldEqual, codes.AlreadyExists)

		snID2 := snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID2,
			},
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		So(pre.Metadata.GetStorageNode(snID2), ShouldBeNil)
		So(cur.Metadata.GetStorageNode(snID2), ShouldNotBeNil)
	})

	Convey("copyOnWrite storage should give the same response for createLogStream", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		rep := 2
		lsID := types.LogStreamID(time.Now().UnixNano())
		lsID2 := lsID + types.LogStreamID(1)

		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = types.StorageNodeID(lsID) + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		ls2 := makeLogStream(types.TopicID(1), lsID2, snIDs)

		err = ms.RegisterLogStream(ls, 0, 0)
		So(err, ShouldBeNil)
		ms.setCopyOnWrite()

		pre, cur := ms.getStateMachine()
		So(pre.Metadata.GetLogStream(lsID), ShouldNotBeNil)
		So(cur.Metadata.GetLogStream(lsID), ShouldBeNil)

		conflict := makeLogStream(types.TopicID(1), lsID, snIDs)
		ls.Replicas[0].StorageNodeID += 100
		err = ms.RegisterLogStream(conflict, 0, 0)
		So(status.Code(err), ShouldEqual, codes.AlreadyExists)

		err = ms.RegisterLogStream(ls2, 0, 0)
		So(err, ShouldBeNil)

		So(pre.Metadata.GetLogStream(lsID2), ShouldBeNil)
		So(cur.Metadata.GetLogStream(lsID2), ShouldNotBeNil)
	})

	Convey("update UncommitReport does not make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
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
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		ms.registerLogStream(ls) //nolint:errcheck,revive // TODO:: Handle an error returned.

		r := snpb.LogStreamUncommitReport{
			UncommittedLLSNOffset: types.MinLLSN,
			UncommittedLLSNLength: 5,
			Version:               types.MinVersion,
		}
		ms.UpdateUncommitReport(lsID, snIDs[0], r)
		So(ms.isCopyOnWrite(), ShouldBeFalse)

		for i := 0; i < rep; i++ {
			_, ok := ms.LookupUncommitReport(lsID, snIDs[i])
			So(ok, ShouldBeTrue)
		}

		Convey("lookup UncommitReport with copyOnWrite should give merged response", func(ctx C) {
			ms.setCopyOnWrite()

			r := snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.MinLLSN,
				UncommittedLLSNLength: 5,
				Version:               types.MinVersion,
			}
			ms.UpdateUncommitReport(lsID, snIDs[1], r)

			for i := 0; i < rep; i++ {
				_, ok := ms.LookupUncommitReport(lsID, snIDs[i])
				So(ok, ShouldBeTrue)
			}
		})
	})

	Convey("update GlobalLogStream does not make storage copyOnWrite", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		gls := &mrpb.LogStreamCommitResults{
			Version: types.Version(2),
		}

		lsID := types.LogStreamID(time.Now().UnixNano())
		commit := snpb.LogStreamCommitResult{
			LogStreamID:         lsID,
			CommittedGLSNOffset: types.GLSN(6),
			CommittedGLSNLength: 5,
		}
		gls.CommitResults = append(gls.CommitResults, commit)

		ms.AppendLogStreamCommitHistory(gls)
		So(ms.isCopyOnWrite(), ShouldBeFalse)
		cr, _ := ms.LookupNextCommitResults(types.MinVersion)
		So(cr, ShouldNotBeNil)
		So(ms.GetLastCommitVersion(), ShouldEqual, types.Version(2))

		Convey("lookup GlobalLogStream with copyOnWrite should give merged response", func(ctx C) {
			ms.setCopyOnWrite()

			gls := &mrpb.LogStreamCommitResults{
				Version: types.Version(3),
			}

			commit := snpb.LogStreamCommitResult{
				LogStreamID:         lsID,
				CommittedGLSNOffset: types.GLSN(11),
				CommittedGLSNLength: 5,
			}
			gls.CommitResults = append(gls.CommitResults, commit)

			ms.AppendLogStreamCommitHistory(gls)
			cr, _ := ms.LookupNextCommitResults(types.MinVersion)
			So(cr, ShouldNotBeNil)
			cr, _ = ms.LookupNextCommitResults(types.Version(2))
			So(cr, ShouldNotBeNil)
			So(ms.GetLastCommitVersion(), ShouldEqual, types.Version(3))
		})
	})
}

func TestStorageMetadataCache(t *testing.T) {
	Convey("metadata storage returns empty cache, when it has no metadata", t, func(ctx C) {
		cb := func(uint64, uint64, error) {}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, zap.NewNop())
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		m := ms.GetMetadata()
		So(m, ShouldNotBeNil)
		So(m.GetStorageNodes(), ShouldBeNil)
		So(m.GetLogStreams(), ShouldBeNil)
	})

	Convey("cacheCompleteCB should return after make cache", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, zap.NewNop())
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
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
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)
	})

	Convey("createMetadataCache should dedup", t, func(ctx C) {
		ch := make(chan struct{}, 2)
		cb := func(uint64, uint64, error) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, zap.NewNop())

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		ms.setCopyOnWrite()
		ms.nrRunning.Add(1)
		ms.jobC <- &storageAsyncJob{job: &jobMetadataCache{}}

		snID2 := snID + types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID2,
			},
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		ms.nrRunning.Add(1)
		ms.jobC <- &storageAsyncJob{job: &jobMetadataCache{}}

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
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)
	})
}

func TestStorageStateMachineMerge(t *testing.T) {
	Convey("merge stateMachine should not operate while job running", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}

		ms := NewMetadataStorage(cb, DefaultSnapshotCount, zap.NewNop())

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		ms.setCopyOnWrite()
		ms.nrRunning.Add(1)
		ms.jobC <- &storageAsyncJob{job: &jobMetadataCache{}}

		ms.mergeStateMachine()
		So(ms.isCopyOnWrite(), ShouldBeTrue)

		Convey("merge stateMachine should operate if no more job", func(ctx C) {
			ms.Run()
			Reset(func() {
				ms.Close()
				testutil.GC()
			})

			snID += types.StorageNodeID(1)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)

			pre, cur := ms.getStateMachine()
			So(pre == cur, ShouldBeFalse)
			So(pre.Metadata.GetStorageNode(snID), ShouldBeNil)
			So(cur.Metadata.GetStorageNode(snID), ShouldNotBeNil)

			<-ch

			So(testutil.CompareWaitN(10, func() bool {
				return ms.nrRunning.Load() == 0
			}), ShouldBeTrue)

			ms.mergeStateMachine()
			So(ms.isCopyOnWrite(), ShouldBeFalse)

			pre, cur = ms.getStateMachine()
			So(pre == cur, ShouldBeTrue)
			So(pre.Metadata.GetStorageNode(snID), ShouldNotBeNil)
			So(cur.Metadata.GetStorageNode(snID), ShouldNotBeNil)
		})
	})

	Convey("merge performance:: # of UncommitReports:1024. RepFactor:3:: ", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		for i := 0; i < 1024; i++ {
			lsID := types.LogStreamID(i)

			for j := 0; j < 3; j++ {
				snID := types.StorageNodeID(j)

				s := snpb.LogStreamUncommitReport{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i*3),
					UncommittedLLSNLength: 1,
					Version:               types.InvalidVersion,
				}

				ms.UpdateUncommitReport(lsID, snID, s)
			}
		}

		ms.setCopyOnWrite()

		for i := 0; i < 1024; i++ {
			lsID := types.LogStreamID(i)

			for j := 0; j < 3; j++ {
				snID := types.StorageNodeID(j)

				s := snpb.LogStreamUncommitReport{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(1+i*3),
					UncommittedLLSNLength: 1,
					Version:               types.Version(1),
				}

				ms.UpdateUncommitReport(lsID, snID, s)
			}
		}

		ms.releaseCopyOnWrite()

		ms.mergeStateMachine()
	})
}

func TestStorageSnapshot(t *testing.T) {
	Convey("create snapshot should not operate while job running", t, func(ctx C) {
		ch := make(chan struct{})
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, 1, zap.NewNop())
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		appliedIndex := uint64(0)

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		appliedIndex++
		ms.UpdateAppliedIndex(appliedIndex)
		ms.triggerSnapshot(appliedIndex)

		<-ch

		snID += types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		<-ch

		snap, _, _ := ms.GetSnapshot()
		So(snap, ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)

		Convey("create snapshot should operate if no more job", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				return ms.nrRunning.Load() == 0
			}), ShouldBeTrue)

			appliedIndex++
			ms.UpdateAppliedIndex(appliedIndex)
			ms.triggerSnapshot(appliedIndex)

			snID2 := snID + types.StorageNodeID(1)
			sn = &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID2,
				},
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

		ms := NewMetadataStorage(cb, 1, zap.NewNop())
		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		appliedIndex := uint64(0)

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		appliedIndex++
		ms.UpdateAppliedIndex(appliedIndex)

		<-ch

		snID += types.StorageNodeID(1)
		sn = &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err = ms.RegisterStorageNode(sn, 0, 0)
		So(err, ShouldBeNil)

		appliedIndex++
		ms.UpdateAppliedIndex(appliedIndex)

		<-ch

		So(testutil.CompareWaitN(10, func() bool {
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)

		ms.mergeStateMachine()
		ms.triggerSnapshot(appliedIndex)

		So(testutil.CompareWaitN(10, func() bool {
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)

		snap, confState, snapIndex := ms.GetSnapshot()
		So(snap, ShouldNotBeNil)
		So(snapIndex, ShouldEqual, appliedIndex)

		Convey("When new MetadataStorage which load snapshot", func(ctx C) {
			loaded := NewMetadataStorage(cb, DefaultSnapshotCount, zap.NewNop())
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
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())
		ms.snapCount = uint64(100 + rand.Int31n(64))

		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		n := 10000
		numLS := 128
		numRep := 3

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		lsIDs := make([]types.LogStreamID, numLS)
		snIDs := make([][]types.StorageNodeID, numLS)
		for i := 0; i < numLS; i++ {
			lsIDs[i] = types.LogStreamID(i)
			snIDs[i] = make([]types.StorageNodeID, numRep)
			for j := 0; j < numRep; j++ {
				snIDs[i][j] = types.StorageNodeID(i*numRep + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i][j],
					},
				}

				err := ms.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}

			ls := makeLogStream(types.TopicID(1), lsIDs[i], snIDs[i])
			err := ms.RegisterLogStream(ls, 0, 0)
			So(err, ShouldBeNil)
		}

		appliedIndex := uint64(0)
		checkGLS := 0
		checkLS := 0

		for i := 0; i < n; i++ {
			preVersion := types.Version(i)
			newVersion := types.Version(i + 1)
			gls := &mrpb.LogStreamCommitResults{
				Version: newVersion,
			}

			for j := 0; j < numLS; j++ {
				lsID := types.LogStreamID(j)

				for k := 0; k < numRep; k++ {
					snID := types.StorageNodeID(j*numRep + k)

					r := snpb.LogStreamUncommitReport{
						UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i),
						UncommittedLLSNLength: 1,
						Version:               preVersion,
					}

					ms.UpdateUncommitReport(lsID, snID, r)

					appliedIndex++
					ms.UpdateAppliedIndex(appliedIndex)
				}

				commit := snpb.LogStreamCommitResult{
					LogStreamID:         lsID,
					CommittedGLSNOffset: types.GLSN(preVersion + 1),
					CommittedGLSNLength: uint64(numLS),
				}
				gls.CommitResults = append(gls.CommitResults, commit)
			}

			ms.AppendLogStreamCommitHistory(gls)

			appliedIndex++
			ms.UpdateAppliedIndex(appliedIndex)

			gls, _ = ms.LookupNextCommitResults(preVersion)
			if gls != nil &&
				gls.Version == newVersion &&
				ms.GetLastCommitVersion() == newVersion {
				checkGLS++
			}

		CHECKLS:
			for j := 0; j < numLS; j++ {
				lsID := types.LogStreamID(j)
				ls := ms.LookupUncommitReports(lsID)
				if ls == nil {
					continue CHECKLS
				}

				for k := 0; k < numRep; k++ {
					snID := types.StorageNodeID(j*numRep + k)

					r, ok := ls.Replicas[snID]
					if ok &&
						r.Version == preVersion {
						checkLS++
					}
				}
			}
		}

		So(checkGLS, ShouldEqual, n)
		So(checkLS, ShouldEqual, n*numLS*numRep)

		So(testutil.CompareWaitN(100, func() bool {
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)

		ms.mergeStateMachine()
		ms.triggerSnapshot(appliedIndex)

		So(testutil.CompareWaitN(100, func() bool {
			return ms.nrRunning.Load() == 0
		}), ShouldBeTrue)

		_, _, recv := ms.GetSnapshot()
		So(recv, ShouldEqual, appliedIndex)
	})
}

func TestStorageVerifyReport(t *testing.T) {
	Convey("Given MetadataStorage which has GlobalLogSteams with HWM [10,15,20]", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		lsID := types.LogStreamID(time.Now().UnixNano())

		snID := types.StorageNodeID(lsID)
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		err = ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, []types.StorageNodeID{snID})

		err = ms.RegisterLogStream(ls, 0, 0)
		So(err, ShouldBeNil)

		for i := 1; i < 4; i++ {
			gls := &mrpb.LogStreamCommitResults{
				Version: types.Version(i + 1),
			}

			commit := snpb.LogStreamCommitResult{
				LogStreamID:         lsID,
				CommittedGLSNOffset: types.GLSN(6 + i*5),
				CommittedGLSNLength: 5,
			}
			gls.CommitResults = append(gls.CommitResults, commit)

			ms.AppendLogStreamCommitHistory(gls)
		}

		Convey("When update report with valid version, then it should be succeed", func(ctx C) {
			for i := 1; i < 5; i++ {
				r := snpb.LogStreamUncommitReport{
					UncommittedLLSNOffset: types.MinLLSN + types.LLSN(i*5),
					UncommittedLLSNLength: 5,
					Version:               types.Version(i),
				}
				So(ms.verifyUncommitReport(r), ShouldBeTrue)
			}
		})

		Convey("When update report with invalid version, then it should not be succeed", func(ctx C) {
			r := snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.MinLLSN + types.LLSN(5),
				UncommittedLLSNLength: 5,
				Version:               types.Version(0),
			}
			So(ms.verifyUncommitReport(r), ShouldBeFalse)

			r = snpb.LogStreamUncommitReport{
				UncommittedLLSNOffset: types.MinLLSN + types.LLSN(5),
				UncommittedLLSNLength: 5,
				Version:               types.Version(5),
			}
			So(ms.verifyUncommitReport(r), ShouldBeFalse)
		})
	})
}

func TestStorageRecoverStateMachine(t *testing.T) {
	Convey("Given MetadataStorage", t, func(ctx C) {
		ch := make(chan struct{}, 1)
		cb := func(uint64, uint64, error) {
			ch <- struct{}{}
		}

		ms := NewMetadataStorage(cb, 1, zap.NewNop())

		appliedIndex := uint64(0)

		nrSN := 5
		snIDs := make([]types.StorageNodeID, nrSN)
		base := types.StorageNodeID(time.Now().UnixNano())
		for i := 0; i < nrSN; i++ {
			snIDs[i] = base + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.RegisterStorageNode(sn, 0, 0)
			So(err, ShouldBeNil)
			ms.setCopyOnWrite()

			appliedIndex++
			ms.UpdateAppliedIndex(appliedIndex)
		}

		appliedIndex++
		err := ms.AddPeer(types.NodeID(0), "test", false, &raftpb.ConfState{}, appliedIndex)
		So(err, ShouldBeNil)
		ms.UpdateAppliedIndex(appliedIndex)

		err = ms.RegisterEndpoint(types.NodeID(0), "test", 0, 0)
		So(err, ShouldBeNil)

		<-ch

		ms.Run()
		Reset(func() {
			ms.Close()
			testutil.GC()
		})

		Convey("When recover state machine ", func(ctx C) {
			stateMachine := &mrpb.MetadataRepositoryDescriptor{}
			stateMachine.Metadata = &varlogpb.MetadataDescriptor{}
			stateMachine.LogStream = &mrpb.MetadataRepositoryDescriptor_LogStreamDescriptor{}
			stateMachine.LogStream.UncommitReports = make(map[types.LogStreamID]*mrpb.LogStreamUncommitReports)

			stateMachine.PeersMap.Peers = make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
			stateMachine.Endpoints = make(map[types.NodeID]string)

			newAppliedIndex := uint64(1)

			err := ms.RecoverStateMachine(stateMachine, newAppliedIndex, 0, 0)
			So(err, ShouldBeNil)

			Convey("Then MetadataStorage should recover", func(ctx C) {
				<-ch

				So(ms.appliedIndex, ShouldEqual, newAppliedIndex)
				So(ms.LookupEndpoint(types.NodeID(0)), ShouldEqual, "test")
				So(ms.IsMember(types.NodeID(0)), ShouldBeTrue)
				for i := 0; i < nrSN; i++ {
					So(ms.lookupStorageNode(snIDs[i]), ShouldBeNil)
				}
				So(ms.running.Load(), ShouldBeTrue)
			})
		})
	})
}

func TestStorageRegisterTopic(t *testing.T) {
	Convey("Topic should be registered if not existed", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		Convey("Topic should be registered even though it already exist", func(ctx C) {
			err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)
		})
	})

	Convey("LS should not be registered if not exist topic", t, func(ctx C) {
		rep := 2
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		lsID := types.LogStreamID(time.Now().UnixNano())
		tmp := types.StorageNodeID(time.Now().UnixNano())

		snIDs := make([]types.StorageNodeID, rep)
		for i := 0; i < rep; i++ {
			snIDs[i] = tmp + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := ms.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		err := ms.registerLogStream(ls)
		So(status.Code(err), ShouldEqual, codes.NotFound)

		Convey("LS should be registered if exist topic", func(ctx C) {
			err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)

			err = ms.registerLogStream(ls)
			So(err, ShouldBeNil)

			for i := 1; i < 10; i++ {
				lsID += types.LogStreamID(1)
				ls := makeLogStream(types.TopicID(1), lsID, snIDs)

				err := ms.registerLogStream(ls)
				So(err, ShouldBeNil)
			}

			topic := ms.lookupTopic(types.TopicID(1))
			So(topic, ShouldNotBeNil)

			So(len(topic.LogStreams), ShouldEqual, 10)
		})
	})
}

func TestStoragUnregisterTopic(t *testing.T) {
	Convey("unregister non-exist topic should return ErrNotExist", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		err := ms.unregisterTopic(types.TopicID(1))
		So(status.Code(err), ShouldEqual, codes.NotFound)
	})

	Convey("unregister exist topic", t, func(ctx C) {
		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		err = ms.unregisterTopic(types.TopicID(1))
		So(err, ShouldBeNil)

		topic := ms.lookupTopic(types.TopicID(1))
		So(topic, ShouldBeNil)
	})
}

func TestStorage_MaxTopicsCount(t *testing.T) {
	tcs := []struct {
		name           string
		maxTopicsCount int32
		testf          func(t *testing.T, ms *MetadataStorage)
	}{
		{
			name:           "LimitOne",
			maxTopicsCount: 1,
			testf: func(t *testing.T, ms *MetadataStorage) {
				err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: 1})
				require.NoError(t, err)

				err = ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: 2})
				require.Error(t, err)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))

				err = ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: 1})
				require.NoError(t, err)

				err = ms.unregisterTopic(1)
				require.NoError(t, err)

				err = ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: 2})
				require.NoError(t, err)
			},
		},
		{
			name:           "LimitZero",
			maxTopicsCount: 0,
			testf: func(t *testing.T, ms *MetadataStorage) {
				err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: 1})
				require.Error(t, err)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ms := NewMetadataStorage(nil, DefaultSnapshotCount, zaptest.NewLogger(t))
			ms.limits.maxTopicsCount = tc.maxTopicsCount
			tc.testf(t, ms)
		})
	}
}

func TestStorage_MaxLogStreamsCountPerTopic(t *testing.T) {
	const snid = types.StorageNodeID(1)
	const tpid = types.TopicID(1)

	tcs := []struct {
		name                       string
		maxLogStreamsCountPerTopic int32
		testf                      func(t *testing.T, ms *MetadataStorage)
	}{
		{
			name:                       "LimitZero",
			maxLogStreamsCountPerTopic: 0,
			testf: func(t *testing.T, ms *MetadataStorage) {
				err := ms.registerLogStream(makeLogStream(tpid, types.LogStreamID(1), []types.StorageNodeID{snid}))
				require.Error(t, err)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			},
		},
		{
			name:                       "LimitOne",
			maxLogStreamsCountPerTopic: 1,
			testf: func(t *testing.T, ms *MetadataStorage) {
				err := ms.registerLogStream(makeLogStream(tpid, types.LogStreamID(1), []types.StorageNodeID{snid}))
				require.NoError(t, err)

				err = ms.registerLogStream(makeLogStream(tpid, types.LogStreamID(2), []types.StorageNodeID{snid}))
				require.Error(t, err)
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ms := NewMetadataStorage(nil, DefaultSnapshotCount, zaptest.NewLogger(t))
			ms.limits.maxLogStreamsCountPerTopic = tc.maxLogStreamsCountPerTopic

			err := ms.registerStorageNode(&varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snid,
				},
			})
			require.NoError(t, err)

			err = ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: tpid})
			require.NoError(t, err)

			tc.testf(t, ms)
		})
	}
}

func TestStorageSortedTopicLogStreamIDs(t *testing.T) {
	Convey("UncommitReport should be committed", t, func(ctx C) {
		/*
			Topic-1 : LS-1, LS-3
			Topic-2 : LS-2, LS-4
		*/

		nrLS := 4
		nrTopic := 2

		ms := NewMetadataStorage(nil, DefaultSnapshotCount, zap.NewNop())

		snID := types.StorageNodeID(0)
		snIDs := make([]types.StorageNodeID, 1)
		snIDs = append(snIDs, snID)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := ms.registerStorageNode(sn)
		So(err, ShouldBeNil)

		for i := 0; i < nrTopic; i++ {
			err := ms.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(i + 1)})
			So(err, ShouldBeNil)
		}

		for i := 0; i < nrLS; i++ {
			lsID := types.LogStreamID(i + 1)
			ls := makeLogStream(types.TopicID(i%2+1), lsID, snIDs)
			err = ms.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		ids := ms.GetSortedTopicLogStreamIDs()
		So(sort.SliceIsSorted(ids, func(i, j int) bool {
			if ids[i].TopicID == ids[j].TopicID {
				return ids[i].LogStreamID < ids[j].LogStreamID
			}

			return ids[i].TopicID < ids[j].TopicID
		}), ShouldBeTrue)
	})
}
