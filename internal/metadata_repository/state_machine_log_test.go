package metadata_repository

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestStateMachineLogOpenForWrite(t *testing.T) {
	Convey("Given not exist directory", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"

		Reset(func() {
			os.RemoveAll(dir)
		})

		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		Convey("When state machine log open with write", func(ctx C) {
			err := sml.OpenForWrite(0)
			So(err, ShouldBeNil)
			Convey("Then it should have file", func(ctx C) {
				So(sml.file, ShouldNotBeNil)
				So(existStateMachineLog(dir), ShouldBeTrue)
			})
		})
	})
}

func TestStateMachineLogCreateSegment(t *testing.T) {
	Convey("Given directory", t, func(ctx C) {
		lg := zap.NewNop()
		raftdir := "./raftdata"
		dir := filepath.Join(raftdir, "sml")

		Reset(func() {
			os.RemoveAll(raftdir)
		})

		os.Mkdir(raftdir, 0777)

		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		err := sml.OpenForWrite(0)
		So(err, ShouldBeNil)

		Convey("When state machine log create new segment", func(ctx C) {
			nrSeg := 10
			for i := 1; i < nrSeg; i++ {
				err := sml.createNewSegment(uint64(i), 0)
				So(err, ShouldBeNil)
				So(sml.file, ShouldNotBeNil)
			}

			Convey("Then it should have .sml", func(ctx C) {
				names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
				So(err, ShouldBeNil)
				So(len(names), ShouldEqual, nrSeg)
			})
		})
	})
}

func TestStateMachineLogCleanup(t *testing.T) {
	Convey("Given outdated state machine log", t, func(ctx C) {
		lg := zap.NewNop()
		raftdir := "./raftdata"
		dir := filepath.Join(raftdir, "sml")

		Reset(func() {
			os.RemoveAll(raftdir)
		})

		os.Mkdir(raftdir, 0777)

		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		err := sml.OpenForWrite(0)
		So(err, ShouldBeNil)

		nrSeg := 10
		for i := 1; i < nrSeg; i++ {
			err := sml.createNewSegment(uint64(i), 0)
			So(err, ShouldBeNil)
			So(sml.file, ShouldNotBeNil)
		}

		sml.Close()

		Convey("When open for write", func(ctx C) {
			err := sml.OpenForWrite(0)
			So(err, ShouldBeNil)
			Reset(func() {
				sml.Close()
			})

			Convey("Then it should cleanup outdated sml", func(ctx C) {
				names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
				So(err, ShouldBeNil)
				So(len(names), ShouldEqual, 1)
			})
		})
	})
}

func TestStateMachineLogCut(t *testing.T) {
	Convey("Given state machine log with segmentSizeByte = 1KB", t, func(ctx C) {
		lg := zap.NewNop()
		raftdir := "./raftdata"
		dir := filepath.Join(raftdir, "sml")

		Reset(func() {
			os.RemoveAll(raftdir)
		})

		os.Mkdir(raftdir, 0777)

		segmentSizeBytes = 1024 // 1KB
		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		err := sml.OpenForWrite(0)
		So(err, ShouldBeNil)

		Reset(func() {
			sml.Close()
		})

		Convey("When append more than 1KB", func(ctx C) {
			total := 0
			for i := 0; int64(total) < segmentSizeBytes; i++ {
				l := &mrpb.RegisterStorageNode{
					StorageNode: &varlogpb.StorageNodeDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "127.0.0.1:50000",
						},
					},
				}

				entry := &mrpb.StateMachineLogEntry{
					AppliedIndex: uint64(i),
				}

				entry.Payload.SetValue(l)
				sml.Append(entry)

				total += entry.ProtoSize()
			}

			Convey("Then it should cut", func(ctx C) {
				names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
				So(err, ShouldBeNil)
				So(len(names), ShouldBeGreaterThan, 1)
			})
		})
	})
}

func TestStateMachineLogSelectSegment(t *testing.T) {
	Convey("Given state machine log", t, func(ctx C) {
		dir := "./sml"

		Reset(func() {
			os.RemoveAll(dir)
		})

		os.Mkdir(dir, 0777)

		Convey("When it has multi segments", func(ctx C) {
			var segments []string
			for i := 0; i < 10; i++ {
				segment := fmt.Sprintf("%020d.sml", 100*i)
				path := filepath.Join(dir, segment)
				segments = append(segments, segment)

				f, err := os.Create(path)
				So(err, ShouldBeNil)
				Reset(func() {
					f.Close()
				})
			}

			Convey("Then it should select profer file", func(ctx C) {
				for i := 0; i < 10; i++ {
					idx, err := selectStateMachineLogSegment(segments, uint64(100*i+1))
					So(err, ShouldBeNil)
					So(idx, ShouldEqual, i)
				}
			})
		})
	})
}

func TestStateMachineLogReadFrom(t *testing.T) {
	Convey("Given state machine log", t, func(ctx C) {
		lg := zap.NewNop()
		raftdir := "./raftdata"
		dir := filepath.Join(raftdir, "sml")

		Reset(func() {
			os.RemoveAll(raftdir)
		})

		os.Mkdir(raftdir, 0777)

		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		Convey("When dir is empty", func(ctx C) {
			Convey("Then it should return ErrNotExist", func(ctx C) {
				_, err := sml.ReadFrom(0)
				So(err, ShouldResemble, verrors.ErrNotExist)
			})
		})

		Convey("When append entries", func(ctx C) {
			segmentSizeBytes = 1024 // 1KB
			err := sml.OpenForWrite(0)
			So(err, ShouldBeNil)

			appliedIndex := uint64(0)
			for {
				l := &mrpb.RegisterStorageNode{
					StorageNode: &varlogpb.StorageNodeDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: types.StorageNodeID(1),
							Address:       "127.0.0.1:50000",
						},
					},
				}

				entry := &mrpb.StateMachineLogEntry{
					AppliedIndex: appliedIndex,
				}

				entry.Payload.SetValue(l)
				sml.Append(entry)

				segments, _ := getStateMachineLogSegments(dir)
				if len(segments) == 3 {
					break
				}

				appliedIndex++
			}

			sml.Close()

			Convey("Then ReadFrom should return profer entries", func(ctx C) {
				for i := uint64(0); i <= appliedIndex; i++ {
					entries, err := sml.ReadFrom(i)
					So(err, ShouldBeNil)
					So(len(entries), ShouldBeGreaterThan, 0)
					So(entries[0].GetAppliedIndex(), ShouldEqual, i)
				}
			})
		})
	})
}

func TestStateMachineLogReadFromHole(t *testing.T) {
	Convey("Given state machine log with 3 segments", t, func(ctx C) {
		lg := zap.NewNop()
		raftdir := "./raftdata"
		dir := filepath.Join(raftdir, "sml")

		Reset(func() {
			os.RemoveAll(raftdir)
		})

		os.Mkdir(raftdir, 0777)

		segmentSizeBytes = 1024 // 1KB

		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		err := sml.OpenForWrite(0)
		So(err, ShouldBeNil)

		appliedIndex := uint64(0)
		for {
			l := &mrpb.RegisterStorageNode{
				StorageNode: &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: types.StorageNodeID(1),
						Address:       "127.0.0.1:50000",
					},
				},
			}

			entry := &mrpb.StateMachineLogEntry{
				AppliedIndex: appliedIndex,
			}

			entry.Payload.SetValue(l)
			sml.Append(entry)

			segments, _ := getStateMachineLogSegments(dir)
			if len(segments) == 3 {
				break
			}

			appliedIndex++
		}

		sml.Close()

		Convey("When Second segment is deleted", func(ctx C) {
			segments, _ := getStateMachineLogSegments(dir)
			So(len(segments), ShouldEqual, 3)
			os.Remove(filepath.Join(dir, segments[1]))

			Convey("Then ReadFrom returns crc mismatch", func(ctx C) {
				entries, err := sml.ReadFrom(0)
				So(err, ShouldNotBeNil)
				So(len(entries), ShouldBeGreaterThan, 0)
			})
		})
	})
}

func TestStateMachineLogReadFromWithDirty(t *testing.T) {
	Convey("Given state machine log", t, func(ctx C) {
		lg := zap.NewNop()
		raftdir := "./raftdata"
		dir := filepath.Join(raftdir, "sml")

		Reset(func() {
			os.RemoveAll(raftdir)
		})

		os.Mkdir(raftdir, 0777)

		sml := newStateMachineLog(lg, dir)
		So(sml, ShouldNotBeNil)

		segmentSizeBytes = 1024 // 1KB
		err := sml.OpenForWrite(0)
		So(err, ShouldBeNil)

		appliedIndex := uint64(0)
		for ; appliedIndex < uint64(5); appliedIndex++ {
			l := &mrpb.RegisterStorageNode{
				StorageNode: &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: types.StorageNodeID(1),
						Address:       "127.0.0.1:50000",
					},
				},
			}

			entry := &mrpb.StateMachineLogEntry{
				AppliedIndex: appliedIndex,
			}

			entry.Payload.SetValue(l)
			sml.Append(entry)
		}

		Convey("When append dirty entry", func(ctx C) {
			sml.file.Write([]byte("foo"))
			sml.Close()

			Convey("Then ReadFrom should return all valid entries", func(ctx C) {
				entries, err := sml.ReadFrom(0)
				So(err, ShouldNotBeNil)
				So(len(entries), ShouldEqual, appliedIndex)
				So(entries[0].GetAppliedIndex(), ShouldEqual, 0)
			})
		})
	})
}
