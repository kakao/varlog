package metadata_repository

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.uber.org/zap"

	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type dummySnapGetter struct {
	snap *mrpb.StateMachineLogSnapshot
}

func (d *dummySnapGetter) GetStateMachineLogSnapshot() *mrpb.StateMachineLogSnapshot {
	return d.snap
}

func newDummySnapGetter() *dummySnapGetter {
	d := &dummySnapGetter{}
	d.snap = &mrpb.StateMachineLogSnapshot{}
	d.snap.Metadata = &varlogpb.MetadataDescriptor{}

	return d
}

func TestStateMachineLogOpenForWrite(t *testing.T) {
	Convey("Given not exist directory", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"
		snapGetter := newDummySnapGetter()

		Reset(func() {
			os.RemoveAll(dir)
		})

		sml := newStateMachineLog(lg, dir, snapGetter)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		Convey("When state machine log open with write", func(ctx C) {
			err := sml.OpenForWrite()
			Convey("Then it should be succeed", func(ctx C) {
				So(err, ShouldBeNil)
				So(sml.file, ShouldNotBeNil)
				So(exist(dir), ShouldBeTrue)
			})
		})
	})
}

func TestStateMachineLogCleanupTemp(t *testing.T) {
	Convey("Given directory with temp file", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"
		snapGetter := newDummySnapGetter()

		Reset(func() {
			os.RemoveAll(dir)
		})

		os.Mkdir(dir, 0777)

		f, err := os.Create(filepath.Join(dir, "0000.tmp"))
		So(err, ShouldBeNil)
		f.Close()

		names, err := fileutil.ReadDir(dir, fileutil.WithExt(".tmp"))
		So(err, ShouldBeNil)
		So(len(names), ShouldEqual, 1)

		Convey("When new state machine log", func(ctx C) {
			sml := newStateMachineLog(lg, dir, snapGetter)
			So(sml, ShouldNotBeNil)

			Convey("Then there are no temp file", func(ctx C) {
				names, err := fileutil.ReadDir(dir, fileutil.WithExt(".tmp"))
				So(err, ShouldBeNil)
				So(len(names), ShouldEqual, 0)
			})
		})
	})
}

func TestStateMachineLogSegment(t *testing.T) {
	Convey("Given directory", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"
		snapGetter := newDummySnapGetter()

		Reset(func() {
			os.RemoveAll(dir)
		})

		os.Mkdir(dir, 0777)

		sml := newStateMachineLog(lg, dir, snapGetter)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		Convey("When state machine log create new segment", func(ctx C) {
			for i := 0; i < 10; i++ {
				err := sml.createNewSegment()
				So(err, ShouldBeNil)
				So(sml.file, ShouldNotBeNil)

				names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
				So(err, ShouldBeNil)
				So(len(names), ShouldEqual, i+1)
			}
		})
	})
}

func TestStateMachineLogContinueTail(t *testing.T) {
	Convey("Given state machine log", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"
		snapGetter := newDummySnapGetter()

		Reset(func() {
			os.RemoveAll(dir)
		})

		os.Mkdir(dir, 0777)

		sml := newStateMachineLog(lg, dir, snapGetter)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		err := sml.OpenForWrite()
		So(err, ShouldBeNil)

		for i := 1; i < 10; i++ {
			err := sml.createNewSegment()
			So(err, ShouldBeNil)
			So(sml.file, ShouldNotBeNil)

			names, err := fileutil.ReadDir(dir, fileutil.WithExt(".sml"))
			So(err, ShouldBeNil)
			So(len(names), ShouldEqual, i+1)
		}

		sml.Close()

		Convey("When re-create state machine log", func(ctx C) {
			sml := newStateMachineLog(lg, dir, snapGetter)
			So(sml, ShouldNotBeNil)

			err := sml.OpenForWrite()
			So(err, ShouldBeNil)

			Convey("Then it should ", func(ctx C) {
				So(tailSeq(dir), ShouldEqual, 10)
				So(sml.seq, ShouldEqual, 11)
			})
		})
	})
}

func TestStateMachineLogOpenForRead(t *testing.T) {
	Convey("Given state machine log", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"
		snapGetter := newDummySnapGetter()

		Reset(func() {
			os.RemoveAll(dir)
		})

		os.Mkdir(dir, 0777)

		sml := newStateMachineLog(lg, dir, snapGetter)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		Convey("When dir is empty", func(ctx C) {
			Convey("Then it should return EOF", func(ctx C) {
				err := sml.OpenForRead()
				So(err, ShouldResemble, io.EOF)
			})
		})

		Convey("When dir have temp file only", func(ctx C) {
			f, err := os.Create(filepath.Join(dir, "0000.tmp"))
			So(err, ShouldBeNil)
			f.Close()

			Convey("Then it should return EOF", func(ctx C) {
				err := sml.OpenForRead()
				So(err, ShouldResemble, io.EOF)
			})
		})

		Convey("When dir have multi sml file", func(ctx C) {
			var i int
			for i = 0; i < 10; i++ {
				f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%020d.sml", i)))
				So(err, ShouldBeNil)
				f.Close()
			}

			Convey("Then it should open tail sml file", func(ctx C) {
				err := sml.OpenForRead()
				So(err, ShouldBeNil)
				So(sml.file.Name(), ShouldEqual, filepath.Join(dir, fmt.Sprintf("%020d.sml", i-1)))
			})
		})

		Convey("When dir have sml file and temp", func(ctx C) {
			var i int
			for i = 0; i < 10; i++ {
				f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%020d.sml", i)))
				So(err, ShouldBeNil)
				f.Close()
			}

			for j := i; j < 20; j++ {
				f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%020d.tmp", j)))
				So(err, ShouldBeNil)
				f.Close()
			}

			Convey("Then it should ignore temp file", func(ctx C) {
				err := sml.OpenForRead()
				So(err, ShouldBeNil)
				So(sml.file.Name(), ShouldEqual, filepath.Join(dir, fmt.Sprintf("%020d.sml", i-1)))
			})
		})
	})
}

func TestStateMachineLogReplay(t *testing.T) {
	Convey("Given state machine log", t, func(ctx C) {
		lg := zap.NewNop()
		dir := "./sml"
		snapGetter := newDummySnapGetter()

		Reset(func() {
			os.RemoveAll(dir)
		})

		os.Mkdir(dir, 0777)

		sml := newStateMachineLog(lg, dir, snapGetter)
		So(sml, ShouldNotBeNil)

		Reset(func() {
			sml.Close()
		})

		Convey("When OpenForWite", func(ctx C) {
			err := sml.OpenForWrite()
			So(err, ShouldBeNil)
			sml.Close()

			Convey("Then it should have snapshot", func(ctx C) {
				err := sml.OpenForRead()
				So(err, ShouldBeNil)

				Reset(func() {
					sml.Close()
				})

				f, err := sml.Read()
				So(err, ShouldBeNil)
				_, ok := f.(*mrpb.StateMachineLogSnapshot)
				So(ok, ShouldBeTrue)
			})
		})

		Convey("When Append 10 entries ", func(ctx C) {
			err := sml.OpenForWrite()
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				entry := &mrpb.StateMachineLogEntry{}
				entry.AppliedIndex = uint64(i)
				entry.Payload.SetValue(&mrpb.RegisterStorageNode{})

				err := sml.Append(entry)
				So(err, ShouldBeNil)
			}

			sml.Close()
			Convey("Then it should have 10 entries ", func(ctx C) {
				err := sml.OpenForRead()
				So(err, ShouldBeNil)

				Reset(func() {
					sml.Close()
				})

				f, err := sml.Read()
				So(err, ShouldBeNil)
				_, ok := f.(*mrpb.StateMachineLogSnapshot)
				So(ok, ShouldBeTrue)

				for i := 0; i < 10; i++ {
					f, err := sml.Read()
					So(err, ShouldBeNil)
					entry, ok := f.(*mrpb.StateMachineLogEntry)
					So(ok, ShouldBeTrue)

					So(entry.AppliedIndex, ShouldEqual, uint64(i))
				}

				_, err = sml.Read()
				So(err, ShouldResemble, io.EOF)
			})
		})
	})
}
