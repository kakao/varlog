package storagenode

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
)

func TestOptions(t *testing.T) {
	Convey("Options", t, func() {
		var opts Options

		Convey("Zero value", func() {
			So(opts.Valid(), ShouldNotBeNil)
		})

		Convey("Empty volumes", func() {
			opts.Volumes = map[Volume]struct{}{}
			So(opts.Valid(), ShouldNotBeNil)
		})

		Convey("Invalid volumes", func() {
			opts.Volumes = map[Volume]struct{}{"abc": {}}
			So(opts.Valid(), ShouldNotBeNil)
		})

		Convey("Invalid storage", func() {
			tmpdir := t.TempDir()
			opts.Volumes = map[Volume]struct{}{Volume(tmpdir): {}}
			opts.Logger = zap.NewNop()
			So(opts.Valid(), ShouldNotBeNil)
		})

		Convey("Nil logger", func() {
			tmpdir := t.TempDir()
			opts.Volumes = map[Volume]struct{}{Volume(tmpdir): {}}
			opts.StorageName = DefaultStorageName
			So(opts.Valid(), ShouldNotBeNil)
		})

		Convey("Default options", func() {
			So(DefaultOptions.Valid(), ShouldBeNil)
		})
	})
}
