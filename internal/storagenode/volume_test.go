package storagenode

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func newTempVolume(t *testing.T) Volume {
	t.Helper()
	volume, err := NewVolume(t.TempDir())
	if err != nil {
		t.Error(err)
	}
	return volume
}

func TestVolume(t *testing.T) {
	Convey("Volume", t, func() {
		Convey("TempVolume", func() {
			volume := newTempVolume(t)
			So(len(volume), ShouldBeGreaterThan, 0)
			So(os.Remove(string(volume)), ShouldBeNil)
		})
	})
}

func TestValidDir(t *testing.T) {
	writableDir := newTempVolume(t)
	fp, err := os.Create(filepath.Join(string(writableDir), "_file"))
	if err != nil {
		t.Fatal(err)
	}
	tmpfile := fp.Name()
	if err := fp.Close(); err != nil {
		t.Fatal(err)
	}

	notWritableDir := newTempVolume(t)
	if err := os.Chmod(string(notWritableDir), 0400); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := os.RemoveAll(string(writableDir)); err != nil {
			t.Error(err)
		}

		if err := os.Chmod(string(notWritableDir), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(string(notWritableDir)); err != nil {
			t.Error(err)
		}

	}()

	var tests = []struct {
		in string
		ok bool
	}{
		{"", false},
		{tmpfile, false},
		{string(writableDir), true},
		{string(notWritableDir), false},
	}

	for i := range tests {
		test := tests[i]
		t.Run(test.in, func(t *testing.T) {
			actual := ValidDir(test.in)
			if test.ok != (actual == nil) {
				t.Errorf("input=%v, expected=%v, actual=%v", test.in, test.ok, actual)
			}
		})
	}
}
