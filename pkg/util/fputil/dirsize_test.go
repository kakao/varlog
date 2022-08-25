package fputil

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDirectorySize(t *testing.T) {
	path := t.TempDir()
	assert.Zero(t, DirectorySize(path))

	assert.NoError(t, os.WriteFile(filepath.Join(path, "foo"), []byte{'a'}, fs.FileMode(0777)))
	assert.EqualValues(t, 1, DirectorySize(path))
}

func TestDiskSize(t *testing.T) {
	all, used, err := DiskSize("/")
	assert.NoError(t, err)
	t.Logf("all=%d, free=%d, used=%d", all, all-used, used)
}
