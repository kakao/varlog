package fputil

import (
	"io/fs"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirectorySize(t *testing.T) {
	path := t.TempDir()
	assert.Zero(t, DirectorySize(path))

	assert.NoError(t, ioutil.WriteFile(filepath.Join(path, "foo"), []byte{'a'}, fs.FileMode(777)))
	assert.EqualValues(t, 1, DirectorySize(path))
}
