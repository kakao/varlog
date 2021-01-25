package fputil

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	touchFileName = ".touch"
	touchFileMode = os.FileMode(0600)
)

func IsWritableDir(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	filename := filepath.Join(dir, touchFileName)
	if err := ioutil.WriteFile(filename, []byte(""), touchFileMode); err != nil {
		return errors.WithStack(err)
	}
	err = os.Remove(filename)
	return errors.WithStack(err)
}
