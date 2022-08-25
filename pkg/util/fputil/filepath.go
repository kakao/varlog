package fputil

import (
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
	if err := os.WriteFile(filename, []byte(""), touchFileMode); err != nil {
		return errors.WithStack(err)
	}
	err = os.Remove(filename)
	return errors.WithStack(err)
}
