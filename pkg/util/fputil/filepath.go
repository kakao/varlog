package fputil

import (
	"os"
	"path/filepath"
)

const (
	touchFileName = ".touch"
	touchFileMode = os.FileMode(0600)
)

func IsWritableDir(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	filename := filepath.Join(dir, touchFileName)
	if err := os.WriteFile(filename, []byte(""), touchFileMode); err != nil {
		return err
	}
	return os.Remove(filename)
}
