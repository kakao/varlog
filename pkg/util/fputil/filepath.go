package fputil

import (
	"io/ioutil"
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
	if err := ioutil.WriteFile(filename, []byte(""), touchFileMode); err != nil {
		return err
	}
	if err := os.Remove(filename); err != nil {
		return err
	}
	return nil
}
