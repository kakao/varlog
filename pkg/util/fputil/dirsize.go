package fputil

import (
	"io/fs"
	"path/filepath"
)

func DirectorySize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info fs.FileInfo, _ error) error {
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	return size
}
