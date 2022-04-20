package fputil

import (
	"io/fs"
	"path/filepath"
	"syscall"
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

func DiskSize(path string) (all, used uint64, err error) {
	var stat syscall.Statfs_t
	err = syscall.Statfs(path, &stat)
	if err != nil {
		return
	}

	all = stat.Blocks * uint64(stat.Bsize)
	used = (stat.Blocks - stat.Bfree) * uint64(stat.Bsize)
	return all, used, nil
}
