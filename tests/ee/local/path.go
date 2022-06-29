package local

import "path/filepath"

func findPath(rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}

	return filepath.Join(basepath, rel)
}

func binDir() string {
	return findPath("../../bin")
}
