package storagenode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/fputil"
)

// /<volume>/cid_<cluster_id>/snid_<storage_node_id>/lsid_<log_stream_id>
const (
	clusterDirPrefix   = "cid"
	storageDirPrefix   = "snid"
	logStreamDirPrefix = "lsid"

	touchFileName = ".touch"

	VolumeFileMode = os.FileMode(0700)
)

// Volume is an absolute directory to store varlog data.
type Volume string

// NewVolume returns volume that should already exists. If the given volume does not exist, it
// returns os.ErrNotExist.
func NewVolume(volume string) (Volume, error) {
	volume, err := filepath.Abs(volume)
	if err != nil {
		return "", err
	}
	if err := ValidDir(volume); err != nil {
		return "", err
	}
	return Volume(volume), nil
}

func (vol Volume) Valid() error {
	return ValidDir(string(vol))
}

// ValidDir check if the volume (or path) is valid. Valid volume (or path) should be:
// - absolute path
// - existing directory
// - writable directory
func ValidDir(dir string) error {
	if !filepath.IsAbs(dir) {
		return errors.New("not absolute path")
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return errors.New("not directory")
	}
	return fputil.IsWritableDir(dir)
}

// CreateStorageNodePath creates a new directory to store various data related to the storage node.
// If creating the new directory fails, it returns an error.
func (vol Volume) CreateStorageNodePath(clusterID types.ClusterID, storageNodeID types.StorageNodeID) (string, error) {
	clusterDir := fmt.Sprintf("%s_%v", clusterDirPrefix, clusterID)
	storageNodeDir := fmt.Sprintf("%s_%v", storageDirPrefix, storageNodeID)
	snPath := filepath.Join(string(vol), clusterDir, storageNodeDir)
	snPath, err := filepath.Abs(snPath)
	if err != nil {
		return "", err
	}
	snPath, err = createPath(snPath)
	return snPath, err
}

// type StorageNodePath string

// CreateLogStreamPath creates a new directory to store various data related to the log stream
// replica. If creating the new directory fails, it returns an error.
func /*(snp StorageNodePath)*/ CreateLogStreamPath(storageNodePath string, logStreamID types.LogStreamID) (string, error) {
	logStreamDir := fmt.Sprintf("%s_%v", logStreamDirPrefix, logStreamID)
	lsPath := filepath.Join(storageNodePath, logStreamDir)
	lsPath, err := filepath.Abs(lsPath)
	if err != nil {
		return "", err
	}
	return createPath(lsPath)
}

func createPath(dir string) (string, error) {
	if err := os.MkdirAll(dir, VolumeFileMode); err != nil {
		return "", err
	}
	if err := ValidDir(dir); err != nil {
		return "", err
	}
	return dir, nil
}
