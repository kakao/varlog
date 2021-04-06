package storagenode

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/fputil"
)

// /<volume>/cid_<cluster_id>/snid_<storage_node_id>/lsid_<log_stream_id>
const (
	clusterDirPrefix   = "cid"
	storageDirPrefix   = "snid"
	logStreamDirPrefix = "lsid"

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
		return errors.New("storagenode: not absolute path")
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	if !fi.IsDir() {
		return errors.New("storagenode: not directory")
	}
	err = fputil.IsWritableDir(dir)
	return errors.WithMessage(err, "storagenode")
}

// CreateStorageNodePath creates a new directory to store various data related to the storage node.
// If creating the new directory fails, it returns an error.
func (vol Volume) CreateStorageNodePath(clusterID types.ClusterID, storageNodeID types.StorageNodeID) (string, error) {
	clusterDir := fmt.Sprintf("%s_%v", clusterDirPrefix, clusterID)
	storageNodeDir := fmt.Sprintf("%s_%v", storageDirPrefix, storageNodeID)
	snPath := filepath.Join(string(vol), clusterDir, storageNodeDir)
	snPath, err := filepath.Abs(snPath)
	if err != nil {
		return "", errors.Wrapf(err, "storagenode")
	}
	snPath, err = createPath(snPath)
	return snPath, err
}

func (vol Volume) ReadLogStreamPaths(clusterID types.ClusterID, storageNodeID types.StorageNodeID) []string {
	clusterDir := fmt.Sprintf("%s_%v", clusterDirPrefix, clusterID)
	storageNodeDir := fmt.Sprintf("%s_%v", storageDirPrefix, storageNodeID)
	storageNodePath := filepath.Join(string(vol), clusterDir, storageNodeDir)

	var logStreamPaths []string
	fis, err := ioutil.ReadDir(storageNodePath)
	if err != nil {
		return nil
	}
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		toks := strings.SplitN(fi.Name(), "_", 2)
		if toks[0] != logStreamDirPrefix {
			continue
		}
		if _, err := types.ParseLogStreamID(toks[1]); err != nil {
			continue
		}
		path := filepath.Join(storageNodePath, fi.Name())
		logStreamPaths = append(logStreamPaths, path)
	}
	return logStreamPaths
}

// CreateLogStreamPath creates a new directory to store various data related to the log stream
// replica. If creating the new directory fails, it returns an error.
func CreateLogStreamPath(storageNodePath string, logStreamID types.LogStreamID) (string, error) {
	logStreamDir := fmt.Sprintf("%s_%v", logStreamDirPrefix, logStreamID)
	lsPath := filepath.Join(storageNodePath, logStreamDir)
	lsPath, err := filepath.Abs(lsPath)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return createPath(lsPath)
}

func createPath(dir string) (string, error) {
	if err := os.MkdirAll(dir, VolumeFileMode); err != nil {
		return "", errors.WithStack(err)
	}
	if err := ValidDir(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func ParseLogStreamPath(path string) (vol Volume, cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID, err error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return "", 0, 0, 0, errors.New("not absolute path")
	}

	toks := strings.Split(path, string(filepath.Separator))
	if len(toks) < 4 {
		return "", 0, 0, 0, errors.New("invalid path")
	}

	lsidPath := toks[len(toks)-1]
	snidPath := toks[len(toks)-2]
	cidPath := toks[len(toks)-3]
	volPath := filepath.Join("/", filepath.Join(toks[0:len(toks)-3]...))

	vol = Volume(volPath)
	if lsid, err = types.ParseLogStreamID(strings.TrimPrefix(lsidPath, logStreamDirPrefix+"_")); err != nil {
		goto errOut
	}
	if snid, err = types.ParseStorageNodeID(strings.TrimPrefix(snidPath, storageDirPrefix+"_")); err != nil {
		goto errOut
	}
	if cid, err = types.ParseClusterID(strings.TrimPrefix(cidPath, clusterDirPrefix+"_")); err != nil {
		goto errOut
	}
	return vol, cid, snid, lsid, nil

errOut:
	return "", 0, 0, 0, errors.New("invalid path")
}
