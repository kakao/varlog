package volume

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/fputil"
)

// /<volume>/cid_<cluster_id>/snid_<storage_node_id>/tpid_<topic_id>_lsid_<log_stream_id>
const (
	clusterDirPrefix   = "cid"
	storageDirPrefix   = "snid"
	topicDirPrefix     = "tpid"
	logStreamDirPrefix = "lsid"

	VolumeFileMode = os.FileMode(0700)
)

// Volume is an absolute directory to store varlog data.
type Volume string

// New returns volume that should already exist and be a writable directory.
// The result will be converted to absolute if the given volume is relative.
// If the given volume does not exist, it returns os.ErrNotExist.
func New(volume string) (Volume, error) {
	volume, err := filepath.Abs(volume)
	if err != nil {
		return "", err
	}
	if err := validDirPath(volume); err != nil {
		return "", err
	}
	return Volume(volume), nil
}

// ReadLogStreamPaths returns all of log stream paths under the given clusterID and storageNodeID.
func (vol Volume) ReadLogStreamPaths(clusterID types.ClusterID, storageNodeID types.StorageNodeID) []string {
	clusterDir := fmt.Sprintf("%s_%d", clusterDirPrefix, clusterID)
	storageNodeDir := fmt.Sprintf("%s_%d", storageDirPrefix, storageNodeID)
	storageNodePath := filepath.Join(string(vol), clusterDir, storageNodeDir)

	fis, err := ioutil.ReadDir(storageNodePath)
	if err != nil {
		return nil
	}
	logStreamPaths := make([]string, 0, len(fis))
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		if _, _, err := parseLogStreamDirName(fi.Name()); err != nil {
			continue
		}
		path := filepath.Join(storageNodePath, fi.Name())
		logStreamPaths = append(logStreamPaths, path)
	}
	return logStreamPaths
}

// ParseLogStreamPath parses the given path into volume, ClusterID, StorageNodeID, TopicID and LogStreamID.
func ParseLogStreamPath(path string) (vol Volume, cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, err error) {
	const minParts = 4
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return "", 0, 0, 0, 0, errors.Errorf("not absolute path: %s", path)
	}

	toks := strings.Split(path, string(filepath.Separator))
	if len(toks) < minParts {
		return "", 0, 0, 0, 0, errors.Errorf("invalid path: %s", path)
	}

	lsidPath := toks[len(toks)-1]
	snidPath := toks[len(toks)-2]
	cidPath := toks[len(toks)-3]
	volPath := filepath.Join("/", filepath.Join(toks[0:len(toks)-3]...))

	vol = Volume(volPath)

	if tpid, lsid, err = parseLogStreamDirName(lsidPath); err != nil {
		goto errOut
	}
	if snid, err = types.ParseStorageNodeID(strings.TrimPrefix(snidPath, storageDirPrefix+"_")); err != nil {
		goto errOut
	}
	if cid, err = types.ParseClusterID(strings.TrimPrefix(cidPath, clusterDirPrefix+"_")); err != nil {
		goto errOut
	}
	return vol, cid, snid, tpid, lsid, nil

errOut:
	return "", 0, 0, 0, 0, err
}

// CreateStorageNodePath creates a new directory to store various data related to the storage node.
// If creating the new directory fails, it returns an error.
// StorageNodePath = /<volume>/cid_<cluster_id>/snid_<storage_node_id>
func CreateStorageNodePath(vol Volume, clusterID types.ClusterID, storageNodeID types.StorageNodeID) (string, error) {
	clusterDir := fmt.Sprintf("%s_%d", clusterDirPrefix, clusterID)
	storageNodeDir := fmt.Sprintf("%s_%d", storageDirPrefix, storageNodeID)
	snPath := filepath.Join(string(vol), clusterDir, storageNodeDir)
	snPath, err := filepath.Abs(snPath)
	if err != nil {
		return "", errors.Wrapf(err, "storagenode")
	}
	return createPath(snPath)
}

// CreateLogStreamPath creates a new directory to store various data related to the log stream
// replica. If creating the new directory fails, it returns an error.
// LogStreamPath = /<volume>/cid_<cluster_id>/snid_<storage_node_id>/tpid_<topic_id>_lsid_<log_stream_id>
func CreateLogStreamPath(storageNodePath string, topicID types.TopicID, logStreamID types.LogStreamID) (string, error) {
	logStreamDir := fmt.Sprintf("%s_%d_%s_%d", topicDirPrefix, topicID, logStreamDirPrefix, logStreamID)
	lsPath := filepath.Join(storageNodePath, logStreamDir)
	lsPath, err := filepath.Abs(lsPath)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return createPath(lsPath)
}

// validDirPath checks if the parameter dir is an absolute path and existed writable directory.
func validDirPath(dir string) error {
	if !filepath.IsAbs(dir) {
		return errors.Errorf("not absolute path: %s", dir)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	if !fi.IsDir() {
		return errors.Errorf("not directory: %s", dir)
	}
	return fputil.IsWritableDir(dir)
}

func createPath(dir string) (string, error) {
	if err := os.MkdirAll(dir, VolumeFileMode); err != nil {
		return "", errors.WithStack(err)
	}
	if err := validDirPath(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func parseLogStreamDirName(dirName string) (tpid types.TopicID, lsid types.LogStreamID, err error) {
	toks := strings.Split(dirName, "_")
	if len(toks) != 4 || toks[0] != topicDirPrefix || toks[2] != logStreamDirPrefix {
		goto Out
	}
	if tpid, err = types.ParseTopicID(toks[1]); err != nil {
		goto Out
	}
	if lsid, err = types.ParseLogStreamID(toks[3]); err != nil {
		goto Out
	}
	return tpid, lsid, nil
Out:
	return tpid, lsid, errors.Errorf("invalid log stream directory name: %s", dirName)
}
