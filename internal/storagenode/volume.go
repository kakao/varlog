package storagenode

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kakao/varlog/pkg/types"
)

const (
	clusterDirPrefix   = "cid"
	storageDirPrefix   = "snid"
	topicDirPrefix     = "tpid"
	logStreamDirPrefix = "lsid"
	dirNameSeparator   = "_"
	volumeFileMode     = os.FileMode(0700)
)

// storageNodeDirName returns the directory name of the storage node data.
// The directory name is composed of the cluster ID and storage node ID, for example, "cid_1_snid_1".
func storageNodeDirName(cid types.ClusterID, snid types.StorageNodeID) string {
	return strings.Join([]string{
		clusterDirPrefix, cid.String(),
		storageDirPrefix, snid.String(),
	}, dirNameSeparator)
}

// logStreamDirName returns the directory name of the log stream data.
// The directory name is composed of the topic ID and log stream ID, for example, "tpid_1_lsid_1".
func logStreamDirName(tpid types.TopicID, lsid types.LogStreamID) string {
	return strings.Join([]string{
		topicDirPrefix, tpid.String(),
		logStreamDirPrefix, lsid.String(),
	}, dirNameSeparator)
}

// createStorageNodePaths creates the directory structure for the storage node data.
// The directory structure is composed of the volume, cluster ID and storage node ID, for example, "/volume/cid_1_snid_1/".
func createStorageNodePaths(volumes []string, cid types.ClusterID, snid types.StorageNodeID) (snPaths []string, err error) {
	for _, volume := range volumes {
		snPath := filepath.Join(volume, storageNodeDirName(cid, snid))
		if err := os.MkdirAll(snPath, volumeFileMode); err != nil {
			return nil, err
		}
		snPaths = append(snPaths, snPath)
	}
	return snPaths, nil
}

// readLogStreamPaths reads the directory structure for the log stream data in the list of storage node data paths.
// Returned paths are composed of the volume, cluster ID, storage node ID, topic ID and log stream ID, for example, "/volume/cid_1_snid_1/tpid_1_lsid_1/".
// If the log stream data directories are not found, empty list is returned.
func readLogStreamPaths(snPaths []string) (lsPaths []string) {
	for _, snPath := range snPaths {
		des, err := os.ReadDir(snPath)
		if err != nil {
			continue
		}
		for _, de := range des {
			if !de.IsDir() {
				continue
			}
			if _, _, err := parseLogStreamDirName(de.Name()); err != nil {
				continue
			}
			lsPath := filepath.Join(snPath, de.Name())
			lsPaths = append(lsPaths, lsPath)
		}
	}
	return lsPaths
}

// parseLogStreamPath returns the cluster ID, storage node ID, topic ID and log stream ID from the path of the log stream data.
// The argument lsPath should be absolute path.
func parseLogStreamPath(lsPath string) (cid types.ClusterID, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, err error) {
	const minParts = 3 // "", "cid_1_snid_2", "tpid_3_lsid_4"

	lsPath = filepath.Clean(lsPath)
	if !filepath.IsAbs(lsPath) {
		err = fmt.Errorf("storage node: log stream path: not absolute %s", lsPath)
		return
	}

	toks := strings.Split(lsPath, string(filepath.Separator))
	if len(toks) < minParts {
		err = fmt.Errorf("storage node: log stream path: invalid format %s", lsPath)
		return
	}

	toksLen := len(toks)
	snDirName := toks[toksLen-2]
	cid, snid, err = parseStorageNodeDirName(snDirName)
	if err != nil {
		return
	}

	lsDirName := toks[toksLen-1]
	tpid, lsid, err = parseLogStreamDirName(lsDirName)
	return cid, snid, tpid, lsid, err
}

func parseStorageNodeDirName(snDirName string) (cid types.ClusterID, snid types.StorageNodeID, err error) {
	toks := strings.Split(snDirName, dirNameSeparator)
	if len(toks) != 4 || toks[0] != clusterDirPrefix || toks[2] != storageDirPrefix {
		err = fmt.Errorf("storage node: storage node directory: invalid name: %s", snDirName)
		return
	}
	cid, err = types.ParseClusterID(toks[1])
	if err != nil {
		err = fmt.Errorf("storage node: storage node directory: invalid cluster id: %w", err)
		return
	}
	snid, err = types.ParseStorageNodeID(toks[3])
	if err != nil {
		err = fmt.Errorf("storage node: storage node directory: invalid storage node id: %w", err)
		return
	}
	return cid, snid, nil
}

func parseLogStreamDirName(lsDirName string) (tpid types.TopicID, lsid types.LogStreamID, err error) {
	toks := strings.Split(lsDirName, dirNameSeparator)
	if len(toks) != 4 || toks[0] != topicDirPrefix || toks[2] != logStreamDirPrefix {
		err = fmt.Errorf("storage node: log stream directory: invalid name: %s", lsDirName)
		return
	}
	if tpid, err = types.ParseTopicID(toks[1]); err != nil {
		err = fmt.Errorf("storage node: log stream directory: invalid topic id: %w", err)
		return
	}
	if lsid, err = types.ParseLogStreamID(toks[3]); err != nil {
		err = fmt.Errorf("storage node: log stream directory: invalid log stream id: %w", err)
		return
	}
	return tpid, lsid, nil
}
