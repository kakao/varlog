package volume

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/fputil"
)

const (
	clusterDirPrefix   = "cid"
	storageDirPrefix   = "snid"
	topicDirPrefix     = "tpid"
	logStreamDirPrefix = "lsid"
	dirNameSeparator   = "_"
	volumeFileMode     = os.FileMode(0700)
)

// DataDir represents data directory by using various identifiers.
type DataDir struct {
	// Volume should be an absolute directory.
	Volume        string
	ClusterID     types.ClusterID
	StorageNodeID types.StorageNodeID
	TopicID       types.TopicID
	LogStreamID   types.LogStreamID
}

// String returns a full path of data directory. This method is opposite of
// function ParseDataDir.
func (dd *DataDir) String() string {
	return path.Join(
		dd.Volume,
		StorageNodeDirName(dd.ClusterID, dd.StorageNodeID),
		LogStreamDirName(dd.TopicID, dd.LogStreamID),
	)
}

// Valid if the cluster ID and storage node ID are correct and the data directory is writable.
func (dd *DataDir) Valid(cid types.ClusterID, snid types.StorageNodeID) error {
	if dd.ClusterID != cid {
		return fmt.Errorf("unexpected cluster id %d", uint32(dd.ClusterID))
	}
	if dd.StorageNodeID != snid {
		return fmt.Errorf("unexpected storage node id %d", int32(dd.StorageNodeID))
	}
	if !filepath.IsAbs(dd.Volume) {
		return fmt.Errorf("volume %s not absolute", dd.Volume)
	}
	return WritableDirectory(dd.String())
}

// ParseDataDir parses data directory into DataDir struct only by lexical
// processing. The argument dataDir should be absolute path.
func ParseDataDir(dataDir string) (dd DataDir, err error) {
	dataDir = filepath.Clean(dataDir)
	if !filepath.IsAbs(dataDir) {
		return DataDir{}, fmt.Errorf("not absolute directory %s", dataDir)
	}

	snPath, lsDirName := filepath.Split(dataDir)
	dd.TopicID, dd.LogStreamID, err = parseLogStreamDirName(lsDirName)
	if err != nil {
		return DataDir{}, err
	}

	snPath = filepath.Clean(snPath)
	volume, snDirName := filepath.Split(snPath)
	dd.ClusterID, dd.StorageNodeID, err = parseStorageNodeDirName(snDirName)
	if err != nil {
		return DataDir{}, err
	}

	dd.Volume = filepath.Clean(volume)
	return dd, nil
}

// GetValidDataDirectories checks data directories read from volumes whether
// they are valid to load.
// The argument given, specified by the flag `--data-dirs`, represents required
// directories that should exist in the directories read from the volumes.
// If the argument strict is true, which is set by the flag
// `--volume-strict-check`, no other directories must not exist in the volumes
// except the directories specified by the argument given.
// In non-strict mode, on the other hand, those other directories are ignored.
func GetValidDataDirectories(read []DataDir, given []string, strict bool, cid types.ClusterID, snid types.StorageNodeID) ([]DataDir, error) {
	readmap := make(map[string]DataDir, len(read))
	for _, dd := range read {
		readmap[dd.String()] = dd
	}
	givenmap := make(map[string]bool, len(given))
	for _, dir := range given {
		givenmap[dir] = true
	}

	// Given directories specified by the flag `--data-dirs` must exist in
	// the volumes.
	for _, dir := range given {
		if _, ok := readmap[dir]; !ok {
			return nil, fmt.Errorf("volume: no data directory %s", dir)
		}
	}

	// In non-strict mode, data directories read from the volumes can be
	// loaded except those having the wrong cluster ID or storage node ID.
	if !strict {
		dataDirs := make([]DataDir, 0, len(read))
		for _, dd := range read {
			if err := dd.Valid(cid, snid); err != nil {
				// logging
				continue
			}
			dataDirs = append(dataDirs, dd)
		}
		return dataDirs, nil
	}

	// In strict mode, the argument given, which is a set of data
	// directories that are given and the argument read, which is a set of
	// data directories read in volumes should be the same.
	// The facts that the directories read contains all directories
	// specified by the given and the number of both are same imply that
	// they are equal.
	if len(read) != len(given) {
		return nil, errors.New("volume: unmatched data directories")
	}

	// The directories read in volumes must be given by the `--data-dirs`.
	for dir, dd := range readmap {
		if err := dd.Valid(cid, snid); err != nil {
			return nil, fmt.Errorf("volume: invalid data directory %s: %w", dir, err)
		}
	}

	return read, nil
}

// WritableDirectory decides that the argument dir is writable directory.
func WritableDirectory(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("%s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s: not directory", dir)
	}
	if err := fputil.IsWritableDir(dir); err != nil {
		return fmt.Errorf("%s: unwritable: %w", dir, err)
	}
	return nil
}

// ReadVolumes reads all data directories from the volumes regardless their
// validness. It ignores files while walking the volumes.
// If duplicated volumes are given, this method returns an error.
// If there are malformed sub-directories in the volumes, it returns an error.
// To validate each data directories, user should call DataDir.Validate()
// method.
func ReadVolumes(vols []string) ([]DataDir, error) {
	dataDirs := make([]DataDir, 0, len(vols))
	visited := make(map[string]bool, len(vols))
	for _, vol := range vols {
		absvol, err := filepath.Abs(vol)
		if err != nil {
			return nil, err
		}
		if visited[absvol] {
			return nil, fmt.Errorf("duplicated volume %s", vol)
		}
		visited[absvol] = true

		dds, err := readVolume(absvol)
		if err != nil {
			return nil, err
		}
		dataDirs = append(dataDirs, dds...)
	}
	return dataDirs, nil
}

func readVolume(vol string) ([]DataDir, error) {
	des, err := os.ReadDir(vol)
	if err != nil {
		return nil, err
	}

	var dataPaths []DataDir
	for _, de := range des {
		if !de.IsDir() {
			continue
		}
		cid, snid, err := parseStorageNodeDirName(de.Name())
		if err != nil {
			return nil, err
		}

		snPath := path.Join(vol, de.Name())
		des2, err := os.ReadDir(snPath)
		if err != nil {
			return nil, err
		}
		for _, de2 := range des2 {
			if !de2.IsDir() {
				continue
			}
			tpid, lsid, err := parseLogStreamDirName(de2.Name())
			if err != nil {
				return nil, err
			}

			dataPaths = append(dataPaths, DataDir{
				Volume:        vol,
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       tpid,
				LogStreamID:   lsid,
			})
		}
	}
	return dataPaths, nil
}

// CreateStorageNodePaths creates the directory structure for the storage node
// data. The directory structure is composed of the volume, cluster ID and
// storage node ID, for example, "/volume/cid_1_snid_1/".
func CreateStorageNodePaths(volumes []string, cid types.ClusterID, snid types.StorageNodeID) (snPaths []string, err error) {
	for _, volume := range volumes {
		snPath := filepath.Join(volume, StorageNodeDirName(cid, snid))
		if err := os.MkdirAll(snPath, volumeFileMode); err != nil {
			return nil, err
		}
		snPaths = append(snPaths, snPath)
	}
	return snPaths, nil
}

// StorageNodeDirName returns the directory name of the storage node data. The
// directory name is composed of the cluster ID and storage node ID, for
// example, "cid_1_snid_1".
func StorageNodeDirName(cid types.ClusterID, snid types.StorageNodeID) string {
	return strings.Join([]string{
		clusterDirPrefix, cid.String(),
		storageDirPrefix, snid.String(),
	}, dirNameSeparator)
}

// LogStreamDirName returns the directory name of the log stream data. The
// directory name is composed of the topic ID and log stream ID, for example,
// "tpid_1_lsid_1".
func LogStreamDirName(tpid types.TopicID, lsid types.LogStreamID) string {
	return strings.Join([]string{
		topicDirPrefix, tpid.String(),
		logStreamDirPrefix, lsid.String(),
	}, dirNameSeparator)
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
