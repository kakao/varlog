package storagenode

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/pkg/types"
)

func TestStorageNodeDirName(t *testing.T) {
	tcs := []struct {
		cid      types.ClusterID
		snid     types.StorageNodeID
		expected string
	}{
		{cid: 1, snid: 1, expected: "cid_1_snid_1"},
		{cid: 0, snid: 1, expected: "cid_0_snid_1"},
	}
	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, storageNodeDirName(tc.cid, tc.snid))
		})
	}
}

func TestLogStreamDirName(t *testing.T) {
	tcs := []struct {
		tpid     types.TopicID
		lsid     types.LogStreamID
		expected string
	}{
		{tpid: 1, lsid: 1, expected: "tpid_1_lsid_1"},
		{tpid: 0, lsid: 1, expected: "tpid_0_lsid_1"},
	}
	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, logStreamDirName(tc.tpid, tc.lsid))
		})
	}
}

func TestCreateStorageNodePaths(t *testing.T) {
	volumes := []string{
		t.TempDir(),
		t.TempDir(),
	}
	expectedPaths := []string{
		filepath.Join(volumes[0], "cid_1_snid_2"),
		filepath.Join(volumes[1], "cid_1_snid_2"),
	}
	snPaths, err := createStorageNodePaths(volumes, 1, 2)
	assert.NoError(t, err)
	for _, expectedPath := range expectedPaths {
		assert.Contains(t, snPaths, expectedPath)
	}
}

func TestCreateStorageNodePathsWithProhibitedVolume(t *testing.T) {
	volume := t.TempDir()
	assert.NoError(t, os.Chmod(volume, 0400))
	defer func() {
		assert.NoError(t, os.Chmod(volume, 0700))
	}()

	_, err := createStorageNodePaths([]string{volume}, 1, 2)
	assert.Error(t, err)
}

func TestReadLogStreamPaths(t *testing.T) {
	unreadableVolume := t.TempDir()
	unreadableSNPaths, err := createStorageNodePaths([]string{unreadableVolume}, 1, 2)
	assert.NoError(t, err)
	unreadableSNPath := unreadableSNPaths[0]
	assert.NoError(t, os.MkdirAll(filepath.Join(unreadableSNPath, "tpid_1_lsid_1"), 0700))
	assert.NoError(t, os.Chmod(unreadableSNPath, 0200))
	defer func() {
		assert.NoError(t, os.Chmod(unreadableSNPath, 0700))
	}()
	lsPaths := readLogStreamPaths(unreadableSNPaths)
	assert.Empty(t, lsPaths)

	volume := t.TempDir()
	snPaths, err := createStorageNodePaths([]string{volume}, 1, 2)
	assert.NoError(t, err)

	snPath := snPaths[0]
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_1_lsid_1"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_1"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "lsid_1"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_lsid_1"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_1_lsid"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_a_lsid_b"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_a_lsid"), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_1_lsid_a"), 0700))
	assert.NoError(t, ioutil.WriteFile(filepath.Join(snPath, "tpid_1_lsid_2"), []byte(""), 0700))
	assert.NoError(t, os.MkdirAll(filepath.Join(snPath, "tpid_1_lsid_3"), 0400))

	lsPaths = readLogStreamPaths(snPaths)
	assert.Len(t, lsPaths, 2)
	assert.Contains(t, lsPaths, filepath.Join(snPath, "tpid_1_lsid_1"))
	assert.Contains(t, lsPaths, filepath.Join(snPath, "tpid_1_lsid_3"))
}

func TestParseLogStreamPath(t *testing.T) {
	tcs := []struct {
		input string
		cid   types.ClusterID
		snid  types.StorageNodeID
		tpid  types.TopicID
		lsid  types.LogStreamID
		isErr bool
	}{
		{
			input: "/data/volume/cid_1_snid_2/tpid_3_lsid_4",
			cid:   1,
			snid:  2,
			tpid:  3,
			lsid:  4,
		},
		{
			input: "/volume/cid_1_snid_2/tpid_3_lsid_4",
			cid:   1,
			snid:  2,
			tpid:  3,
			lsid:  4,
		},
		{
			input: "/cid_1_snid_2/tpid_3_lsid_4",
			cid:   1,
			snid:  2,
			tpid:  3,
			lsid:  4,
		},
		{
			input: "/volume/cid_1_snid_2",
			isErr: true,
		},
		{
			input: "/volume/tpid_3_lsid_4",
			isErr: true,
		},
		{
			input: "./cid_1_snid_2/tpid_3_lsid_4",
			isErr: true,
		},
		{
			input: "/cid_a_snid_2/tpid_3_lsid_4",
			isErr: true,
		},
		{
			input: "/cid_1_snid_a/tpid_3_lsid_4",
			isErr: true,
		},
		{
			input: "/cid_1_snid_2/tpid_b_lsid_4",
			isErr: true,
		},
		{
			input: "/cid_1_snid_2/tpid_3_lsid_a",
			isErr: true,
		},
		{
			input: "/lspath",
			isErr: true,
		},
		{
			input: "/",
			isErr: true,
		},
		{
			input: ".",
			isErr: true,
		},
	}
	for i := range tcs {
		tc := tcs[i]
		t.Run(tc.input, func(t *testing.T) {
			cid, snid, tpid, lsid, err := parseLogStreamPath(tc.input)
			if tc.isErr {
				assert.Error(t, err)
				return
			}
			assert.Equal(t, tc.cid, cid)
			assert.Equal(t, tc.snid, snid)
			assert.Equal(t, tc.tpid, tpid)
			assert.Equal(t, tc.lsid, lsid)
		})
	}
}
