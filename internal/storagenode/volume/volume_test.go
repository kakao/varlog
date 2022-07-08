package volume

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/types"
)

func TestVolume_StorageNodeDirName(t *testing.T) {
	tcs := []struct {
		cid      types.ClusterID
		snid     types.StorageNodeID
		expected string
	}{
		{cid: 1, snid: 1, expected: "cid_1_snid_1"},
		{cid: 0, snid: 1, expected: "cid_0_snid_1"},
	}
	for _, tc := range tcs {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, StorageNodeDirName(tc.cid, tc.snid))
		})
	}
}

func TestVolume_LogStreamDirName(t *testing.T) {
	tcs := []struct {
		tpid     types.TopicID
		lsid     types.LogStreamID
		expected string
	}{
		{tpid: 1, lsid: 1, expected: "tpid_1_lsid_1"},
		{tpid: 0, lsid: 1, expected: "tpid_0_lsid_1"},
	}
	for _, tc := range tcs {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, LogStreamDirName(tc.tpid, tc.lsid))
		})
	}
}

func TestVolume_CreateStorageNodePaths(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(2)
	)
	volumes := []string{
		t.TempDir(),
		t.TempDir(),
	}
	expectedPaths := []string{
		filepath.Join(volumes[0], StorageNodeDirName(cid, snid)),
		filepath.Join(volumes[1], StorageNodeDirName(cid, snid)),
	}
	snPaths, err := CreateStorageNodePaths(volumes, cid, snid)
	assert.NoError(t, err)
	for _, expectedPath := range expectedPaths {
		assert.Contains(t, snPaths, expectedPath)
	}
}

func TestVolume_CreateStorageNodePaths_NotPermittedVolume(t *testing.T) {
	volume := t.TempDir()
	assert.NoError(t, os.Chmod(volume, 0400))
	defer func() {
		assert.NoError(t, os.Chmod(volume, 0700))
	}()

	_, err := CreateStorageNodePaths([]string{volume}, 1, 2)
	assert.Error(t, err)
}

func TestVolume_ReadVolumes(t *testing.T) {
	dataDirs, err := ReadVolumes([]string{"./testdata/volume0"})
	assert.NoError(t, err)
	assert.Len(t, dataDirs, 4)

	// duplidated volumes
	dataDirs, err = ReadVolumes([]string{"./testdata/volume0", "./testdata/volume0"})
	assert.Errorf(t, err, "%v", dataDirs)

	// malformed subdirectories
	dataDirs, err = ReadVolumes([]string{"./testdata/volume1"})
	assert.Errorf(t, err, "%v", dataDirs)

	// malformed storage node path
	dataDirs, err = ReadVolumes([]string{"./testdata/volume2"})
	assert.Errorf(t, err, "%v", dataDirs)

	// malformed storage node path
	dataDirs, err = ReadVolumes([]string{"./testdata/volume3"})
	assert.Errorf(t, err, "%v", dataDirs)

	// not directory
	dataDirs, err = ReadVolumes([]string{"./testdata/volume4"})
	assert.Errorf(t, err, "%v", dataDirs)
}

func TestVolume_ParseDataDir(t *testing.T) {
	tcs := []struct {
		input  string
		volume string
		cid    types.ClusterID
		snid   types.StorageNodeID
		tpid   types.TopicID
		lsid   types.LogStreamID
		isErr  bool
	}{
		{
			input:  "/data/volume/cid_1_snid_2/tpid_3_lsid_4",
			volume: "/data/volume",
			cid:    1,
			snid:   2,
			tpid:   3,
			lsid:   4,
		},
		{
			input:  "/volume/cid_1_snid_2/tpid_3_lsid_4",
			volume: "/volume",
			cid:    1,
			snid:   2,
			tpid:   3,
			lsid:   4,
		},
		{
			input:  "/cid_1_snid_2/tpid_3_lsid_4",
			volume: "/",
			cid:    1,
			snid:   2,
			tpid:   3,
			lsid:   4,
		},
		{
			input:  "/data/volume/cid_1_snid_2/tpid_3_lsid_4/",
			volume: "/data/volume",
			cid:    1,
			snid:   2,
			tpid:   3,
			lsid:   4,
		},
		{
			input:  "/volume/cid_1_snid_2/tpid_3_lsid_4/",
			volume: "/volume",
			cid:    1,
			snid:   2,
			tpid:   3,
			lsid:   4,
		},
		{
			input:  "/cid_1_snid_2/tpid_3_lsid_4/",
			volume: "/",
			cid:    1,
			snid:   2,
			tpid:   3,
			lsid:   4,
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
	for _, tc := range tcs {
		t.Run(tc.input, func(t *testing.T) {
			dataDir, err := ParseDataDir(tc.input)
			if tc.isErr {
				assert.Error(t, err)
				return
			}

			assert.Equal(t, DataDir{
				Volume:        tc.volume,
				ClusterID:     tc.cid,
				StorageNodeID: tc.snid,
				TopicID:       tc.tpid,
				LogStreamID:   tc.lsid,
			}, dataDir)
			assert.Equal(t, filepath.Clean(tc.input), dataDir.String())
		})
	}
}

func TestVolume_DataDir(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(1)
	)
	testdata, err := filepath.Abs("./testdata")
	require.NoError(t, err)

	tcs := []struct {
		name     string
		dd       DataDir
		ok       bool
		readOnly bool
	}{
		{
			name: "ok",
			dd: DataDir{
				Volume:        filepath.Join(testdata, "volume0"),
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   1,
			},
			ok: true,
		},
		{
			name: "relative",
			dd: DataDir{
				Volume:        "./testdata/volume0",
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   1,
			},
		},
		{
			name: "no_such_path",
			dd: DataDir{
				Volume:        filepath.Join(testdata, "volume0"),
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   3,
			},
		},
		{
			name: "volume_not_directory",
			dd: DataDir{
				Volume:        filepath.Join(testdata, "volume4"),
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   1,
			},
		},
		{
			name: "storage_path_not_directory",
			dd: DataDir{
				Volume:        filepath.Join(testdata, "volume5"),
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   1,
			},
		},
		{
			name: "data_dir_not_directory",
			dd: DataDir{
				Volume:        filepath.Join(testdata, "volume6"),
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   1,
			},
		},
		{
			name: "read_only",
			dd: DataDir{
				Volume:        filepath.Join(testdata, "volume7"),
				ClusterID:     cid,
				StorageNodeID: snid,
				TopicID:       1,
				LogStreamID:   1,
			},
			readOnly: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnly {
				err := os.Chmod(tc.dd.String(), 0400)
				require.NoError(t, err)
				defer func() {
					err := os.Chmod(tc.dd.String(), 0700)
					require.NoError(t, err)
				}()
			}

			err = tc.dd.Valid(cid, snid)
			if tc.ok {
				assert.NoError(t, err)
				assert.Error(t, tc.dd.Valid(cid, snid+1))
				assert.Error(t, tc.dd.Valid(cid+1, snid))
				return
			}
			assert.Error(t, err)
		})
	}
}

func TestVolume_GetValidDataDirectories_NonStrict(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(1)
	)

	testdata, err := filepath.Abs("./testdata")
	require.NoError(t, err)

	tcs := []struct {
		name    string
		volumes []string
		given   []string
		ok      bool
		want    []string
	}{
		{
			name:    "ok",
			volumes: []string{"./testdata/volume0"},
			given: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
			},
			ok: true,
			want: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_3"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_4"),
			},
		},
		{
			name:    "read_more_dirs",
			volumes: []string{"./testdata/volume0", "./testdata/volume8"},
			given: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
			},
			ok: true,
			want: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_3"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_4"),
			},
		},
		{
			name:    "no_dirs",
			volumes: []string{"./testdata/volume0"},
			given: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_3"),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			read, err := ReadVolumes(tc.volumes)
			assert.NoError(t, err)

			dataDirs, err := GetValidDataDirectories(read, tc.given, false, cid, snid)
			if tc.ok {
				assert.NoError(t, err)
				actual := make([]string, 0, len(dataDirs))
				for _, dd := range dataDirs {
					actual = append(actual, dd.String())
				}
				sort.Strings(actual)
				sort.Strings(tc.want)
				assert.Equal(t, tc.want, actual)
				return
			}
			assert.Error(t, err)
		})
	}
}

func TestVolume_GetValidDataDirectories_Strict(t *testing.T) {
	const (
		cid  = types.ClusterID(1)
		snid = types.StorageNodeID(1)
	)

	testdata, err := filepath.Abs("./testdata")
	require.NoError(t, err)

	tcs := []struct {
		name     string
		volumes  []string
		given    []string
		ok       bool
		want     []string
		readOnly bool
	}{
		{
			name:    "ok",
			volumes: []string{"./testdata/volume0"},
			given: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_3"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_4"),
			},
			ok: true,
			want: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_3"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_4"),
			},
		},
		{
			name:    "invalid",
			volumes: []string{"./testdata/volume7"},
			given: []string{
				filepath.Join(testdata, "volume7", "cid_1_snid_1", "tpid_1_lsid_1"),
			},
			readOnly: true,
		},
		{
			name:    "too_many_dirs_read1",
			volumes: []string{"./testdata/volume0"},
			given: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
			},
		},
		{
			name:    "too_many_dirs_read2",
			volumes: []string{"./testdata/volume0"},
		},
		{
			name:    "too_many_dirs_given",
			volumes: []string{"./testdata/volume0"},
			given: []string{
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_1"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_1_lsid_2"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_3"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_2_lsid_4"),
				filepath.Join(testdata, "volume0", "cid_1_snid_1", "tpid_3_lsid_5"),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			read, err := ReadVolumes(tc.volumes)
			assert.NoError(t, err)

			if tc.readOnly {
				defer func() {
					for _, dd := range read {
						require.NoError(t, os.Chmod(dd.String(), 0700))
					}
				}()
				for _, dd := range read {
					require.NoError(t, os.Chmod(dd.String(), 0400))
				}
			}

			dataDirs, err := GetValidDataDirectories(read, tc.given, true, cid, snid)
			if tc.ok {
				assert.NoError(t, err)
				actual := make([]string, 0, len(dataDirs))
				for _, dd := range dataDirs {
					actual = append(actual, dd.String())
				}
				sort.Strings(actual)
				sort.Strings(tc.want)
				assert.Equal(t, tc.want, actual)
				return
			}
			assert.Error(t, err)
		})
	}
}
