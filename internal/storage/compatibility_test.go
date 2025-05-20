package storage

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// basepath is the root directory of this package.
var basepath string

func init() {
	_, currentFile, _, _ := runtime.Caller(0)
	basepath = filepath.Dir(currentFile)
}

var update = flag.Bool("update", false, "update test data generated from Pebble v1")

func TestStorage_CompatibilityV1(t *testing.T) {
	wantList := []struct {
		key   []byte
		value []byte
	}{
		{key: []byte("key1"), value: []byte("value1")},
		{key: []byte("key2"), value: []byte("value2")},
		{key: []byte("key3"), value: []byte("value3")},
	}

	tcs := []struct {
		golden string
		old    pebble.FormatMajorVersion
		new    pebble.FormatMajorVersion
		skip   bool
	}{
		{
			golden: "from_FormatMostCompatible_to_FormatFlushableIngest",
			old:    pebble.FormatMajorVersion(1),
			new:    pebble.FormatFlushableIngest,
			skip:   true,
		},
		{
			golden: "from_FormatMostCompatible_to_FormatVirtualSSTables",
			old:    pebble.FormatMajorVersion(1),
			new:    pebble.FormatVirtualSSTables,
			skip:   true,
		},
		{
			golden: "from_FormatFlushableIngest_to_FormatVirtualSSTables",
			old:    pebble.FormatFlushableIngest,
			new:    pebble.FormatVirtualSSTables,
		},
		{
			golden: "from_FormatVirtualSSTables_to_FormatVirtualSSTables",
			old:    pebble.FormatVirtualSSTables,
			new:    pebble.FormatVirtualSSTables,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.golden, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping: FormatMostCompatible from v1 cannot be migrated to any v2 format")
			}

			path := filepath.Join(basepath, "testdata", tc.golden)
			opts := &pebble.Options{}

			if *update {
				opts.FormatMajorVersion = tc.old

				db, err := pebble.Open(path, opts)
				require.NoError(t, err)

				for _, kv := range wantList {
					err = db.Set(kv.key, kv.value, pebble.Sync)
					require.NoError(t, err)
				}

				err = db.Close()
				require.NoError(t, err)

				return
			}

			testPath := t.TempDir()
			err := os.CopyFS(testPath, os.DirFS(path))
			require.NoError(t, err)

			opts.FormatMajorVersion = tc.new
			db, err := pebble.Open(testPath, opts)
			require.NoError(t, err)
			t.Cleanup(func() {
				err := db.Close()
				require.NoError(t, err)
			})

			for _, want := range wantList {
				got, closer, err := db.Get(want.key)
				require.NoError(t, err)

				err = closer.Close()
				require.NoError(t, err)

				require.Equal(t, want.value, got)
			}
		})
	}
}
