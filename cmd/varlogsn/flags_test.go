package main

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestStorageStoreSetting(t *testing.T) {
	tcs := []struct {
		name    string
		input   string
		want    *StorageStoreSetting
		wantErr bool
	}{
		{
			name:  "ValidFullSettings",
			input: "--storage-value-store=wal=true,sync_wal=false,wal_bytes_per_sync=128KiB,sst_bytes_per_sync=1MiB,mem_table_size=64MiB,mem_table_stop_writes_threshold=4,flush_split_bytes=32MiB,l0_compaction_file_threshold=500,l0_compaction_threshold=2,l0_stop_writes_threshold=1000,l0_target_file_size=64MiB,lbase_max_bytes=64MiB,max_concurrent_compactions=4,trim_delay=10s,trim_rate=128MiB,max_open_files=16384,verbose=true",
			want: &StorageStoreSetting{
				wal:                         true,
				syncWAL:                     false,
				walBytesPerSync:             131072,
				sstBytesPerSync:             1048576,
				memTableSize:                64 << 20,
				memTableStopWritesThreshold: 4,
				flushSplitBytes:             32 << 20,
				l0CompactionFileThreshold:   500,
				l0CompactionThreshold:       2,
				l0StopWritesThreshold:       1000,
				l0TargetFileSize:            64 << 20,
				lbaseMaxBytes:               64 << 20,
				maxConcurrentCompactions:    4,
				trimDelay:                   10 * time.Second,
				trimRate:                    128 << 20,
				maxOpenFiles:                16384,
				verbose:                     true,
			},
			wantErr: false,
		},
		{
			name:  "DefaultSettings",
			input: "",
			want: func() *StorageStoreSetting {
				var ssf StorageStoreSetting
				ssf.init()
				return &ssf
			}(),
			wantErr: false,
		},
		{
			name:    "ErrorMissingValueForFlag",
			input:   "--storage-value-store",
			wantErr: true,
		},
		{
			name:    "ErrorInvalidBooleanValue",
			input:   "--storage-value-store=wal=yes",
			wantErr: true,
		},
		{
			name:    "ErrorNegativeSize",
			input:   "--storage-value-store=mem_table_size=-32B",
			wantErr: true,
		},
		{
			name:    "ErrorUnknownSettingKey",
			input:   "--storage-value-store=unknown=1MiB",
			wantErr: true,
		},
		{
			name:    "ErrorInvalidValueType",
			input:   "--storage-value-store=wal=1MiB",
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			app := &cli.App{
				Writer:    io.Discard,
				ErrWriter: io.Discard,
				Flags: []cli.Flag{
					&cli.GenericFlag{
						Name:     "storage-value-store",
						Category: categoryStorage,
						EnvVars:  []string{"STORAGE_VALUE_STORE"},
						Value: func() cli.Generic {
							s := &StorageStoreSetting{}
							s.init()
							return s
						}(),
					},
				},
				Action: func(c *cli.Context) error {
					iface := c.Generic(flagStorageValueStore.Name)
					require.NotNil(t, iface)
					got, ok := iface.(*StorageStoreSetting)
					require.True(t, ok)
					require.Equal(t, tc.want, got)
					return nil
				},
			}

			args := []string{"app"}
			if tc.input != "" {
				args = append(args, tc.input)
			}
			err := app.Run(args)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
