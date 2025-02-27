package holescanner

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestHoleScanner(t *testing.T) {
	adminAddr := os.Getenv("ADMIN_ADDR")
	require.NotEmpty(t, adminAddr)

	mrAddr := os.Getenv("MR_ADDR")
	require.NotEmpty(t, mrAddr)

	adm, err := varlog.NewAdmin(context.Background(), adminAddr)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = adm.Close()
	})

	mrc, err := mrc.NewMetadataRepositoryClient(context.Background(), mrAddr)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mrc.Close()
	})

	snms, err := adm.ListStorageNodes(context.Background())
	require.NoError(t, err)

	for _, snm := range snms {
		func() {
			conn, err := rpc.NewConn(context.Background(), snm.Address)
			require.NoError(t, err)

			defer func() {
				_ = conn.Close()
			}()

			client := snpb.NewLogIOClient(conn.Conn)
			for _, lsrmd := range snm.LogStreamReplicas {
				if lsrmd.Status == varlogpb.LogStreamStatusRunning {
					continue
				}

				lastCommittedGLSN, err := mrc.Seal(context.Background(), lsrmd.LogStreamID)
				require.NoError(t, err)

				rsp, err := client.GetLogEntryRange(context.Background(), &snpb.GetLogEntryRangeRequest{
					TopicID:     lsrmd.TopicID,
					LogStreamID: lsrmd.LogStreamID,
				})
				require.NoError(t, err)

				if rsp.DataLast.LLSN < rsp.CommitLast.LLSN {
					t.Logf("SNID=%v TPID=%v LSID=%v DataHole=%v LastCommittedGLSN=%v(%v): %+v",
						snm.StorageNodeID,
						lsrmd.TopicID,
						lsrmd.LogStreamID,
						rsp.CommitLast.LLSN-rsp.DataLast.LLSN,
						lastCommittedGLSN,
						lastCommittedGLSN-rsp.CommitLast.GLSN,
						*rsp,
					)
				}
			}
		}()
	}
}
