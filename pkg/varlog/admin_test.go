package varlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/vmspb"
)

func TestAdmin_Timeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tms := admin.TestNewMockServer(t, ctrl)
	tms.MockClusterManagerServer.EXPECT().ListStorageNodes(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *vmspb.ListStorageNodesRequest) (*vmspb.ListStorageNodesResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	).AnyTimes()
	defer tms.Close()
	tms.Run()

	const defaultTimeout = 50 * time.Millisecond

	client, err := varlog.NewAdmin(context.Background(), tms.Address(),
		varlog.WithDefaultAdminCallOptions(
			varlog.WithTimeout(defaultTimeout),
		),
	)
	assert.NoError(t, err)
	defer func() {
		err := client.Close()
		assert.NoError(t, err)
	}()

	// default timeout
	start := time.Now()
	_, err = client.ListStorageNodes(context.Background())
	timeout1 := time.Since(start)
	assert.Error(t, err)

	// overriding default timeout
	start = time.Now()
	_, err = client.ListStorageNodes(context.Background(), varlog.WithTimeout(defaultTimeout*2))
	timeout2 := time.Since(start)
	assert.Error(t, err)
	assert.Greater(t, timeout2, timeout1)
}
