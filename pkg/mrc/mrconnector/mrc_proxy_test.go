package mrconnector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
)

func TestMRProxyCloseWithoutCalls(t *testing.T) {
	const num = 10

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	connector := &connectorImpl{}

	cl := mrc.NewMockMetadataRepositoryClient(ctrl)
	cl.EXPECT().Close().Return(nil).AnyTimes()

	mcl := mrc.NewMockMetadataRepositoryManagementClient(ctrl)
	mcl.EXPECT().Close().Return(nil).AnyTimes()

	var wg sync.WaitGroup
	proxy := newMRProxy(connector, types.NodeID(1), cl, mcl)
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, proxy.Close())
		}()
	}
	wg.Wait()
}

func TestMRProxyCloseWithConcurrentCalls(t *testing.T) {
	const num = 10

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	connector := &connectorImpl{}

	cl := mrc.NewMockMetadataRepositoryClient(ctrl)
	cl.EXPECT().Close().Return(nil).AnyTimes()

	mcl := mrc.NewMockMetadataRepositoryManagementClient(ctrl)
	mcl.EXPECT().Close().Return(nil).AnyTimes()

	mcl.EXPECT().AddPeer(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ types.ClusterID, _ types.NodeID, _ string) error {
			time.Sleep(time.Millisecond)
			return nil
		},
	).AnyTimes()

	var wg sync.WaitGroup
	proxy := newMRProxy(connector, types.NodeID(1), cl, mcl)
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				assert.NoError(t, proxy.Close())
				wg.Done()
			}()
			_ = proxy.AddPeer(context.Background(), 1, 1, "url")
		}()
	}
	wg.Wait()
}
