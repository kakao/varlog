package x

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestHelloWorld(t *testing.T) {
	const networkName = "testnet"

	network, err := testcontainers.GenericNetwork(context.TODO(), testcontainers.GenericNetworkRequest{
		ProviderType: testcontainers.ProviderDocker,
		NetworkRequest: testcontainers.NetworkRequest{
			Name:           networkName,
			CheckDuplicate: true,
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := network.Remove(context.Background())
		require.NoError(t, err)
	})

	mr := NewMetadataRepository(t, networkName)
	t.Cleanup(func() {
		err = mr.Terminate(context.TODO())
		require.NoError(t, err)
	})

	ip, err := mr.ContainerIP(context.TODO())
	require.NoError(t, err)
	mrAddr := ip + ":9092"
	t.Logf("mrAddr: %s", mrAddr)

	adm := NewAdmin(t, networkName, mrAddr)
	t.Cleanup(func() {
		err := adm.Terminate(context.TODO())
		require.NoError(t, err)
	})

	stop := make(chan struct{})
	<-stop

}
