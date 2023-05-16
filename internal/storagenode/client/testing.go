package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

func TestNewLogClient(t *testing.T, rpcClient snpb.LogIOClient, target varlogpb.StorageNode) *LogClient {
	require.NotNil(t, rpcClient)
	require.False(t, target.StorageNodeID.Invalid())
	require.NotEmpty(t, target.Address)
	return &LogClient{
		rpcClient: rpcClient,
		target:    target,
	}
}
