package controller

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/admpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type Controller struct {
	executable string
	adminAddr  string
	logger     *zap.Logger
}

func New(adminAddr string, logger *zap.Logger) *Controller {
	return &Controller{
		executable: filepath.Join(binDir(), "varlogctl"),
		adminAddr:  adminAddr,
		logger:     logger,
	}
}

func (c *Controller) AddRaftPeer(t *testing.T, raftAddr, rpcAddr string) {
	args := []string{
		"metarepos",
		"add",
		"--raft-url", raftAddr,
		"--rpc-addr", rpcAddr,
		"--admin", c.adminAddr,
	}
	cmd := exec.Command(c.executable, args...)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "%s: %s", cmd.String(), string(out))
	c.logger.Info("add metadata repository node",
		zap.String("command", cmd.String()),
		zap.ByteString("output", out),
	)

	var mrNode varlogpb.MetadataRepositoryNode
	require.NoError(t, json.Unmarshal(out, &mrNode))
	require.NotZero(t, mrNode.NodeID)
}

func (c *Controller) AddStorageNode(t *testing.T, snid types.StorageNodeID, snaddr string) {
	cmd := exec.Command(
		c.executable,
		"storagenode",
		"add",
		"--storage-node-id", snid.String(),
		"--storage-node-address", snaddr,
		"--admin", c.adminAddr,
	)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "%s: %s", cmd.String(), string(out))
}

func (c *Controller) ListStorageNodes(t *testing.T) []admpb.StorageNodeMetadata {
	args := []string{
		"storagenode",
		"get",
		"--admin", c.adminAddr,
	}
	cmd := exec.Command(c.executable, args...)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "%s: %s", cmd.String(), string(out))

	var snms []admpb.StorageNodeMetadata
	require.NoError(t, json.Unmarshal(out, &snms))
	return snms
}
