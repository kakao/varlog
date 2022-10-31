package k8s

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/tests/ee/cluster"
	"github.com/kakao/varlog/tests/ee/cluster/k8s/client"
	"github.com/kakao/varlog/tests/ee/cluster/k8s/vault"
	"github.com/kakao/varlog/tests/ee/controller"
)

// Cluster represents a controller for the cluster on which test suites run.
type Cluster struct {
	config

	c *client.Client
}

var _ cluster.Cluster = (*Cluster)(nil)

func New(t *testing.T, opts ...Option) *Cluster {
	t.Helper()

	connInfo := vault.GetClusterConnectionInfo(t)
	kc, err := client.NewClient(
		client.WithClusterConnectionInfo(connInfo),
	)
	require.NoError(t, err)

	cfg, err := newConfig(opts)
	require.NoError(t, err)

	return &Cluster{
		config: cfg,
		c:      kc,
	}
}

// Setup initializes test cluster.
//
// It stops the admin server, metadata repositories, and storage nodes if they
// are running yet. It also wipes out data from metadata repositories and
// storage nodes.
// To start up the cluster, a metadata repository starts at first. The admin
// server, then, starts. The remaining metadata repositories start after
// beginning the admin server. At last, the storage nodes are created.
func (c *Cluster) Setup(ctx context.Context, t *testing.T) {
	t.Helper()

	c.Logger().Info("setup cluster")

	stopDuration := c.reset(ctx, t)

	now := time.Now()
	c.StartMetadataRepositoryNodes(ctx, t, 1)
	c.StartAdminServer(ctx, t)
	c.StartMetadataRepositoryNodes(ctx, t, c.NumMetaRepos())
	c.StartStorageNodes(ctx, t, c.NumStorageNodes())
	startDuration := time.Since(now)

	c.Logger().Debug("durations for setup cluster",
		zap.Duration("stopDuration", stopDuration),
		zap.Duration("startDuration", startDuration),
	)
}

func (c *Cluster) reset(ctx context.Context, t *testing.T) (duration time.Duration) {
	now := time.Now()
	defer func() {
		duration = time.Since(now)
	}()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		c.StopAdminServer(ctx, t)
	}()
	go func() {
		defer wg.Done()
		c.StopMetadataRepositoryNodes(ctx, t)
		c.ClearMetadataRepositoryNodeData(ctx, t)
	}()
	go func() {
		defer wg.Done()
		c.StopStorageNodes(ctx, t)
		c.ClearStorageNodeData(ctx, t)
	}()
	wg.Wait()
	return duration
}

// StartAdminServer starts the admin server on the node group varlogadm of the
// cluster.
//
// It sets the number of desired replicas of the varlogadm to one and waits
// until the pod runs.
func (c *Cluster) StartAdminServer(ctx context.Context, t *testing.T) {
	t.Helper()
	c.Logger().Info("starting varlogadm")
	c.updateAdminServerReplicas(ctx, t, 1)
}

// StopAdminServer stops the admin server.
//
// It sets the number of desired replicas of the varlogadm to zero and waits
// until the pod stops.
func (c *Cluster) StopAdminServer(ctx context.Context, t *testing.T) {
	t.Helper()
	c.Logger().Info("stopping varlogadm")
	c.updateAdminServerReplicas(ctx, t, 0)
}

func (c *Cluster) ListAdminPods(ctx context.Context, t *testing.T) []core.Pod {
	t.Helper()
	pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorAdmin, true)
	assert.NoError(t, err)
	return pods
}

func (c *Cluster) updateAdminServerReplicas(ctx context.Context, t *testing.T, desired int32) {
	t.Helper()

	err := c.c.UpdateDeploymentReplicas(ctx, c.varlogNamespace, defaultDeploymentNameAdmin, desired)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		pods := c.ListAdminPods(ctx, t)
		return len(pods) == int(desired)
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

// AdminServerAddress returns the address of the admin server.
func (c *Cluster) AdminServerAddress(ctx context.Context, t *testing.T) string {
	t.Helper()
	service, err := c.c.Service(ctx, c.ingressNamespace, c.serviceNameAdmin)
	assert.NoError(t, err)
	return fmt.Sprintf("%s:%d", service.Status.LoadBalancer.Ingress[0].IP, service.Spec.Ports[0].Port)
}

// StartMetadataRepositoryNodes will result in the given number of running
// metadata repositories on the node group varlogmr of the cluster.
// If there are already nodes in the cluster more than the given number, this
// method will fail.
func (c *Cluster) StartMetadataRepositoryNodes(ctx context.Context, t *testing.T, desired int) {
	t.Helper()

	c.Logger().Info("starting metadata repositories", zap.Int("desired", desired))
	c.SetNumMetadataRepositories(ctx, t, desired)
}

// StopMetadataRepositoryNodes stops all metadata repositories.
func (c *Cluster) StopMetadataRepositoryNodes(ctx context.Context, t *testing.T) {
	t.Helper()

	c.Logger().Info("stopping all metadata repositories")
	nodes := c.ListMetadataRepositoryNodes(ctx, t)
	for _, node := range nodes {
		c.updateNodeLabel(ctx, t, node.Name, c.nodeStatusLabelMetaRepos, c.nodeStatusStopped)
	}
	c.SetNumMetadataRepositories(ctx, t, 0)
}

// SetNumMetadataRepositories sets the number of running metadata repositories
// to the given number. It can either shrink or expand the number of metadata
// repositories.
func (c *Cluster) SetNumMetadataRepositories(ctx context.Context, t *testing.T, desired int) {
	t.Helper()

	nodes := c.ListMetadataRepositoryNodes(ctx, t)

	started := make([]string, 0, len(nodes))
	free := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if status := node.Labels[c.nodeStatusLabelMetaRepos]; status == c.nodeStatusStarted {
			started = append(started, node.Name)
			continue
		}
		free = append(free, node.Name)
	}

	need := desired - len(started)
	if need > 0 {
		// start nodes
		assert.GreaterOrEqual(t, len(free), need)
		for i := 0; i < need; i++ {
			nodeName := free[i]
			c.updateNodeLabel(ctx, t, nodeName, c.nodeStatusLabelMetaRepos, c.nodeStatusStarted)
		}
	}
	c.Logger().Debug("setting the number of metadata repository nodes",
		zap.Int("desired", desired),
		zap.Strings("startedNodes", started),
		zap.Strings("freeNodes", free),
		zap.Int("need", need),
	)

	err := c.c.UpdateStatefulSetReplicas(ctx, c.varlogNamespace, defaultStatefulSetNameMetaRepos, int32(desired))
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		pods := c.ListMetadataRepositoryPods(ctx, t)
		return len(pods) == int(desired)
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

// GetMetadataRepositoryNodeName returns a Kubernetes node name of the
// repository node specified by the argument nid.
//
// Note that the metadata repository should use the hostNetwork of Kubernetes.
// To get a node name, this method compares the host IP and container port of
// the metadata repository pods to the RPC address of the metadata repository.
func (c *Cluster) GetMetadataRepositoryNodeName(ctx context.Context, t *testing.T, nid types.NodeID) string {
	t.Helper()

	adminAddr := c.AdminServerAddress(ctx, t)
	adm, err := varlog.NewAdmin(ctx, adminAddr)
	assert.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	mrnode, err := adm.GetMetadataRepositoryNode(ctx, nid)
	assert.NoError(t, err)
	assert.NotEmpty(t, mrnode.RPCAddr)

	pods := c.ListMetadataRepositoryPods(ctx, t)
	for _, pod := range pods {
		hostIP := pod.Status.HostIP
		// The varlogmr has only one container.
		container := pod.Spec.Containers[0]
		for _, port := range container.Ports {
			addr := fmt.Sprintf("%s:%d", hostIP, port.ContainerPort)
			if mrnode.RPCAddr == addr {
				return pod.Spec.NodeName
			}
		}
	}
	assert.Failf(t, "get metadata repository node name: no such node id %s", nid.String())
	return ""
}

// StopMetadataRepositoryNode stops the metadata repository node specified by
// the argument nid.
// func (tc *Cluster) StopMetadataRepositoryNode(ctx context.Context, t *testing.T, nid types.NodeID) {
//	t.Helper()
//	nodeName := tc.GetMetadataRepositoryNodeName(ctx, t, nid)
//	err := tc.c.ReplaceNodeLabel(ctx, nodeName, nodelabel.DefaultNodeStatusLabelMetaRepos, nodelabel.DefaultNodeStatusStopped)
//	assert.NoError(t, err)
// }

// ListMetadataRepositoryPods returns a list of all pods running varlogmr.
func (c *Cluster) ListMetadataRepositoryPods(ctx context.Context, t *testing.T) []core.Pod {
	t.Helper()
	pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorMetaRepos, true)
	assert.NoError(t, err)
	return pods
}

// ListMetadataRepositoryNodes returns a list of Kubernetes nodes labeled the
// node group varlogmr. Note that there can be no running pods in the result
// nodes.
func (c *Cluster) ListMetadataRepositoryNodes(ctx context.Context, t *testing.T) []core.Node {
	t.Helper()
	nodes, err := c.c.Nodes(ctx, map[string]string{
		c.nodeGroupLabel: c.nodeGroupMetaRepos,
	})
	assert.NoError(t, err)
	return nodes
}

func (c *Cluster) MetadataRepositoryAddress(ctx context.Context, t *testing.T) string {
	t.Helper()
	service, err := c.c.Service(ctx, c.ingressNamespace, c.serviceNameMetaRepos)
	assert.NoError(t, err)
	return fmt.Sprintf("%s:%d", service.Status.LoadBalancer.Ingress[0].IP, service.Spec.Ports[0].Port)
}

// ClearMetadataRepositoryNodeData wipes out data from metadata repositories.
// This method cleans up all nodes in the node group varlogmr regardless of
// whether the metadata repositories are running or not.
func (c *Cluster) ClearMetadataRepositoryNodeData(ctx context.Context, t *testing.T) {
	t.Helper()

	c.Logger().Info("wiping out data from metadata repositories")

	nodes := c.ListMetadataRepositoryNodes(ctx, t)
	for _, node := range nodes {
		c.updateNodeLabel(ctx, t, node.Name, c.nodeStatusLabelMetaRepos, c.nodeStatusCleared)
	}

	assert.Eventually(t, func() bool {
		pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorMetaReposClear, true)
		assert.NoError(t, err)
		return len(pods) == len(nodes)
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)

	for _, node := range nodes {
		c.updateNodeLabel(ctx, t, node.Name, c.nodeStatusLabelMetaRepos, c.nodeStatusStopped)
	}

	assert.Eventually(t, func() bool {
		pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorMetaReposClear, true)
		assert.NoError(t, err)
		return len(pods) == 0
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

// StartStorageNodes will result in the given number of running storage nodes
// on the node group varlogsn of the cluster. If there are already nodes in the
// cluster more than the given number, this method will fail.
//
// Note that the DaemonSet varlogsn should have a proper nodeAffinity setting
// since it sets the node label `varlogsn-status` of the node group varlogsn to
// `started`.
func (c *Cluster) StartStorageNodes(ctx context.Context, t *testing.T, desired int) {
	t.Helper()

	nodes := c.ListStorageNodeNodes(ctx, t)
	started := make([]string, 0, len(nodes))
	free := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if status := node.Labels[c.nodeStatusLabelStorageNode]; status == c.nodeStatusStarted {
			started = append(started, node.Name)
			continue
		}
		free = append(free, node.Name)
	}

	need := int(desired) - len(started)
	assert.GreaterOrEqual(t, need, 0)
	assert.GreaterOrEqual(t, len(free), need)

	c.Logger().Info("starting storage nodes",
		zap.Int("desired", desired),
		zap.Strings("startedNodes", started),
		zap.Strings("freeNodes", free),
		zap.Int("need", need),
	)

	for i := 0; i < need; i++ {
		nodeName := free[i]
		c.updateNodeLabel(ctx, t, nodeName, c.nodeStatusLabelStorageNode, c.nodeStatusStarted)
	}

	assert.Eventually(t, func() bool {
		pods := c.ListStorageNodePods(ctx, t)
		return len(pods) == desired
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

// StopStorageNodes stops all storage nodes.
// It changes the node label DefaultNodeStatusLabelStorageNode from DefaultNodeStatusStarted to DefaultNodeStatusStopped.
func (c *Cluster) StopStorageNodes(ctx context.Context, t *testing.T) {
	t.Helper()

	c.Logger().Info("stopping all storage nodes")

	nodes := c.ListStorageNodeNodes(ctx, t)
	for _, node := range nodes {
		c.updateNodeLabel(ctx, t, node.Name, c.nodeStatusLabelStorageNode, c.nodeStatusStopped)
	}

	assert.Eventually(t, func() bool {
		pods := c.ListStorageNodePods(ctx, t)
		return len(pods) == 0
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

func (c *Cluster) StartStorageNode(ctx context.Context, t *testing.T, nodeName string) bool {
	t.Helper()

	c.Logger().Info("starting a storage node", zap.String("nodeName", nodeName))

	c.updateNodeLabel(ctx, t, nodeName, c.nodeStatusLabelStorageNode, c.nodeStatusStarted)

	return assert.Eventually(t, func() bool {
		pods := c.ListStorageNodePods(ctx, t)
		for _, pod := range pods {
			if pod.Spec.NodeName == nodeName {
				return c.c.PodReady(pod)
			}
		}
		return false
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

// StopStorageNode stops the storage node specified by the argument snid and
// returns the Kubernetes node name that had run the storage node.
func (c *Cluster) StopStorageNode(ctx context.Context, t *testing.T, snid types.StorageNodeID) (nodeName string) {
	t.Helper()

	oldPods := c.ListStorageNodePods(ctx, t)
	nodeName = c.GetStorageNodeName(ctx, t, snid)
	assert.Condition(t, func() bool {
		for _, pod := range oldPods {
			if pod.Spec.NodeName == nodeName {
				return true
			}
		}
		return false
	})

	c.Logger().Info("stopping a storage node",
		zap.Int32("snid", int32(snid)),
		zap.String("nodeName", nodeName),
	)

	c.updateNodeLabel(ctx, t, nodeName, c.nodeStatusLabelStorageNode, c.nodeStatusStopped)

	assert.Eventually(t, func() bool {
		newPods := c.ListStorageNodePods(ctx, t)
		return len(newPods) == len(oldPods)-1
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)

	return nodeName
}

func (c *Cluster) GetStorageNodeName(ctx context.Context, t *testing.T, snid types.StorageNodeID) string {
	t.Helper()

	adminAddr := c.AdminServerAddress(ctx, t)
	adm, err := varlog.NewAdmin(ctx, adminAddr)
	assert.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	snm, err := adm.GetStorageNode(ctx, snid)
	assert.NoError(t, err)

	pods := c.ListStorageNodePods(ctx, t)
	for _, pod := range pods {
		hostIP := pod.Status.HostIP
		container := pod.Spec.Containers[0]
		for _, port := range container.Ports {
			addr := fmt.Sprintf("%s:%d", hostIP, port.ContainerPort)
			if snm.Address == addr {
				return pod.Spec.NodeName
			}
		}
	}
	assert.Failf(t, "get storage node name: no such node id %s", snid.String())
	return ""
}

// ClearStorageNodeData wipes out data from storage nodes.
// This method cleans up all nodes in the node group varlogsn regardless of
// whether the storage nodes are running or not.
func (c *Cluster) ClearStorageNodeData(ctx context.Context, t *testing.T) {
	t.Helper()

	c.Logger().Info("wiping out data from storage nodes")

	nodes := c.ListStorageNodeNodes(ctx, t)

	for _, node := range nodes {
		c.updateNodeLabel(ctx, t, node.Name, c.nodeStatusLabelStorageNode, c.nodeStatusCleared)
	}

	assert.Eventually(t, func() bool {
		pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorStorageNodeClear, true)
		assert.NoError(t, err)
		return len(pods) == len(nodes)
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)

	for _, node := range nodes {
		c.updateNodeLabel(ctx, t, node.Name, c.nodeStatusLabelStorageNode, c.nodeStatusStopped)
	}

	assert.Eventually(t, func() bool {
		pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorStorageNodeClear, true)
		assert.NoError(t, err)
		return len(pods) == 0
	}, c.asyncTestWaitDuration, c.asyncTestCheckInterval)
}

// ListStorageNodePods returns a list of all pods running varlogsn.
func (c *Cluster) ListStorageNodePods(ctx context.Context, t *testing.T) []core.Pod {
	t.Helper()
	pods, err := c.c.Pods(ctx, c.varlogNamespace, c.appSelectorStorageNode, true)
	assert.NoError(t, err)
	return pods
}

// ListStorageNodeNodes returns a list of Kubernetes nodes labeled the node
// group varlogsn. Note that there can be no running pods in the result nodes.
func (c *Cluster) ListStorageNodeNodes(ctx context.Context, t *testing.T) []core.Node {
	t.Helper()
	nodes, err := c.c.Nodes(ctx, map[string]string{
		c.nodeGroupLabel: c.nodeGroupStorageNode,
	})
	assert.NoError(t, err)
	return nodes
}

func (c *Cluster) updateNodeLabel(ctx context.Context, t *testing.T, nodeName, label, value string) {
	t.Helper()
	c.Logger().Debug("updating node label",
		zap.String("nodeName", nodeName),
		zap.String(label, value),
	)
	err := c.c.AddNodeLabel(ctx, nodeName, label, value)
	assert.NoError(t, err)
}

func (c *Cluster) Close(context.Context, *testing.T) {}

func (c *Cluster) Controller() *controller.Controller {
	panic("not implemented")
}

func (c *Cluster) String() string {
	panic("k8s")
}
