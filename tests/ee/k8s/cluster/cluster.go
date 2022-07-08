package cluster

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

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

// TestCluster represents a controller for the cluster on which test suites
// run.
type TestCluster struct {
	config
}

func NewTestCluster(t *testing.T, opts ...Option) *TestCluster {
	t.Helper()
	cfg, err := newConfig(opts)
	require.NoError(t, err)
	return &TestCluster{config: cfg}
}

// Setup initializes test cluster.
//
// It stops the admin server, metadata repositories, and storage nodes if they
// are running yet. It also wipes out data from metadata repositories and
// storage nodes.
// To start up the cluster, a metadata repository starts at first. The admin
// server, then, starts. The remaining metadata repositories start after
// beginning the admin server. At last, the storage nodes are created.
func (tc *TestCluster) Setup(ctx context.Context, t *testing.T) {
	t.Helper()

	tc.logger.Info("setup cluster")

	stopDuration := tc.reset(ctx, t)

	now := time.Now()
	tc.StartMetadataRepositoryNodes(ctx, t, 1)
	tc.StartAdminServer(ctx, t)
	tc.StartMetadataRepositoryNodes(ctx, t, tc.numMetaRepos)
	tc.StartStorageNodes(ctx, t, tc.numStorageNodes)
	startDuration := time.Since(now)

	tc.logger.Debug("durations for setup cluster",
		zap.Duration("stopDuration", stopDuration),
		zap.Duration("startDuration", startDuration),
	)
}

func (tc *TestCluster) reset(ctx context.Context, t *testing.T) (duration time.Duration) {
	now := time.Now()
	defer func() {
		duration = time.Since(now)
	}()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		tc.StopAdminServer(ctx, t)
	}()
	go func() {
		defer wg.Done()
		tc.StopMetadataRepositoryNodes(ctx, t)
		tc.ClearMetadataRepositoryNodeData(ctx, t)
	}()
	go func() {
		defer wg.Done()
		tc.StopStorageNodes(ctx, t)
		tc.ClearStorageNodeData(ctx, t)
	}()
	wg.Wait()
	return duration
}

// StartAdminServer starts the admin server on the node group varlogadm of the
// cluster.
//
// It sets the number of desired replicas of the varlogadm to one and waits
// until the pod runs.
func (tc *TestCluster) StartAdminServer(ctx context.Context, t *testing.T) {
	t.Helper()
	tc.logger.Info("starting varlogadm")
	tc.updateAdminServerReplicas(ctx, t, 1)
}

// StopAdminServer stops the admin server.
//
// It sets the number of desired replicas of the varlogadm to zero and waits
// until the pod stops.
func (tc *TestCluster) StopAdminServer(ctx context.Context, t *testing.T) {
	t.Helper()
	tc.logger.Info("stopping varlogadm")
	tc.updateAdminServerReplicas(ctx, t, 0)
}

func (tc *TestCluster) ListAdminPods(ctx context.Context, t *testing.T) []core.Pod {
	t.Helper()
	pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorAdmin, true)
	assert.NoError(t, err)
	return pods
}

func (tc *TestCluster) updateAdminServerReplicas(ctx context.Context, t *testing.T, desired int32) {
	t.Helper()

	err := tc.c.UpdateDeploymentReplicas(ctx, tc.varlogNamespace, defaultDeploymentNameAdmin, desired)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		pods := tc.ListAdminPods(ctx, t)
		return len(pods) == int(desired)
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

// AdminServerAddress returns the address of the admin server.
func (tc *TestCluster) AdminServerAddress(ctx context.Context, t *testing.T) string {
	t.Helper()
	service, err := tc.c.Service(ctx, tc.ingressNamespace, tc.serviceNameAdmin)
	assert.NoError(t, err)
	return fmt.Sprintf("%s:%d", service.Status.LoadBalancer.Ingress[0].IP, service.Spec.Ports[0].Port)
}

// StartMetadataRepositoryNodes will result in the given number of running
// metadata repositories on the node group varlogmr of the cluster.
// If there are already nodes in the cluster more than the given number, this
// method will fail.
func (tc *TestCluster) StartMetadataRepositoryNodes(ctx context.Context, t *testing.T, desired int32) {
	t.Helper()

	tc.logger.Info("starting metadata repositories", zap.Int32("desired", desired))
	tc.SetNumMetadataRepositories(ctx, t, desired)
}

// StopMetadataRepositoryNodes stops all metadata repositories.
func (tc *TestCluster) StopMetadataRepositoryNodes(ctx context.Context, t *testing.T) {
	t.Helper()

	tc.logger.Info("stopping all metadata repositories")
	nodes := tc.ListMetadataRepositoryNodes(ctx, t)
	for _, node := range nodes {
		tc.updateNodeLabel(ctx, t, node.Name, tc.nodeStatusLabelMetaRepos, tc.nodeStatusStopped)
	}
	tc.SetNumMetadataRepositories(ctx, t, 0)
}

// SetNumMetadataRepositories sets the number of running metadata repositories
// to the given number. It can either shrink or expand the number of metadata
// repositories.
func (tc *TestCluster) SetNumMetadataRepositories(ctx context.Context, t *testing.T, desired int32) {
	t.Helper()

	nodes := tc.ListMetadataRepositoryNodes(ctx, t)

	started := make([]string, 0, len(nodes))
	free := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if status := node.Labels[tc.nodeStatusLabelMetaRepos]; status == tc.nodeStatusStarted {
			started = append(started, node.Name)
			continue
		}
		free = append(free, node.Name)
	}

	need := int(desired) - len(started)
	if need > 0 {
		// start nodes
		assert.GreaterOrEqual(t, len(free), need)
		for i := 0; i < need; i++ {
			nodeName := free[i]
			tc.updateNodeLabel(ctx, t, nodeName, tc.nodeStatusLabelMetaRepos, tc.nodeStatusStarted)
		}
	}
	tc.logger.Debug("setting the number of metadata repository nodes",
		zap.Int32("desired", desired),
		zap.Strings("startedNodes", started),
		zap.Strings("freeNodes", free),
		zap.Int("need", need),
	)

	err := tc.c.UpdateStatefulSetReplicas(ctx, tc.varlogNamespace, defaultStatefulSetNameMetaRepos, desired)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		pods := tc.ListMetadataRepositoryPods(ctx, t)
		return len(pods) == int(desired)
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

// GetMetadataRepositoryNodeName returns a Kubernetes node name of the
// repository node specified by the argument nid.
//
// Note that the metadata repository should use the hostNetwork of Kubernetes.
// To get a node name, this method compares the host IP and container port of
// the metadata repository pods to the RPC address of the metadata repository.
func (tc *TestCluster) GetMetadataRepositoryNodeName(ctx context.Context, t *testing.T, nid types.NodeID) string {
	t.Helper()

	adminAddr := tc.AdminServerAddress(ctx, t)
	adm, err := varlog.NewAdmin(ctx, adminAddr)
	assert.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	mrnode, err := adm.GetMetadataRepositoryNode(ctx, nid)
	assert.NoError(t, err)
	assert.NotEmpty(t, mrnode.RPCAddr)

	pods := tc.ListMetadataRepositoryPods(ctx, t)
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
// func (tc *TestCluster) StopMetadataRepositoryNode(ctx context.Context, t *testing.T, nid types.NodeID) {
//	t.Helper()
//	nodeName := tc.GetMetadataRepositoryNodeName(ctx, t, nid)
//	err := tc.c.ReplaceNodeLabel(ctx, nodeName, nodelabel.DefaultNodeStatusLabelMetaRepos, nodelabel.DefaultNodeStatusStopped)
//	assert.NoError(t, err)
// }

// ListMetadataRepositoryPods returns a list of all pods running varlogmr.
func (tc *TestCluster) ListMetadataRepositoryPods(ctx context.Context, t *testing.T) []core.Pod {
	t.Helper()
	pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorMetaRepos, true)
	assert.NoError(t, err)
	return pods
}

// ListMetadataRepositoryNodes returns a list of Kubernetes nodes labeled the
// node group varlogmr. Note that there can be no running pods in the result
// nodes.
func (tc *TestCluster) ListMetadataRepositoryNodes(ctx context.Context, t *testing.T) []core.Node {
	t.Helper()
	nodes, err := tc.c.Nodes(ctx, map[string]string{
		tc.nodeGroupLabel: tc.nodeGroupMetaRepos,
	})
	assert.NoError(t, err)
	return nodes
}

func (tc *TestCluster) MetadataRepositoryAddress(ctx context.Context, t *testing.T) string {
	t.Helper()
	service, err := tc.c.Service(ctx, tc.ingressNamespace, tc.serviceNameMetaRepos)
	assert.NoError(t, err)
	return fmt.Sprintf("%s:%d", service.Status.LoadBalancer.Ingress[0].IP, service.Spec.Ports[0].Port)
}

// ClearMetadataRepositoryNodeData wipes out data from metadata repositories.
// This method cleans up all nodes in the node group varlogmr regardless of
// whether the metadata repositories are running or not.
func (tc *TestCluster) ClearMetadataRepositoryNodeData(ctx context.Context, t *testing.T) {
	t.Helper()

	tc.logger.Info("wiping out data from metadata repositories")

	nodes := tc.ListMetadataRepositoryNodes(ctx, t)
	for _, node := range nodes {
		tc.updateNodeLabel(ctx, t, node.Name, tc.nodeStatusLabelMetaRepos, tc.nodeStatusCleared)
	}

	assert.Eventually(t, func() bool {
		pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorMetaReposClear, true)
		assert.NoError(t, err)
		return len(pods) == len(nodes)
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)

	for _, node := range nodes {
		tc.updateNodeLabel(ctx, t, node.Name, tc.nodeStatusLabelMetaRepos, tc.nodeStatusStopped)
	}

	assert.Eventually(t, func() bool {
		pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorMetaReposClear, true)
		assert.NoError(t, err)
		return len(pods) == 0
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

// StartStorageNodes will result in the given number of running storage nodes
// on the node group varlogsn of the cluster. If there are already nodes in the
// cluster more than the given number, this method will fail.
//
// Note that the DaemonSet varlogsn should have a proper nodeAffinity setting
// since it sets the node label `varlogsn-status` of the node group varlogsn to
// `started`.
func (tc *TestCluster) StartStorageNodes(ctx context.Context, t *testing.T, desired int32) {
	t.Helper()

	nodes := tc.ListStorageNodeNodes(ctx, t)
	started := make([]string, 0, len(nodes))
	free := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if status := node.Labels[tc.nodeStatusLabelStorageNode]; status == tc.nodeStatusStarted {
			started = append(started, node.Name)
			continue
		}
		free = append(free, node.Name)
	}

	need := int(desired) - len(started)
	assert.GreaterOrEqual(t, need, 0)
	assert.GreaterOrEqual(t, len(free), need)

	tc.logger.Info("starting storage nodes",
		zap.Int32("desired", desired),
		zap.Strings("startedNodes", started),
		zap.Strings("freeNodes", free),
		zap.Int("need", need),
	)

	for i := 0; i < need; i++ {
		nodeName := free[i]
		tc.updateNodeLabel(ctx, t, nodeName, tc.nodeStatusLabelStorageNode, tc.nodeStatusStarted)
	}

	assert.Eventually(t, func() bool {
		pods := tc.ListStorageNodePods(ctx, t)
		return len(pods) == int(desired)
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

// StopStorageNodes stops all storage nodes.
// It changes the node label DefaultNodeStatusLabelStorageNode from DefaultNodeStatusStarted to DefaultNodeStatusStopped.
func (tc *TestCluster) StopStorageNodes(ctx context.Context, t *testing.T) {
	t.Helper()

	tc.logger.Info("stopping all storage nodes")

	nodes := tc.ListStorageNodeNodes(ctx, t)
	for _, node := range nodes {
		tc.updateNodeLabel(ctx, t, node.Name, tc.nodeStatusLabelStorageNode, tc.nodeStatusStopped)
	}

	assert.Eventually(t, func() bool {
		pods := tc.ListStorageNodePods(ctx, t)
		return len(pods) == 0
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

func (tc *TestCluster) StartStorageNode(ctx context.Context, t *testing.T, nodeName string) bool {
	t.Helper()

	tc.logger.Info("starting a storage node", zap.String("nodeName", nodeName))

	tc.updateNodeLabel(ctx, t, nodeName, tc.nodeStatusLabelStorageNode, tc.nodeStatusStarted)

	return assert.Eventually(t, func() bool {
		pods := tc.ListStorageNodePods(ctx, t)
		for _, pod := range pods {
			if pod.Spec.NodeName == nodeName {
				return tc.c.PodReady(pod)
			}
		}
		return false
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

// StopStorageNode stops the storage node specified by the argument snid and
// returns the Kubernetes node name that had run the storage node.
func (tc *TestCluster) StopStorageNode(ctx context.Context, t *testing.T, snid types.StorageNodeID) (nodeName string) {
	t.Helper()

	oldPods := tc.ListStorageNodePods(ctx, t)
	nodeName = tc.GetStorageNodeName(ctx, t, snid)
	assert.Condition(t, func() bool {
		for _, pod := range oldPods {
			if pod.Spec.NodeName == nodeName {
				return true
			}
		}
		return false
	})

	tc.logger.Info("stopping a storage node",
		zap.Int32("snid", int32(snid)),
		zap.String("nodeName", nodeName),
	)

	tc.updateNodeLabel(ctx, t, nodeName, tc.nodeStatusLabelStorageNode, tc.nodeStatusStopped)

	assert.Eventually(t, func() bool {
		newPods := tc.ListStorageNodePods(ctx, t)
		return len(newPods) == len(oldPods)-1
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)

	return nodeName
}

func (tc *TestCluster) GetStorageNodeName(ctx context.Context, t *testing.T, snid types.StorageNodeID) string {
	t.Helper()

	adminAddr := tc.AdminServerAddress(ctx, t)
	adm, err := varlog.NewAdmin(ctx, adminAddr)
	assert.NoError(t, err)
	defer func() {
		err := adm.Close()
		assert.NoError(t, err)
	}()

	snm, err := adm.GetStorageNode(ctx, snid)
	assert.NoError(t, err)

	pods := tc.ListStorageNodePods(ctx, t)
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
func (tc *TestCluster) ClearStorageNodeData(ctx context.Context, t *testing.T) {
	t.Helper()

	tc.logger.Info("wiping out data from storage nodes")

	nodes := tc.ListStorageNodeNodes(ctx, t)

	for _, node := range nodes {
		tc.updateNodeLabel(ctx, t, node.Name, tc.nodeStatusLabelStorageNode, tc.nodeStatusCleared)
	}

	assert.Eventually(t, func() bool {
		pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorStorageNodeClear, true)
		assert.NoError(t, err)
		return len(pods) == len(nodes)
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)

	for _, node := range nodes {
		tc.updateNodeLabel(ctx, t, node.Name, tc.nodeStatusLabelStorageNode, tc.nodeStatusStopped)
	}

	assert.Eventually(t, func() bool {
		pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorStorageNodeClear, true)
		assert.NoError(t, err)
		return len(pods) == 0
	}, tc.asyncTestWaitDuration, tc.asyncTestCheckInterval)
}

// ListStorageNodePods returns a list of all pods running varlogsn.
func (tc *TestCluster) ListStorageNodePods(ctx context.Context, t *testing.T) []core.Pod {
	t.Helper()
	pods, err := tc.c.Pods(ctx, tc.varlogNamespace, tc.appSelectorStorageNode, true)
	assert.NoError(t, err)
	return pods
}

// ListStorageNodeNodes returns a list of Kubernetes nodes labeled the node
// group varlogsn. Note that there can be no running pods in the result nodes.
func (tc *TestCluster) ListStorageNodeNodes(ctx context.Context, t *testing.T) []core.Node {
	t.Helper()
	nodes, err := tc.c.Nodes(ctx, map[string]string{
		tc.nodeGroupLabel: tc.nodeGroupStorageNode,
	})
	assert.NoError(t, err)
	return nodes
}

func (tc *TestCluster) updateNodeLabel(ctx context.Context, t *testing.T, nodeName, label, value string) {
	t.Helper()
	tc.logger.Debug("updating node label",
		zap.String("nodeName", nodeName),
		zap.String(label, value),
	)
	err := tc.c.AddNodeLabel(ctx, nodeName, label, value)
	assert.NoError(t, err)
}
