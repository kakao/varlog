package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/multierr"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	vtypes "github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
)

const (
	MR_LABEL      = "varlog-mr"
	MR_STOP_LABEL = "varlog-mr-stop"
	MR_DROP_LABEL = "varlog-mr-drop"
	SN_LABEL      = "varlog-sn"
	SN_STOP_LABEL = "varlog-sn-stop"
	SN_DROP_LABEL = "varlog-sn-drop"
	VMS_RS_NAME   = "varlog-vms"
	VMS_VIP_NAME  = "varlog-vms-rpc-vip"
	MR_VIP_NAME   = "varlog-mr-rpc-vip"
	MRServiceName = "varlog-mr-rpc"

	ENV_REP_FACTOR = "REP_FACTOR"
)

type K8sVarlogPodGetter interface {
	Pods(namespace string, selector map[string]string) (*v1.PodList, error)
}

type K8sVarlogCluster struct {
	K8sVarlogClusterOptions
	cli  *kubernetes.Clientset
	view K8sVarlogView
}

// node ID to k8s node name view
type K8sVarlogView interface {
	Renew() error
	GetMRNodeName(vtypes.NodeID) (string, error)
	GetSNNodeName(vtypes.StorageNodeID) (string, error)
}

type k8sVarlogView struct {
	podGetter K8sVarlogPodGetter
	idGetter  VarlogIDGetter
	mrs       map[vtypes.NodeID]string
	sns       map[vtypes.StorageNodeID]string
	mu        sync.RWMutex
	timeout   time.Duration
}

type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

type patchUInt32Value struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value uint32 `json:"value"`
}

func NewK8sVarlogCluster(opts K8sVarlogClusterOptions) (*K8sVarlogCluster, error) {
	config, err := clientcmd.RESTConfigFromKubeConfig(optsToConfigBytes(opts))
	if err != nil {
		return nil, err
	}

	config.Burst = 100
	config.QPS = 20

	c, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	k8s := &K8sVarlogCluster{K8sVarlogClusterOptions: opts, cli: c}
	k8s.view = newK8sVarlogView(k8s, k8s.timeout)

	return k8s, nil
}

func (k8s *K8sVarlogCluster) TimeoutContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.TODO(), k8s.timeout)
	return ctx, cancel
}

func (k8s *K8sVarlogCluster) WithTimeoutContext(f func(context.Context)) {
	ctx, cancel := k8s.TimeoutContext()
	defer cancel()
	f(ctx)
}

func (k8s *K8sVarlogCluster) Nodes(selector map[string]string) (*v1.NodeList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	labelSelector := labels.Set(selector)
	list, err := k8s.cli.
		CoreV1().
		Nodes().
		List(ctx, metav1.ListOptions{LabelSelector: labelSelector.String()})
	return list, errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) Pods(namespace string, selector map[string]string) (*v1.PodList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	labelSelector := labels.Set(selector)
	list, err := k8s.cli.
		CoreV1().
		Pods(namespace).
		List(ctx, metav1.ListOptions{LabelSelector: labelSelector.String()})
	return list, errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) WorkerNodes() (*v1.NodeList, error) {
	return k8s.Nodes(map[string]string{"node-role.kubernetes.io/worker": "true"})
}

func (k8s *K8sVarlogCluster) Reset() error {
	if err := k8s.StopAll(); err != nil {
		return err
	}

	var numPods int
	if err := testutil.CompareWaitErrorN(500, func() (bool, error) {
		numPods, err := k8s.numPodsReady("default", nil)
		if err != nil {
			return false, errors.Wrap(err, "k8s")
		}
		return numPods == 0, nil
	}); err != nil {
		return errors.Wrapf(err, "k8s: numPodsReady=%d", numPods)
	}

	rep := fmt.Sprintf("%d", k8s.RepFactor)
	if err := k8s.ReplaceEnvToDeployment(VMS_RS_NAME, ENV_REP_FACTOR, rep); err != nil {
		return err
	}

	if err := k8s.ReplaceEnvToDaemonset(MR_LABEL, ENV_REP_FACTOR, rep); err != nil {
		return err
	}

	if k8s.NrMR > 0 {
		if err := k8s.startNodes(MR_LABEL, 1); err != nil {
			return err
		}
	}

	if err := k8s.StartVMS(); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(100, k8s.IsVMSRunning); err != nil {
		return err
	}

	if err := k8s.StartMRs(); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(200, k8s.IsMRRunning); err != nil {
		return err
	}

	if err := k8s.StartSNs(); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(200, k8s.IsSNRunning); err != nil {
		return err
	}

	return nil
}

func (k8s *K8sVarlogCluster) VMSAddress() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	s, err := k8s.cli.
		CoreV1().
		Services("ingress-nginx").
		Get(ctx, VMS_VIP_NAME, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrap(err, "k8s")
	}

	return fmt.Sprintf("%s:%d", s.Status.LoadBalancer.Ingress[0].IP, s.Spec.Ports[0].Port), nil
}

func (k8s *K8sVarlogCluster) MRAddress() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	s, err := k8s.cli.
		CoreV1().
		Services("ingress-nginx").
		Get(ctx, MR_VIP_NAME, metav1.GetOptions{})
	if err != nil {
		return "", errors.Wrap(err, "k8s")
	}

	return fmt.Sprintf("%s:%d", s.Status.LoadBalancer.Ingress[0].IP, s.Spec.Ports[0].Port), nil
}

func (k8s *K8sVarlogCluster) ReplaceLabel(node, label, value string) error {
	payload := []patchStringValue{{
		Op:    "replace",
		Path:  "/metadata/labels/" + label,
		Value: value,
	}}
	payloadBytes, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	_, err := k8s.cli.
		CoreV1().
		Nodes().
		Patch(ctx, node, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) AddLabel(node, label, value string) error {
	payload := []patchStringValue{{
		Op:    "add",
		Path:  "/metadata/labels/" + label,
		Value: value,
	}}
	payloadBytes, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	_, err := k8s.cli.
		CoreV1().
		Nodes().
		Patch(ctx, node, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) RemoveLabel(node, label string) error {
	payload := []patchStringValue{{
		Op:   "remove",
		Path: "/metadata/labels/" + label,
	}}
	payloadBytes, _ := json.Marshal(payload)

	ctx, cancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer cancel()
	_, err := k8s.cli.
		CoreV1().
		Nodes().
		Patch(ctx, node, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) ScaleReplicaSet(replicasetName string, scale int32) error {
	gCtx, gCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer gCancel()
	s, err := k8s.cli.
		AppsV1().
		Deployments("default").
		GetScale(gCtx, replicasetName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	sc := *s
	if sc.Spec.Replicas == scale {
		return nil
	}

	sc.Spec.Replicas = scale

	uCtx, uCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer uCancel()
	_, err = k8s.cli.
		AppsV1().
		Deployments("default").
		UpdateScale(uCtx, replicasetName, &sc, metav1.UpdateOptions{})
	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) ReplaceEnvToDeployment(deployment, name, value string) error {
	gCtx, gCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer gCancel()
	d, err := k8s.cli.
		AppsV1().
		Deployments("default").
		Get(gCtx, deployment, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	exist := false
	for _, env := range d.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name {
			env.Value = value
			exist = true
			break
		}
	}

	if !exist {
		env := v1.EnvVar{
			Name:  name,
			Value: value,
		}

		d.Spec.Template.Spec.Containers[0].Env =
			append(d.Spec.Template.Spec.Containers[0].Env, env)
	}

	uCtx, uCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer uCancel()
	_, err = k8s.cli.
		AppsV1().
		Deployments("default").
		Update(uCtx, d, metav1.UpdateOptions{})

	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) ReplaceEnvToDaemonset(daemonset, name, value string) error {
	gCtx, gCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer gCancel()
	d, err := k8s.cli.
		AppsV1().
		DaemonSets("default").
		Get(gCtx, daemonset, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	exist := false
	for _, env := range d.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name {
			env.Value = value
			exist = true
			break
		}
	}

	if !exist {
		env := v1.EnvVar{
			Name:  name,
			Value: value,
		}

		d.Spec.Template.Spec.Containers[0].Env =
			append(d.Spec.Template.Spec.Containers[0].Env, env)
	}

	uCtx, uCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer uCancel()
	_, err = k8s.cli.
		AppsV1().
		DaemonSets("default").
		Update(uCtx, d, metav1.UpdateOptions{})

	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) StopAll() error {
	if err := k8s.StopVMS(); err != nil {
		return err
	}

	if err := k8s.RemoveLabelAll(); err != nil {
		return err
	}

	return testutil.CompareWaitErrorN(300, func() (bool, error) {
		numPods, err := k8s.numPodsReady("default", nil)
		if err != nil {
			return false, err
		}
		return numPods == 0, nil
	})
}

func (k8s *K8sVarlogCluster) ReplaceLabelAll(label, o, n string) error {
	nodes, err := k8s.Nodes(map[string]string{label: o})
	if err != nil {
		return err
	}

	for _, node := range nodes.Items {
		erri := k8s.ReplaceLabel(node.GetName(), label, n)
		if erri != nil {
			err = erri
		}
	}

	return err
}

func (k8s *K8sVarlogCluster) RemoveLabelAll() (err error) {
	nodes, err := k8s.WorkerNodes()
	if err != nil {
		return err
	}

	for _, node := range nodes.Items {
		if _, ok := node.Labels["type"]; ok {
			if erri := k8s.RemoveLabel(node.GetName(), "type"); erri != nil {
				err = multierr.Append(err, erri)
			}
		}
	}

	return err
}

func (k8s *K8sVarlogCluster) StopMRs() error {
	return k8s.ReplaceLabelAll("type", MR_LABEL, MR_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopDropMRs() error {
	return k8s.ReplaceLabelAll("type", MR_DROP_LABEL, MR_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopSNs() error {
	return k8s.ReplaceLabelAll("type", SN_LABEL, SN_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopDropSNs() error {
	return k8s.ReplaceLabelAll("type", SN_DROP_LABEL, SN_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopVMS() error {
	return k8s.ScaleReplicaSet(VMS_RS_NAME, 0)
}

func (k8s *K8sVarlogCluster) StopMR(mrID vtypes.NodeID) error {
	nodeName, err := k8s.view.GetMRNodeName(mrID)
	if err != nil {
		return err
	}

	return k8s.ReplaceLabel(nodeName, "type", MR_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopSN(snID vtypes.StorageNodeID) error {
	nodeName, err := k8s.view.GetSNNodeName(snID)
	if err != nil {
		return err
	}

	return k8s.ReplaceLabel(nodeName, "type", SN_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) RecoverMR() error {
	return k8s.ReplaceLabelAll("type", MR_STOP_LABEL, MR_LABEL)
}

func (k8s *K8sVarlogCluster) RecoverSN() error {
	return k8s.ReplaceLabelAll("type", SN_STOP_LABEL, SN_LABEL)
}

func (k8s *K8sVarlogCluster) startNodes(label string, expected int) error {
	nodeSelector := map[string]string{"type": label}
	podSelector := map[string]string{"app": label}
	nodes, err := k8s.Nodes(nodeSelector)
	if err != nil {
		return err
	}

	num := len(nodes.Items)
	if num > expected {
		return errors.New("too many nodes")
	}

	if num == expected {
		return nil
	}

	nodes, err = k8s.WorkerNodes()
	if err != nil {
		return err
	}

	for _, node := range nodes.Items {
		if _, ok := node.Labels["type"]; ok {
			continue
		}

		err := k8s.AddLabel(node.GetName(), "type", label)
		if err != nil {
			return err
		}

		num++
		numPods := 0
		if err := testutil.CompareWaitErrorWithRetryIntervalN(1000, 10*time.Second, func() (bool, error) {
			numPods, err := k8s.numPodsReady("default", podSelector)
			return numPods == num, errors.Wrap(err, "k8s")
		}); err != nil {
			return errors.Wrapf(err, "k8s: node=%s, numPodsReady=%d", node.GetName(), numPods)
		}

		if num == expected {
			break
		}
	}

	if num != expected {
		return errors.New("too many or not enough nodes")
	}

	return nil
}

func (k8s *K8sVarlogCluster) StartMRs() error {
	return k8s.startNodes(MR_LABEL, k8s.NrMR)
}

func (k8s *K8sVarlogCluster) StartSNs() error {
	return k8s.startNodes(SN_LABEL, k8s.NrSN)
}

func (k8s *K8sVarlogCluster) StartVMS() error {
	return k8s.ScaleReplicaSet(VMS_RS_NAME, 1)
}

func (k8s *K8sVarlogCluster) NodeNames(labels map[string]string) ([]string, error) {
	nodes, err := k8s.Nodes(labels)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(nodes.Items))
	for _, node := range nodes.Items {
		names = append(names, node.GetName())
	}

	return names, nil
}

func (k8s *K8sVarlogCluster) GetMRs() ([]string, error) {
	return k8s.NodeNames(map[string]string{"type": MR_LABEL})
}

func (k8s *K8sVarlogCluster) GetSNs() ([]string, error) {
	return k8s.NodeNames(map[string]string{"type": SN_LABEL})
}

func (k8s *K8sVarlogCluster) numPodsReady(namespace string, labels map[string]string) (int, error) {
	pods, err := k8s.Pods(namespace, labels)
	if err != nil {
		return -1, err
	}

	n := 0

	for _, pod := range pods.Items {
		if pod.Name[:6] != "varlog" {
			continue
		}
		if pod.Status.Phase == "Running" {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == "Ready" && condition.Status == "True" {
					n++
				}
			}
		}
	}

	return n, nil
}

func (k8s *K8sVarlogCluster) IsVMSRunning() (bool, error) {
	n, err := k8s.numPodsReady("default", map[string]string{"app": VMS_RS_NAME})
	return n == 1, err
}

func (k8s *K8sVarlogCluster) IsMRRunning() (bool, error) {
	n, err := k8s.numPodsReady("default", map[string]string{"app": MR_LABEL})
	return n == k8s.NrMR, err
}

func (k8s *K8sVarlogCluster) IsSNRunning() (bool, error) {
	n, err := k8s.numPodsReady("default", map[string]string{"app": SN_LABEL})
	return n == k8s.NrSN, err
}

func (k8s *K8sVarlogCluster) NumMRRunning() (int, error) {
	return k8s.numPodsReady("default", map[string]string{"app": MR_LABEL})
}

func (k8s *K8sVarlogCluster) NumSNRunning() (int, error) {
	return k8s.numPodsReady("default", map[string]string{"app": SN_LABEL})
}

func withTestCluster(opts K8sVarlogClusterOptions, f func(k8s *K8sVarlogCluster)) func() {
	return func() {
		k8s, err := NewK8sVarlogCluster(opts)
		So(err, ShouldBeNil)

		if opts.Reset {
			err = k8s.Reset()
			if err != nil {
				fmt.Printf("reset failed: %+v\n", err)
			}
			So(err, ShouldBeNil)
		}

		Reset(func() {
			//k8s.StopAll()
			testutil.GC()
		})

		f(k8s)
	}
}

func newK8sVarlogView(podGetter K8sVarlogPodGetter, timeout time.Duration) *k8sVarlogView {
	return &k8sVarlogView{
		podGetter: podGetter,
		idGetter:  &varlogIDGetter{},
		mrs:       make(map[vtypes.NodeID]string),
		sns:       make(map[vtypes.StorageNodeID]string),
		timeout:   timeout,
	}
}

func (view *k8sVarlogView) Renew() error {
	pods, err := view.podGetter.Pods("default", map[string]string{"app": MR_LABEL})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	clusterID := vtypes.ClusterID(0)
	mrs := make(map[vtypes.NodeID]string)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s:%d",
			pod.Status.HostIP,
			pod.Spec.Containers[0].Ports[0].ContainerPort)

		ctx, cancel := context.WithTimeout(context.Background(), view.timeout)
		cid, mrid, err := view.idGetter.MetadataRepositoryID(ctx, addr)
		cancel()
		if err != nil {
			return err
		}
		clusterID = cid

		mrs[mrid] = pod.Spec.NodeName
	}

	pods, err = view.podGetter.Pods("default", map[string]string{"app": SN_LABEL})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	sns := make(map[vtypes.StorageNodeID]string)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s:%d",
			pod.Status.HostIP,
			pod.Spec.Containers[0].Ports[0].ContainerPort)

		ctx, cancel := context.WithTimeout(context.Background(), view.timeout)
		snid, err := view.idGetter.StorageNodeID(ctx, addr, clusterID)
		cancel()
		if err != nil {
			return err
		}

		sns[snid] = pod.Spec.NodeName
	}

	view.mrs = mrs
	view.sns = sns

	return nil
}

func (view *k8sVarlogView) GetMRNodeName(mrID vtypes.NodeID) (string, error) {
	view.mu.Lock()
	defer view.mu.Unlock()

	nodeID, ok := view.mrs[mrID]
	if !ok {
		if err := view.Renew(); err != nil {
			return "", err
		}
		nodeID, _ = view.mrs[mrID]
	}

	return nodeID, nil
}

func (view *k8sVarlogView) GetSNNodeName(snID vtypes.StorageNodeID) (string, error) {
	view.mu.Lock()
	defer view.mu.Unlock()

	nodeID, ok := view.sns[snID]
	if !ok {
		if err := view.Renew(); err != nil {
			return "", err
		}
		nodeID, _ = view.sns[snID]
	}

	return nodeID, nil
}
