package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	vtypes "github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"

	. "github.com/smartystreets/goconvey/convey"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	MR_LABEL      = "varlog-mr"
	MR_STOP_LABEL = "varlog-mr-stop"
	MR_DROP_LABEL = "varlog-mr-drop"
	SN_LABEL      = "varlog-sn"
	SN_STOP_LABEL = "varlog-sn-stop"
	SN_DROP_LABEL = "varlog-sn-drop"
	VMS_RS_NAME   = "varlog-vms"
	VMS_VIP_NAME  = "varlog-vms-vip-service"
	MR_VIP_NAME   = "varlog-mr-vip-service"

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

	c, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	k8s := &K8sVarlogCluster{K8sVarlogClusterOptions: opts, cli: c}
	k8s.view = newK8sVarlogView(k8s)

	return k8s, nil
}

func (k8s *K8sVarlogCluster) Nodes(selector map[string]string) (*v1.NodeList, error) {
	labelSelector := labels.Set(selector)
	return k8s.cli.
		CoreV1().
		Nodes().
		List(context.TODO(), metav1.ListOptions{LabelSelector: labelSelector.String()})
}

func (k8s *K8sVarlogCluster) Pods(namespace string, selector map[string]string) (*v1.PodList, error) {
	labelSelector := labels.Set(selector)
	return k8s.cli.
		CoreV1().
		Pods(namespace).
		List(context.TODO(), metav1.ListOptions{LabelSelector: labelSelector.String()})
}

func (k8s *K8sVarlogCluster) WorkerNodes() (*v1.NodeList, error) {
	return k8s.Nodes(map[string]string{"node-role.kubernetes.io/worker": "true"})
}

func (k8s *K8sVarlogCluster) Reset() error {
	if err := k8s.StopAll(); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(100, func() (bool, error) {
		n, err := k8s.numPodsReady("default", nil)
		if err != nil {
			return false, err
		}
		// NOTE: 1 is jaeger
		return n == 1, nil
	}); err != nil {
		return err
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
	s, err := k8s.cli.
		CoreV1().
		Services("ingress-nginx").
		Get(context.TODO(), VMS_VIP_NAME, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", s.Status.LoadBalancer.Ingress[0].IP, s.Spec.Ports[0].Port), nil
}

func (k8s *K8sVarlogCluster) MRAddress() (string, error) {
	s, err := k8s.cli.
		CoreV1().
		Services("ingress-nginx").
		Get(context.TODO(), MR_VIP_NAME, metav1.GetOptions{})
	if err != nil {
		return "", err
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

	_, err := k8s.cli.
		CoreV1().
		Nodes().
		Patch(context.TODO(), node, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return err
}

func (k8s *K8sVarlogCluster) AddLabel(node, label, value string) error {
	payload := []patchStringValue{{
		Op:    "add",
		Path:  "/metadata/labels/" + label,
		Value: value,
	}}
	payloadBytes, _ := json.Marshal(payload)

	_, err := k8s.cli.
		CoreV1().
		Nodes().
		Patch(context.TODO(), node, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return err
}

func (k8s *K8sVarlogCluster) RemoveLabel(node, label string) error {
	payload := []patchStringValue{{
		Op:   "remove",
		Path: "/metadata/labels/" + label,
	}}
	payloadBytes, _ := json.Marshal(payload)

	_, err := k8s.cli.
		CoreV1().
		Nodes().
		Patch(context.TODO(), node, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return err
}

func (k8s *K8sVarlogCluster) ScaleReplicaSet(replicasetName string, scale int32) error {
	s, err := k8s.cli.
		AppsV1().
		Deployments("default").
		GetScale(context.TODO(), replicasetName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	sc := *s
	if sc.Spec.Replicas == scale {
		return nil
	}

	sc.Spec.Replicas = scale

	_, err = k8s.cli.
		AppsV1().
		Deployments("default").
		UpdateScale(context.TODO(),
			replicasetName, &sc, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (k8s *K8sVarlogCluster) ReplaceEnvToDeployment(deployment, name, value string) error {
	d, err := k8s.cli.
		AppsV1().
		Deployments("default").
		Get(context.TODO(), deployment, metav1.GetOptions{})
	if err != nil {
		return err
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

	_, err = k8s.cli.
		AppsV1().
		Deployments("default").
		Update(context.TODO(), d, metav1.UpdateOptions{})

	return err
}

func (k8s *K8sVarlogCluster) ReplaceEnvToDaemonset(daemonset, name, value string) error {
	d, err := k8s.cli.
		AppsV1().
		DaemonSets("default").
		Get(context.TODO(), daemonset, metav1.GetOptions{})
	if err != nil {
		return err
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

	_, err = k8s.cli.
		AppsV1().
		DaemonSets("default").
		Update(context.TODO(), d, metav1.UpdateOptions{})

	return err
}

func (k8s *K8sVarlogCluster) StopAll() error {
	if err := k8s.StopVMS(); err != nil {
		return err
	}

	return k8s.RemoveLabelAll()
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

func (k8s *K8sVarlogCluster) RemoveLabelAll() error {
	nodes, err := k8s.WorkerNodes()
	if err != nil {
		return err
	}

	for _, node := range nodes.Items {
		if _, ok := node.Labels["type"]; ok {
			erri := k8s.RemoveLabel(node.GetName(), "type")
			if erri != nil {
				err = erri
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
		if err := testutil.CompareWaitErrorN(200, func() (bool, error) {
			p, err := k8s.numPodsReady("default", podSelector)
			return p == num, err
		}); err != nil {
			return err
		}

		if num == expected {
			break
		}
	}

	if num < expected {
		return errors.New("not enough nodes")
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
			So(err, ShouldBeNil)
		}

		Reset(func() {
			//k8s.StopAll()
			testutil.GC()
		})

		f(k8s)
	}
}

func newK8sVarlogView(podGetter K8sVarlogPodGetter) *k8sVarlogView {
	return &k8sVarlogView{
		podGetter: podGetter,
		idGetter:  &varlogIDGetter{},
		mrs:       make(map[vtypes.NodeID]string),
		sns:       make(map[vtypes.StorageNodeID]string),
	}
}

func (view *k8sVarlogView) Renew() error {
	pods, err := view.podGetter.Pods("default", map[string]string{"app": MR_LABEL})
	if err != nil {
		return err
	}

	clusterID := vtypes.ClusterID(0)
	mrs := make(map[vtypes.NodeID]string)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s:%d",
			pod.Status.HostIP,
			pod.Spec.Containers[0].ReadinessProbe.Handler.TCPSocket.Port.IntValue())

		cid, mrid, err := view.idGetter.MetadataRepositoryID(addr)
		if err != nil {
			return err
		}
		clusterID = cid

		mrs[mrid] = pod.Spec.NodeName
	}

	pods, err = view.podGetter.Pods("default", map[string]string{"app": SN_LABEL})
	if err != nil {
		return err
	}

	sns := make(map[vtypes.StorageNodeID]string)
	for _, pod := range pods.Items {
		addr := fmt.Sprintf("%s:%d",
			pod.Status.HostIP,
			pod.Spec.Containers[0].ReadinessProbe.Handler.TCPSocket.Port.IntValue())

		snid, err := view.idGetter.StorageNodeID(addr, clusterID)
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
