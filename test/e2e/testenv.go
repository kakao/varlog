package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"

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
)

type K8sVarlogClusterOptions struct {
	MasterUrl string
	User      string
	Token     string
	Cluster   string
	Context   string
	NrRep     int
	NrMR      int
	NrSN      int
}

type K8sVarlogCluster struct {
	K8sVarlogClusterOptions
	cli *kubernetes.Clientset
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

func optsToConfigBytes(opts K8sVarlogClusterOptions) []byte {
	return []byte(fmt.Sprintf("apiVersion: v1\n"+
		"clusters:\n"+
		"- cluster:\n"+
		"    insecure-skip-tls-verify: true\n"+
		"    server: %s\n"+
		"  name: %s\n"+
		"contexts:\n"+
		"- context:\n"+
		"    cluster: %s\n"+
		"    user: %s\n"+
		"  name: %s\n"+
		"current-context: %s\n"+
		"kind: Config\n"+
		"preferences: {}\n"+
		"users:\n"+
		"- name: %s\n"+
		"  user:\n"+
		"    token: %s",
		opts.MasterUrl,
		opts.Cluster,
		opts.Cluster,
		opts.User,
		opts.Context,
		opts.Context,
		opts.User,
		opts.Token))
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

	return &K8sVarlogCluster{K8sVarlogClusterOptions: opts, cli: c}, nil
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

		return n == 0, nil
	}); err != nil {
		return err
	}

	if err := k8s.StartVMS(); err != nil {
		return err
	}

	if err := k8s.StartMRs(); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(100, k8s.IsMRRunning); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(100, k8s.IsVMSRunning); err != nil {
		return err
	}

	if err := k8s.StartSNs(); err != nil {
		return err
	}

	if err := testutil.CompareWaitErrorN(100, k8s.IsSNRunning); err != nil {
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
		if err := testutil.CompareWaitErrorN(100, func() (bool, error) {
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
