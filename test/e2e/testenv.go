package e2e

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (k8s *K8sVarlogCluster) Restart() error {
	if err := k8s.StopAll(); err != nil {
		return err
	}

	if err := k8s.StartMRs(); err != nil {
		return err
	}

	if err := k8s.StartVMS(); err != nil {
		return err
	}

	//TODO:: check vms alive

	return k8s.StartSNs()
}

func (k8s *K8sVarlogCluster) StopAll() error {
	if err := k8s.StopVMS(); err != nil {
		return err
	}

	if err := k8s.StopMRs(); err != nil {
		return err
	}

	return k8s.StopSNs()
}

func (k8s *K8sVarlogCluster) ReplaceLabelAll(selector, label, replaceLabel string) error {
	nodes, err := k8s.cli.
		CoreV1().
		Nodes().
		List(context.TODO(), metav1.ListOptions{LabelSelector: "type=" + selector})
	if err != nil {
		return err
	}

	for _, node := range nodes.Items {
		erri := k8s.ReplaceLabel(node.GetName(), label, replaceLabel)
		if erri != nil {
			err = erri
		}
	}

	return err
}

func (k8s *K8sVarlogCluster) StopMRs() error {
	return k8s.ReplaceLabelAll(MR_LABEL, "type", MR_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopDropMRs() error {
	return k8s.ReplaceLabelAll(MR_DROP_LABEL, "type", MR_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopSNs() error {
	return k8s.ReplaceLabelAll(SN_LABEL, "type", SN_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopDropSNs() error {
	return k8s.ReplaceLabelAll(SN_DROP_LABEL, "type", SN_STOP_LABEL)
}

func (k8s *K8sVarlogCluster) StopVMS() error {
	return k8s.ScaleReplicaSet(VMS_RS_NAME, 0)
}

func (k8s *K8sVarlogCluster) StartMRs() error {
	return k8s.ReplaceLabelAll(MR_STOP_LABEL, "type", MR_LABEL)
}

func (k8s *K8sVarlogCluster) StartSNs() error {
	return k8s.ReplaceLabelAll(SN_STOP_LABEL, "type", SN_LABEL)
}

func (k8s *K8sVarlogCluster) StartVMS() error {
	return k8s.ScaleReplicaSet(VMS_RS_NAME, 1)
}

func (k8s *K8sVarlogCluster) GetNodes(selector string) ([]string, error) {
	nodes, err := k8s.cli.
		CoreV1().
		Nodes().
		List(context.TODO(), metav1.ListOptions{LabelSelector: "type=" + selector})
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
	return k8s.GetNodes(MR_LABEL)
}

func (k8s *K8sVarlogCluster) GetSNs() ([]string, error) {
	return k8s.GetNodes(SN_LABEL)
}

func (k8s *K8sVarlogCluster) IsPodsRunning(label string) (bool, error) {
	pods, err := k8s.cli.
		CoreV1().
		Pods("default").
		List(context.TODO(), metav1.ListOptions{LabelSelector: "app=" + label})

	if err != nil {
		return false, err
	}

	if len(pods.Items) == 0 {
		return false, nil
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" {
			return false, nil
		}
	}

	return true, nil
}

func (k8s *K8sVarlogCluster) IsVMSRunning() (bool, error) {
	return k8s.IsPodsRunning(VMS_RS_NAME)
}

func (k8s *K8sVarlogCluster) IsMRRunning() (bool, error) {
	return k8s.IsPodsRunning(MR_LABEL)
}

func (k8s *K8sVarlogCluster) IsSNRunning() (bool, error) {
	return k8s.IsPodsRunning(SN_LABEL)
}
