package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// Client is a Kubernetes client that provides simpler interfaces than the
// official Kubernetes package.
type Client struct {
	config
	*kubernetes.Clientset
}

// NewClient returns a new client.
func NewClient(opts ...Option) (*Client, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	cluster := &Client{
		config: cfg,
	}
	if err := cluster.initClient(); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (c *Client) initClient() error {
	configBytes := []byte(fmt.Sprintf(
		"apiVersion: v1\n"+
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
		c.masterURL,
		c.cluster,
		c.cluster,
		c.user,
		c.context,
		c.context,
		c.user,
		c.token))

	config, err := clientcmd.RESTConfigFromKubeConfig(configBytes)
	if err != nil {
		return errors.WithMessage(err, "k8s: rest config")
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return errors.WithMessage(err, "k8s: creating client")
	}

	c.Clientset = client
	return nil
}

// Nodes returns a list of Kubernetes nodes labeled by the argument
// labelSelector.
func (c *Client) Nodes(ctx context.Context, labelSelector map[string]string) ([]core.Node, error) {
	nodes, err := c.CoreV1().Nodes().List(
		ctx,
		meta.ListOptions{LabelSelector: labels.Set(labelSelector).String()},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "k8s: list nodes")
	}
	return nodes.Items, nil
}

// Pods returns a list of Kubernetes pods labeled by the argument
// labelSelector. It returns only ready pods if the argument onlyReady is true.
func (c *Client) Pods(ctx context.Context, namespace string, labelSelector map[string]string, onlyReady bool) ([]core.Pod, error) {
	pods, err := c.CoreV1().Pods(namespace).List(
		ctx,
		meta.ListOptions{LabelSelector: labels.Set(labelSelector).String()},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "k8s: list pods")
	}

	ret := pods.Items
	if onlyReady {
		ret = c.FilterReadyPods(ret)
	}
	return ret, nil
}

// FilterReadyPods filters out not-ready pods.
func (c *Client) FilterReadyPods(pods []core.Pod) []core.Pod {
	readyPods := make([]core.Pod, 0, len(pods))
	for idx := range pods {
		if c.PodReady(pods[idx]) {
			readyPods = append(readyPods, pods[idx])
		}
	}
	return readyPods
}

// PodReady decides whether the pod is ready.
func (c *Client) PodReady(pod core.Pod) bool {
	if pod.Status.Phase != core.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		// NB: No need to check other condition types?
		if condition.Type != core.PodReady {
			continue
		}
		return condition.Status == core.ConditionTrue
	}
	return false
}

// Service returns a service resource named by the argument name defined in the
// given namespace.
func (c *Client) Service(ctx context.Context, namespace, name string) (*core.Service, error) {
	service, err := c.CoreV1().Services(namespace).Get(ctx, name, meta.GetOptions{})
	return service, errors.WithMessage(err, "k8s: get service")
}

// UpdateDeploymentReplicas updates replicas of the deployment in the
// namespace.
// The replicas are changed asynchronously, thus caller has a responsibility to
// check the done of change.
func (c *Client) UpdateDeploymentReplicas(ctx context.Context, namespace, deploymentName string, replicas int32) error {
	deployment := c.AppsV1().Deployments(namespace)
	scale, err := deployment.GetScale(ctx, deploymentName, meta.GetOptions{})
	if err != nil {
		return errors.WithMessage(err, "k8s: get deployment scale")
	}

	if scale.Spec.Replicas == replicas {
		return nil
	}

	scale.Spec.Replicas = replicas
	_, err = deployment.UpdateScale(ctx, deploymentName, scale, meta.UpdateOptions{})
	return errors.WithMessage(err, "k8s: update deployment scale")
}

// UpdateStatefulSetReplicas updates the number of replicas of the statefulSet
// in the namespace.
// The replicas are changed asynchronously, thus caller has a responsibility to
// check the done of change.
func (c *Client) UpdateStatefulSetReplicas(ctx context.Context, namespace, statefulSetName string, replicas int32) error {
	statefulSets := c.AppsV1().StatefulSets(namespace)

	scale, err := statefulSets.GetScale(ctx, statefulSetName, meta.GetOptions{})
	if err != nil {
		return errors.WithMessage(err, "k8s: get statefulset scale")
	}

	if scale.Spec.Replicas == replicas {
		return nil
	}

	scale.Spec.Replicas = replicas
	_, err = statefulSets.UpdateScale(ctx, statefulSetName, scale, meta.UpdateOptions{})
	return errors.WithMessage(err, "k8s: update statefulset scale")
}

// AddNodeLabel adds or replaces a node label to the Kubernetes node.
func (c *Client) AddNodeLabel(ctx context.Context, nodeName, label, value string) error {
	patch := []PatchStringValue{
		NewLabelPatchStringValue(PatchOpAdd, label, value),
	}
	err := c.applyNodePatch(ctx, nodeName, patch)
	return errors.WithMessage(err, "k8s: add node labels")
}

// ReplaceNodeLabel replaces a node label of the Kubernetes node.
// Deprecated: Use AddNodeLabel.
// More info: https://datatracker.ietf.org/doc/html/rfc6902#section-4.1
func (c *Client) ReplaceNodeLabel(ctx context.Context, nodeName, label, value string) error {
	patch := []PatchStringValue{
		NewLabelPatchStringValue(PatchOpReplace, label, value),
	}
	err := c.applyNodePatch(ctx, nodeName, patch)
	return errors.WithMessage(err, "k8s: replace node labels")
}

// RemoveNodeLabel removes a node label from the Kubernetes node.
func (c *Client) RemoveNodeLabel(ctx context.Context, nodeName, label string) error {
	patch := []PatchStringValue{
		NewLabelPatchStringValue(PatchOpRemove, label, ""),
	}
	err := c.applyNodePatch(ctx, nodeName, patch)
	return errors.WithMessage(err, "k8s: remove node labels")
}

func (c *Client) applyNodePatch(ctx context.Context, nodeName string, patch any) error {
	data, err := json.Marshal(patch)
	if err != nil {
		return errors.WithMessage(err, "k8s: node patch")
	}
	_, err = c.CoreV1().Nodes().Patch(ctx, nodeName, k8stypes.JSONPatchType, data, meta.PatchOptions{})
	return errors.WithMessage(err, "k8s: node patch")
}
