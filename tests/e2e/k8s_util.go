package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/multierr"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
)

const (
	TypeLabelKey = "varlog-type"
	AppLabelKey  = "app"

	// metadata repository
	MRLabel      = "varlogmr"
	MRStopLabel  = "varlogmr-stop"
	MRDropLabel  = "varlogmr-drop"
	MRClearLabel = "varlogmr-clear"

	// storage node
	SNLabel     = "varlogsn"
	SNStopLabel = "varlogsn-stop"
	SNDropLabel = "varlogsn-drop"

	// vms
	VMSRSName = "varlogadm"

	// vip
	VMSVIPName = "varlogadm-rpc-vip"
	MRVIPName  = "varlogmr-rpc-vip"

	// namespaces
	VarlogNamespace       = "default"
	IngressNginxNamespace = "ingress-nginx"

	EnvRepFactor = "REP_FACTOR"

	// telemetry
	TelemetryLabelValue = "telemetry"
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
	GetMRNodeName(types.NodeID) (string, error)
	GetSNNodeName(types.StorageNodeID) (string, error)

	SetStoppedMR(types.NodeID)
	SetStoppedSN(types.StorageNodeID)
	UnsetStoppedMR()
	UnsetStoppedSN()
	StoppedMR(types.NodeID) bool
	StoppedSN(types.StorageNodeID) bool
}

type k8sVarlogView struct {
	podGetter  K8sVarlogPodGetter
	idGetter   VarlogIDGetter
	mrs        map[types.NodeID]string
	sns        map[types.StorageNodeID]string
	stoppedMRs map[types.NodeID]struct{}
	stoppedSNs map[types.StorageNodeID]struct{}
	mu         sync.RWMutex
	timeout    time.Duration
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
		numPods, err := k8s.numPodsReady(VarlogNamespace, nil)
		if err != nil {
			return false, errors.Wrap(err, "k8s")
		}
		return numPods == 0, nil
	}); err != nil {
		return errors.Wrapf(err, "k8s: numPodsReady=%d", numPods)
	}

	rep := fmt.Sprintf("%d", k8s.RepFactor)
	if err := k8s.ReplaceEnvToDeployment(VMSRSName, EnvRepFactor, rep); err != nil {
		return err
	}

	if err := k8s.ReplaceEnvToDaemonset(MRLabel, EnvRepFactor, rep); err != nil {
		return err
	}

	if err := k8s.clearMRDatas(); err != nil {
		return err
	}

	if k8s.NrMR > 0 {
		if err := k8s.startNodes(MRLabel, 1); err != nil {
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
		Services(IngressNginxNamespace).
		Get(ctx, VMSVIPName, metav1.GetOptions{})
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
		Services(IngressNginxNamespace).
		Get(ctx, MRVIPName, metav1.GetOptions{})
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
		Patch(ctx, node, k8stypes.JSONPatchType, payloadBytes, metav1.PatchOptions{})
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
		Patch(ctx, node, k8stypes.JSONPatchType, payloadBytes, metav1.PatchOptions{})
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
		Patch(ctx, node, k8stypes.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) ScaleReplicaSet(replicasetName string, scale int32) error {
	gCtx, gCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer gCancel()
	s, err := k8s.cli.
		AppsV1().
		Deployments(VarlogNamespace).
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
		Deployments(VarlogNamespace).
		UpdateScale(uCtx, replicasetName, &sc, metav1.UpdateOptions{})
	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) ReplaceEnvToDeployment(deployment, name, value string) error {
	gCtx, gCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer gCancel()
	d, err := k8s.cli.
		AppsV1().
		Deployments(VarlogNamespace).
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
		Deployments(VarlogNamespace).
		Update(uCtx, d, metav1.UpdateOptions{})

	return errors.Wrap(err, "k8s")
}

func (k8s *K8sVarlogCluster) ReplaceEnvToDaemonset(daemonset, name, value string) error {
	gCtx, gCancel := context.WithTimeout(context.Background(), k8s.timeout)
	defer gCancel()
	d, err := k8s.cli.
		AppsV1().
		DaemonSets(VarlogNamespace).
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
		DaemonSets(VarlogNamespace).
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
		numPods, err := k8s.numPodsReady(VarlogNamespace, nil)
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
		if labelValue, ok := node.Labels[TypeLabelKey]; ok {
			if labelValue == TelemetryLabelValue {
				continue
			}
			if erri := k8s.RemoveLabel(node.GetName(), TypeLabelKey); erri != nil {
				err = multierr.Append(err, erri)
			}
		}
	}

	return err
}

func (k8s *K8sVarlogCluster) StopMRs() error {
	return k8s.ReplaceLabelAll(TypeLabelKey, MRLabel, MRStopLabel)
}

func (k8s *K8sVarlogCluster) StopDropMRs() error {
	return k8s.ReplaceLabelAll(TypeLabelKey, MRDropLabel, MRStopLabel)
}

func (k8s *K8sVarlogCluster) StopSNs() error {
	return k8s.ReplaceLabelAll(TypeLabelKey, SNLabel, SNStopLabel)
}

func (k8s *K8sVarlogCluster) StopDropSNs() error {
	return k8s.ReplaceLabelAll(TypeLabelKey, SNDropLabel, SNStopLabel)
}

func (k8s *K8sVarlogCluster) StopVMS() error {
	return k8s.ScaleReplicaSet(VMSRSName, 0)
}

func (k8s *K8sVarlogCluster) StopMR(mrID types.NodeID) error {
	nodeName, err := k8s.view.GetMRNodeName(mrID)
	if err != nil {
		return err
	}

	err = k8s.ReplaceLabel(nodeName, TypeLabelKey, MRStopLabel)
	if err == nil {
		k8s.view.SetStoppedMR(mrID)
	}

	return err
}

func (k8s *K8sVarlogCluster) StopSN(snID types.StorageNodeID) error {
	nodeName, err := k8s.view.GetSNNodeName(snID)
	if err != nil {
		return err
	}

	err = k8s.ReplaceLabel(nodeName, TypeLabelKey, SNStopLabel)
	if err == nil {
		k8s.view.SetStoppedSN(snID)
	}

	return err
}

func (k8s *K8sVarlogCluster) RecoverMR() error {
	err := k8s.ReplaceLabelAll(TypeLabelKey, MRStopLabel, MRLabel)
	if err == nil {
		k8s.view.UnsetStoppedMR()
	}

	return err
}

func (k8s *K8sVarlogCluster) RecoverSN() error {
	err := k8s.ReplaceLabelAll(TypeLabelKey, SNStopLabel, SNLabel)
	if err == nil {
		k8s.view.UnsetStoppedSN()
	}

	return err
}

func (k8s *K8sVarlogCluster) startNodes(label string, expected int) error {
	nodeSelector := map[string]string{TypeLabelKey: label}
	podSelector := map[string]string{AppLabelKey: label}
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

	for idx := range rand.Perm(len(nodes.Items)) {
		node := nodes.Items[idx]
		if _, ok := node.Labels[TypeLabelKey]; ok {
			continue
		}

		err := k8s.AddLabel(node.GetName(), TypeLabelKey, label)
		if err != nil {
			return err
		}

		num++
		numPods := 0
		if err := testutil.CompareWaitErrorWithRetryIntervalN(1000, 10*time.Second, func() (bool, error) {
			numPods, err := k8s.numPodsReady(VarlogNamespace, podSelector)
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

func (k8s *K8sVarlogCluster) clearMRDatas() error {
	podSelector := map[string]string{AppLabelKey: MRClearLabel}

	nodes, err := k8s.WorkerNodes()
	if err != nil {
		return err
	}

	targetNodes := 0
	for _, node := range nodes.Items {
		if _, ok := node.Labels[TypeLabelKey]; ok {
			continue
		}
		targetNodes++
	}

	for _, node := range nodes.Items {
		if _, ok := node.Labels[TypeLabelKey]; ok {
			continue
		}

		err := k8s.AddLabel(node.GetName(), TypeLabelKey, MRClearLabel)
		if err != nil {
			return err
		}
	}

	if err := testutil.CompareWaitErrorWithRetryIntervalN(1000, 10*time.Second, func() (bool, error) {
		numPods, err := k8s.numPodsReady(VarlogNamespace, podSelector)
		return numPods == targetNodes, errors.Wrap(err, "k8s")
	}); err != nil {
		return err
	}

	if err := k8s.StopAll(); err != nil {
		return err
	}

	return nil
}

func (k8s *K8sVarlogCluster) StartMRs() error {
	return k8s.startNodes(MRLabel, k8s.NrMR)
}

func (k8s *K8sVarlogCluster) StartSNs() error {
	return k8s.startNodes(SNLabel, k8s.NrSN)
}

func (k8s *K8sVarlogCluster) StartVMS() error {
	return k8s.ScaleReplicaSet(VMSRSName, 1)
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
	return k8s.NodeNames(map[string]string{TypeLabelKey: MRLabel})
}

func (k8s *K8sVarlogCluster) GetSNs() ([]string, error) {
	return k8s.NodeNames(map[string]string{TypeLabelKey: SNLabel})
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
	n, err := k8s.numPodsReady(VarlogNamespace, map[string]string{AppLabelKey: VMSRSName})
	return n == 1, err
}

func (k8s *K8sVarlogCluster) IsMRRunning() (bool, error) {
	n, err := k8s.numPodsReady(VarlogNamespace, map[string]string{AppLabelKey: MRLabel})
	return n == k8s.NrMR, err
}

func (k8s *K8sVarlogCluster) IsSNRunning() (bool, error) {
	n, err := k8s.numPodsReady(VarlogNamespace, map[string]string{AppLabelKey: SNLabel})
	return n == k8s.NrSN, err
}

func (k8s *K8sVarlogCluster) NumMRRunning() (int, error) {
	return k8s.numPodsReady(VarlogNamespace, map[string]string{AppLabelKey: MRLabel})
}

func (k8s *K8sVarlogCluster) NumSNRunning() (int, error) {
	return k8s.numPodsReady(VarlogNamespace, map[string]string{AppLabelKey: SNLabel})
}

func (k8s *K8sVarlogCluster) StoppedMR(mrID types.NodeID) bool {
	return k8s.view.StoppedMR(mrID)
}

func (k8s *K8sVarlogCluster) StoppedSN(snID types.StorageNodeID) bool {
	return k8s.view.StoppedSN(snID)
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
		podGetter:  podGetter,
		idGetter:   &varlogIDGetter{},
		mrs:        make(map[types.NodeID]string),
		sns:        make(map[types.StorageNodeID]string),
		stoppedMRs: make(map[types.NodeID]struct{}),
		stoppedSNs: make(map[types.StorageNodeID]struct{}),
		timeout:    timeout,
	}
}

func (view *k8sVarlogView) Renew() error {
	pods, err := view.podGetter.Pods(VarlogNamespace, map[string]string{AppLabelKey: MRLabel})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	clusterID := types.ClusterID(0)
	mrs := make(map[types.NodeID]string)
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

	pods, err = view.podGetter.Pods(VarlogNamespace, map[string]string{AppLabelKey: SNLabel})
	if err != nil {
		return errors.Wrap(err, "k8s")
	}

	sns := make(map[types.StorageNodeID]string)
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

func (view *k8sVarlogView) GetMRNodeName(mrID types.NodeID) (string, error) {
	view.mu.Lock()
	defer view.mu.Unlock()

	nodeID, ok := view.mrs[mrID]
	if !ok {
		if err := view.Renew(); err != nil {
			return "", err
		}
		nodeID = view.mrs[mrID]
	}

	return nodeID, nil
}

func (view *k8sVarlogView) GetSNNodeName(snID types.StorageNodeID) (string, error) {
	view.mu.Lock()
	defer view.mu.Unlock()

	nodeID, ok := view.sns[snID]
	if !ok {
		if err := view.Renew(); err != nil {
			return "", err
		}
		nodeID = view.sns[snID]
	}

	return nodeID, nil
}

func (view *k8sVarlogView) SetStoppedMR(mrID types.NodeID) {
	view.mu.Lock()
	defer view.mu.Unlock()

	_, ok := view.mrs[mrID]
	if ok {
		view.stoppedMRs[mrID] = struct{}{}
	}
}

func (view *k8sVarlogView) UnsetStoppedMR() {
	view.mu.Lock()
	defer view.mu.Unlock()

	view.stoppedMRs = make(map[types.NodeID]struct{})
}

func (view *k8sVarlogView) SetStoppedSN(snID types.StorageNodeID) {
	view.mu.Lock()
	defer view.mu.Unlock()

	_, ok := view.sns[snID]
	if ok {
		view.stoppedSNs[snID] = struct{}{}
	}
}

func (view *k8sVarlogView) UnsetStoppedSN() {
	view.mu.Lock()
	defer view.mu.Unlock()

	view.stoppedSNs = make(map[types.StorageNodeID]struct{})
}

func (view *k8sVarlogView) StoppedMR(mrID types.NodeID) bool {
	view.mu.Lock()
	defer view.mu.Unlock()

	_, ok := view.stoppedMRs[mrID]
	return ok
}

func (view *k8sVarlogView) StoppedSN(snID types.StorageNodeID) bool {
	view.mu.Lock()
	defer view.mu.Unlock()

	_, ok := view.stoppedSNs[snID]
	return ok
}
