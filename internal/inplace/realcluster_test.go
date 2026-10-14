//go:build realcluster

package inplace

// Runs the hooks against a real single-node k3s with system-upgrade-controller
// installed (Rancher's chart, namespace cattle-system). The management
// cluster is faked: only the Machine is needed.
//
//	WORKLOAD_KUBECONFIG=... TARGET_VERSION=v1.37.1+k3s1 \
//	  go test -tags realcluster -run TestRealCluster -v -timeout 20m ./internal/inplace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRealCluster(t *testing.T) {
	kubeconfig, target := os.Getenv("WORKLOAD_KUBECONFIG"), os.Getenv("TARGET_VERSION")
	if kubeconfig == "" || target == "" {
		t.Skip("WORKLOAD_KUBECONFIG and TARGET_VERSION are required")
	}
	ctrl.SetLogger(testr.New(t))
	ctx := context.Background()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Timeout = 10 * time.Second
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	// A fresh client per call, as in main.go: the API restarts.
	newWorkload := func(context.Context, types.NamespacedName) (client.Client, error) {
		return client.New(cfg, client.Options{Scheme: scheme})
	}
	wc, err := newWorkload(ctx, types.NamespacedName{})
	if err != nil {
		t.Fatal(err)
	}

	nodes := &corev1.NodeList{}
	if err := wc.List(ctx, nodes); err != nil || len(nodes.Items) != 1 {
		t.Fatalf("want one node, got %d (%v)", len(nodes.Items), err)
	}
	node := nodes.Items[0]
	current := node.Status.NodeInfo.KubeletVersion
	t.Logf("node %s runs %s, target %s", node.Name, current, target)

	witnessBefore := witness(t, wc)

	m := machine(target, true)
	m.Status.NodeRef = clusterv1.MachineNodeReference{Name: node.Name}
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(m).Build()
	h := &Handlers{Client: mgmt, Workload: newWorkload, Plan: settings, RetryAfter: 10 * time.Second}

	canReq := &runtimehooksv1.CanUpdateMachineRequest{}
	canReq.Current.Machine = *machine(current, true)
	canReq.Desired.Machine = *machine(target, true)
	canResp := &runtimehooksv1.CanUpdateMachineResponse{}
	h.DoCanUpdateMachine(ctx, canReq, canResp)
	var ops []jsonPatchOperation
	_ = json.Unmarshal(canResp.MachinePatch.Patch, &ops)
	if canResp.Status != runtimehooksv1.ResponseStatusSuccess || len(ops) != 1 {
		t.Fatalf("CanUpdateMachine must claim the version: %s %s %s", canResp.Status, canResp.Message, canResp.MachinePatch.Patch)
	}

	start := time.Now()
	for {
		req := &runtimehooksv1.UpdateMachineRequest{}
		req.Desired.Machine = *machine(target, false)
		resp := &runtimehooksv1.UpdateMachineResponse{}
		h.DoUpdateMachine(ctx, req, resp)
		t.Logf("%4.0fs UpdateMachine %s retry=%d: %s", time.Since(start).Seconds(), resp.Status, resp.RetryAfterSeconds, resp.Message)
		if resp.Status == runtimehooksv1.ResponseStatusFailure {
			t.Fatalf("upgrade failed: %s", resp.Message)
		}
		if resp.RetryAfterSeconds == 0 {
			break
		}
		if time.Since(start) > 15*time.Minute {
			t.Fatal("timeout")
		}
		time.Sleep(time.Duration(resp.RetryAfterSeconds) * time.Second)
	}

	if _, err := getPlan(t, wc); err == nil {
		t.Error("the Plan must be deleted once done")
	}
	witnessAfter := witness(t, wc)
	t.Logf("witness container before %s, after %s", witnessBefore, witnessAfter)
	if witnessBefore != witnessAfter {
		t.Errorf("the witness container was restarted by the upgrade")
	}
}

// witness returns the container ID and restart count of the witness pod.
func witness(t *testing.T, wc client.Client) string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := wc.List(context.Background(), pods, client.InNamespace("default"), client.MatchingLabels{"app": "witness"}); err != nil || len(pods.Items) != 1 {
		t.Fatalf("want the witness pod: %v", err)
	}
	cs := pods.Items[0].Status.ContainerStatuses
	if len(cs) != 1 {
		t.Fatalf("witness has no container status")
	}
	return fmt.Sprintf("%s restarts=%d", cs[0].ContainerID, cs[0].RestartCount)
}
