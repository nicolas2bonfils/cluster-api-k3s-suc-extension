// Package inplace implements the Cluster API in-place update hooks for k3s
// nodes, executed by system-upgrade-controller (SUC) in the workload cluster.
//
// Only Machine.spec.version is updated in place: KThreesControlPlane keeps
// the KThreesConfig version of an existing Machine, so a version bump only
// changes the Machine. Any other change is left unclaimed and Cluster API
// falls back to its rollout (which blocks on a single-replica control plane
// with maxSurge 0, the intended behaviour).
//
// UpdateMachine is stateless: the progress lives in the SUC Plan and in the
// Node, so the extension may restart at any time, including while k3s
// restarts on the cluster that runs it.
package inplace

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadClientFunc returns a client for the workload cluster of a Cluster.
type WorkloadClientFunc func(ctx context.Context, cluster types.NamespacedName) (client.Client, error)

// Handlers implements CanUpdateMachine and UpdateMachine.
type Handlers struct {
	// Client reads the management cluster (Machines).
	Client client.Client
	// Workload connects to the workload clusters.
	Workload WorkloadClientFunc
	// Plan holds the cluster-wide parts of the SUC Plans.
	Plan PlanSettings
	// RetryAfter is how long Cluster API waits before calling UpdateMachine
	// again while an upgrade runs.
	RetryAfter time.Duration
}

type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// DoCanUpdateMachine claims Machine.spec.version when the version change is
// one k3s supports in place (same major, same or next minor, no downgrade)
// and SUC runs in the workload cluster. Otherwise it claims nothing.
func (h *Handlers) DoCanUpdateMachine(ctx context.Context, req *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx).WithValues("Machine", client.ObjectKeyFromObject(&req.Current.Machine))

	var ops []jsonPatchOperation
	current, desired := req.Current.Machine.Spec.Version, req.Desired.Machine.Spec.Version
	if current != desired {
		reason := h.versionChangeRefusal(ctx, &req.Desired.Machine, current, desired)
		if reason == "" {
			ops = append(ops, jsonPatchOperation{Op: "replace", Path: "/spec/version", Value: desired})
		} else {
			log.Info("Version change not handled in place", "current", current, "desired", desired, "reason", reason)
		}
	}

	var err error
	if resp.MachinePatch, err = jsonPatch(ops); err != nil {
		fail(&resp.CommonResponse, err)
		return
	}
	if resp.InfrastructureMachinePatch, err = jsonPatch(nil); err != nil {
		fail(&resp.CommonResponse, err)
		return
	}
	if resp.BootstrapConfigPatch, err = jsonPatch(nil); err != nil {
		fail(&resp.CommonResponse, err)
		return
	}
	resp.Status = runtimehooksv1.ResponseStatusSuccess
}

// versionChangeRefusal returns why a version change cannot be done in
// place, or "" when it can.
func (h *Handlers) versionChangeRefusal(ctx context.Context, machine *clusterv1.Machine, current, desired string) string {
	if reason := checkVersionChange(current, desired); reason != "" {
		return reason
	}
	wc, err := h.Workload(ctx, clusterKey(machine))
	if err != nil {
		return fmt.Sprintf("workload cluster unreachable: %v", err)
	}
	plans := &unstructured.UnstructuredList{}
	plans.SetGroupVersionKind(PlanGVK.GroupVersion().WithKind("PlanList"))
	if err := wc.List(ctx, plans, client.InNamespace(h.Plan.Namespace), client.Limit(1)); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return "system-upgrade-controller is not installed in the workload cluster"
		}
		return fmt.Sprintf("cannot list SUC Plans: %v", err)
	}
	return ""
}

// checkVersionChange accepts upgrades within a major version, one minor at a
// time, as k3s supports them.
func checkVersionChange(current, desired string) string {
	cv, err := semver.ParseTolerant(current)
	if err != nil {
		return fmt.Sprintf("current version %q: %v", current, err)
	}
	dv, err := semver.ParseTolerant(desired)
	if err != nil {
		return fmt.Sprintf("desired version %q: %v", desired, err)
	}
	switch {
	case !strings.HasPrefix(desired, "v"):
		return fmt.Sprintf("desired version %q must start with v", desired)
	case dv.Major != cv.Major:
		return "major version change"
	case dv.Minor < cv.Minor || (dv.Minor == cv.Minor && dv.Patch < cv.Patch):
		return "downgrade"
	case dv.Minor > cv.Minor+1:
		return "more than one minor version at a time"
	}
	return ""
}

// DoUpdateMachine upgrades k3s on the Machine's node through a SUC Plan and
// reports success once the node runs the desired version and is Ready.
func (h *Handlers) DoUpdateMachine(ctx context.Context, req *runtimehooksv1.UpdateMachineRequest, resp *runtimehooksv1.UpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx).WithValues("Machine", client.ObjectKeyFromObject(&req.Desired.Machine))
	version := req.Desired.Machine.Spec.Version
	if version == "" {
		fail(&resp.CommonResponse, fmt.Errorf("desired Machine has no version"))
		return
	}

	// The request carries the spec only: the node comes from the stored
	// Machine.
	machine := &clusterv1.Machine{}
	if err := h.Client.Get(ctx, client.ObjectKeyFromObject(&req.Desired.Machine), machine); err != nil {
		fail(&resp.CommonResponse, fmt.Errorf("get Machine: %w", err))
		return
	}
	if !machine.Status.NodeRef.IsDefined() {
		fail(&resp.CommonResponse, fmt.Errorf("machine has no node"))
		return
	}

	wc, err := h.Workload(ctx, clusterKey(machine))
	if err != nil {
		// Expected while k3s restarts on the node: wait for it.
		h.wait(resp, fmt.Sprintf("workload cluster unreachable: %v", err))
		return
	}

	node := &corev1.Node{}
	if err := wc.Get(ctx, client.ObjectKey{Name: machine.Status.NodeRef.Name}, node); err != nil {
		h.wait(resp, fmt.Sprintf("cannot read node %s: %v", machine.Status.NodeRef.Name, err))
		return
	}

	planKey := client.ObjectKey{Namespace: h.Plan.Namespace, Name: PlanName(machine.Name)}
	if nodeAtVersion(node, version) {
		if err := deletePlan(ctx, wc, planKey); err != nil {
			fail(&resp.CommonResponse, fmt.Errorf("delete Plan %s: %w", planKey, err))
			return
		}
		log.Info("Node upgraded", "node", node.Name, "version", version)
		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.Message = fmt.Sprintf("node %s runs %s", node.Name, version)
		return
	}

	plan, err := h.ensurePlan(ctx, wc, machine.Name, node, version)
	if err != nil {
		h.wait(resp, fmt.Sprintf("cannot apply Plan %s: %v", planKey, err))
		return
	}
	if message := planFailure(plan); message != "" {
		// SUC retries the job; Cluster API keeps calling and reports the error.
		fail(&resp.CommonResponse, fmt.Errorf("upgrade of node %s to %s failed: %s", node.Name, version, message))
		return
	}
	h.wait(resp, fmt.Sprintf("upgrading node %s from %s to %s", node.Name, node.Status.NodeInfo.KubeletVersion, version))
}

// ensurePlan creates the node's Plan, or sets its version when it already
// exists (the desired version changed during an upgrade).
func (h *Handlers) ensurePlan(ctx context.Context, wc client.Client, machineName string, node *corev1.Node, version string) (*unstructured.Unstructured, error) {
	desired := BuildPlan(h.Plan, machineName, node, version)
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(PlanGVK)
	err := wc.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := wc.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	case err != nil:
		return nil, err
	}
	if v, _, _ := unstructured.NestedString(existing.Object, "spec", "version"); v != version {
		base := existing.DeepCopy()
		if err := unstructured.SetNestedField(existing.Object, version, "spec", "version"); err != nil {
			return nil, err
		}
		if err := wc.Patch(ctx, existing, client.MergeFrom(base)); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

func deletePlan(ctx context.Context, wc client.Client, key client.ObjectKey) error {
	plan := &unstructured.Unstructured{}
	plan.SetGroupVersionKind(PlanGVK)
	plan.SetNamespace(key.Namespace)
	plan.SetName(key.Name)
	err := wc.Delete(ctx, plan, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return err
	}
	return nil
}

// nodeAtVersion reports whether the kubelet runs version and the node is
// Ready, i.e. k3s restarted on the new binary.
func nodeAtVersion(node *corev1.Node, version string) bool {
	if node.Status.NodeInfo.KubeletVersion != version {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (h *Handlers) wait(resp *runtimehooksv1.UpdateMachineResponse, message string) {
	resp.Status = runtimehooksv1.ResponseStatusSuccess
	resp.Message = message
	resp.RetryAfterSeconds = int32(h.RetryAfter / time.Second)
	if resp.RetryAfterSeconds < 1 {
		resp.RetryAfterSeconds = 1
	}
}

func clusterKey(machine *clusterv1.Machine) types.NamespacedName {
	return types.NamespacedName{Namespace: machine.Namespace, Name: machine.Spec.ClusterName}
}

func jsonPatch(ops []jsonPatchOperation) (runtimehooksv1.Patch, error) {
	if ops == nil {
		ops = []jsonPatchOperation{}
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return runtimehooksv1.Patch{}, fmt.Errorf("marshal JSON patch: %w", err)
	}
	return runtimehooksv1.Patch{PatchType: runtimehooksv1.JSONPatchType, Patch: patch}, nil
}

func fail(resp *runtimehooksv1.CommonResponse, err error) {
	resp.Status = runtimehooksv1.ResponseStatusFailure
	resp.Message = err.Error()
}
