// Package inplace implements the Cluster API in-place update hooks for k3s
// nodes, executed by system-upgrade-controller (SUC) in the workload cluster.
//
// Two kinds of change are updated in place, together or apart, in one k3s
// restart:
//   - Machine.spec.version (KThreesControlPlane keeps the KThreesConfig
//     version of an existing Machine, so a version bump only changes the
//     Machine);
//   - KThreesConfig spec.files drop-ins directly under ConfigDir, added or
//     modified (see checkFilesChange): k0smotron writes the files only when
//     it provisions a machine, so the extension writes them on the host.
//
// Any other change is left unclaimed and Cluster API falls back to its
// rollout (which blocks on a single-replica control plane with maxSurge 0,
// the intended behaviour).
//
// UpdateMachine is stateless: the progress lives in the SUC Plan, its
// Secret and the Node, so the extension may restart at any time, including
// while k3s restarts on the cluster that runs it.
package inplace

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
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
	// RestartTimeout is how long k3s may take to come back Ready after the
	// Plan's job completed before UpdateMachine reports a failure.
	RestartTimeout time.Duration
	// ImmutableKeys are the k3s configuration keys a drop-in change must not
	// touch (DefaultImmutableKeys).
	ImmutableKeys []string
}

// planHashAnnotation records on the Plan the DesiredHash it applies: the
// Plan is only rewritten when the desired state changes, never in the
// middle of its own restart.
const planHashAnnotation = "cluster-api-k3s-suc-extension/hash"

type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

// DoCanUpdateMachine claims Machine.spec.version when the version change is
// one k3s supports in place (same major, same or next minor, no downgrade),
// and the KThreesConfig spec.files when checkFilesChange accepts the change,
// provided SUC runs in the workload cluster. Otherwise it claims nothing.
func (h *Handlers) DoCanUpdateMachine(ctx context.Context, req *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx).WithValues("Machine", client.ObjectKeyFromObject(&req.Current.Machine))

	var machineOps, bootstrapOps []jsonPatchOperation
	current, desired := req.Current.Machine.Spec.Version, req.Desired.Machine.Spec.Version
	if current != desired {
		if reason := checkVersionChange(current, desired); reason == "" {
			machineOps = append(machineOps, jsonPatchOperation{Op: "replace", Path: "/spec/version", Value: desired})
		} else {
			log.Info("Version change not handled in place", "current", current, "desired", desired, "reason", reason)
		}
	}

	currentFiles, currentRaw, err := bootstrapFiles(req.Current.BootstrapConfig)
	if err != nil {
		fail(&resp.CommonResponse, fmt.Errorf("current bootstrap config: %w", err))
		return
	}
	desiredFiles, desiredRaw, err := bootstrapFiles(req.Desired.BootstrapConfig)
	if err != nil {
		fail(&resp.CommonResponse, fmt.Errorf("desired bootstrap config: %w", err))
		return
	}
	if !reflect.DeepEqual(currentRaw, desiredRaw) {
		if reason := checkFilesChange(currentFiles, desiredFiles, h.ImmutableKeys); reason == "" {
			// add replaces the list when it exists (RFC 6902).
			bootstrapOps = append(bootstrapOps, jsonPatchOperation{Op: "add", Path: "/spec/files", Value: desiredRaw})
		} else {
			log.Info("Files change not handled in place", "reason", reason)
		}
	}

	if len(machineOps)+len(bootstrapOps) > 0 {
		if reason := h.sucRefusal(ctx, &req.Desired.Machine); reason != "" {
			log.Info("Change not handled in place", "reason", reason)
			machineOps, bootstrapOps = nil, nil
		}
	}

	if resp.MachinePatch, err = jsonPatch(machineOps); err != nil {
		fail(&resp.CommonResponse, err)
		return
	}
	if resp.InfrastructureMachinePatch, err = jsonPatch(nil); err != nil {
		fail(&resp.CommonResponse, err)
		return
	}
	if resp.BootstrapConfigPatch, err = jsonPatch(bootstrapOps); err != nil {
		fail(&resp.CommonResponse, err)
		return
	}
	resp.Status = runtimehooksv1.ResponseStatusSuccess
}

// sucRefusal returns why the workload cluster cannot run the update (SUC
// missing or the cluster unreachable), or "" when it can.
func (h *Handlers) sucRefusal(ctx context.Context, machine *clusterv1.Machine) string {
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

// DoUpdateMachine brings the Machine's node to the desired k3s version and
// ConfigDir drop-ins through a SUC Plan, and reports success once SUC
// labelled the node with the desired state's hash and the node runs the
// version and is Ready.
func (h *Handlers) DoUpdateMachine(ctx context.Context, req *runtimehooksv1.UpdateMachineRequest, resp *runtimehooksv1.UpdateMachineResponse) {
	log := ctrl.LoggerFrom(ctx).WithValues("Machine", client.ObjectKeyFromObject(&req.Desired.Machine))
	version := req.Desired.Machine.Spec.Version
	if version == "" {
		fail(&resp.CommonResponse, fmt.Errorf("desired Machine has no version"))
		return
	}
	desiredFiles, _, err := bootstrapFiles(req.Desired.BootstrapConfig)
	if err != nil {
		fail(&resp.CommonResponse, fmt.Errorf("desired bootstrap config: %w", err))
		return
	}
	files := configFiles(desiredFiles)
	hash := DesiredHash(version, files)

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

	key := client.ObjectKey{Namespace: h.Plan.Namespace, Name: PlanName(machine.Name)}
	applied := node.Labels[AppliedLabel] == hash
	if applied && nodeAtVersion(node, version) {
		if err := deletePlan(ctx, wc, key); err != nil {
			fail(&resp.CommonResponse, fmt.Errorf("delete Plan %s: %w", key, err))
			return
		}
		if err := wc.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}); err != nil && !apierrors.IsNotFound(err) {
			fail(&resp.CommonResponse, fmt.Errorf("delete Secret %s: %w", key, err))
			return
		}
		log.Info("Node updated", "node", node.Name, "version", version, "files", len(files))
		resp.Status = runtimehooksv1.ResponseStatusSuccess
		resp.Message = fmt.Sprintf("node %s runs %s with %d drop-ins", node.Name, version, len(files))
		return
	}

	versionChange := node.Status.NodeInfo.KubeletVersion != version
	plan, err := h.ensurePlan(ctx, wc, machine.Name, node, version, hash, versionChange, files)
	if err != nil {
		h.wait(resp, fmt.Sprintf("cannot apply Plan %s: %v", key, err))
		return
	}
	if message := planFailure(plan); message != "" {
		// SUC retries the job; Cluster API keeps calling and reports the error.
		fail(&resp.CommonResponse, fmt.Errorf("update of node %s failed: %s", node.Name, message))
		return
	}
	created := plan.GetCreationTimestamp()
	if applied && h.RestartTimeout > 0 && !created.IsZero() && time.Since(created.Time) > h.RestartTimeout {
		fail(&resp.CommonResponse, fmt.Errorf(
			"node %s is not Ready at %s %s after the update was applied; the previous drop-ins are under /var/lib/rancher/k3s-suc-backup on the node",
			node.Name, version, h.RestartTimeout))
		return
	}
	h.wait(resp, fmt.Sprintf("updating node %s to %s (%d drop-ins, kubelet %s)", node.Name, version, len(files), node.Status.NodeInfo.KubeletVersion))
}

// ensurePlan writes the drop-ins' Secret and the node's Plan for hash. An
// existing Plan for the same hash is left as it is (it may be restarting
// k3s); one for another hash is replaced, with its Secret.
func (h *Handlers) ensurePlan(ctx context.Context, wc client.Client, machineName string, node *corev1.Node, version, hash string, versionChange bool, files map[string]string) (*unstructured.Unstructured, error) {
	desired := BuildPlan(h.Plan, machineName, node, version, hash, versionChange)
	desired.SetAnnotations(map[string]string{planHashAnnotation: hash})
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(PlanGVK)
	err := wc.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	switch {
	case err == nil && existing.GetAnnotations()[planHashAnnotation] == hash:
		return existing, nil
	case err != nil && !apierrors.IsNotFound(err):
		return nil, err
	}

	if err := h.ensureSecret(ctx, wc, BuildFilesSecret(h.Plan, machineName, files)); err != nil {
		return nil, err
	}
	if apierrors.IsNotFound(err) {
		if err := wc.Create(ctx, desired); err != nil {
			return nil, err
		}
		return desired, nil
	}
	existing.Object["spec"] = desired.Object["spec"]
	existing.SetAnnotations(desired.GetAnnotations())
	if err := wc.Update(ctx, existing); err != nil {
		return nil, err
	}
	return existing, nil
}

func (h *Handlers) ensureSecret(ctx context.Context, wc client.Client, desired *corev1.Secret) error {
	existing := &corev1.Secret{}
	err := wc.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	switch {
	case apierrors.IsNotFound(err):
		return wc.Create(ctx, desired)
	case err != nil:
		return err
	}
	existing.Data = desired.Data
	existing.Labels = desired.Labels
	return wc.Update(ctx, existing)
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
