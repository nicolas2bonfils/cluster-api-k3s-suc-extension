package inplace

import (
	"crypto/sha256"
	"encoding/hex"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// PlanGVK is the system-upgrade-controller Plan.
var PlanGVK = schema.GroupVersionKind{Group: "upgrade.cattle.io", Version: "v1", Kind: "Plan"}

const (
	// managedByLabel marks the Plans this extension owns.
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "cluster-api-k3s-suc-extension"
	// machineLabel records the Machine a Plan upgrades, for humans.
	machineLabel = "cluster.x-k8s.io/machine-name"

	planNamePrefix = "capi-"
	// SUC labels the node with plan.upgrade.cattle.io/<plan name>: the name
	// part of a label key is limited to 63 characters.
	maxPlanNameLength = 63
	hashLength        = 8

	hostnameLabel = "kubernetes.io/hostname"
)

// PlanName returns the name of the Plan upgrading a Machine.
func PlanName(machineName string) string {
	name := planNamePrefix + machineName
	if len(name) <= maxPlanNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(machineName))
	suffix := "-" + hex.EncodeToString(sum[:])[:hashLength]
	return name[:maxPlanNameLength-len(suffix)] + suffix
}

// PlanLabel returns the node label SUC sets to the Plan hash once the Plan
// is applied on that node.
func PlanLabel(planName string) string {
	return "plan.upgrade.cattle.io/" + planName
}

// PlanSettings are the cluster-wide parts of every Plan.
type PlanSettings struct {
	// Namespace where system-upgrade-controller runs; it only reads Plans
	// from its own namespace.
	Namespace string
	// UpgradeImage replaces the k3s binary and restarts k3s; SUC appends the
	// version as tag, with + replaced by -.
	UpgradeImage string
	// ServiceAccountName runs the upgrade job.
	ServiceAccountName string
}

// BuildPlan returns the Plan that upgrades k3s on one node to version.
//
// No cordon nor drain: the clusters have a single node, where draining
// would only stop the workloads that k3s keeps running across its restart.
// Every taint is tolerated so that the job runs on a control plane node.
func BuildPlan(s PlanSettings, machineName string, node *corev1.Node, version string) *unstructured.Unstructured {
	hostname := node.Labels[hostnameLabel]
	if hostname == "" {
		hostname = node.Name
	}
	plan := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{
			"name":      PlanName(machineName),
			"namespace": s.Namespace,
			"labels": map[string]any{
				managedByLabel: managedByValue,
				machineLabel:   machineName,
			},
		},
		"spec": map[string]any{
			"concurrency":        int64(1),
			"serviceAccountName": s.ServiceAccountName,
			"nodeSelector": map[string]any{
				"matchExpressions": []any{
					map[string]any{
						"key":      hostnameLabel,
						"operator": "In",
						"values":   []any{hostname},
					},
				},
			},
			"tolerations": []any{
				map[string]any{"operator": "Exists"},
			},
			"upgrade": map[string]any{
				"image": s.UpgradeImage,
			},
			"version": version,
		},
	}}
	plan.SetGroupVersionKind(PlanGVK)
	return plan
}

// planFailure returns the message of a failed upgrade job, read from the
// Plan's Complete condition that SUC sets to JobFailed.
func planFailure(plan *unstructured.Unstructured) string {
	conditions, _, _ := unstructured.NestedSlice(plan.Object, "status", "conditions")
	for _, c := range conditions {
		condition, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if condition["type"] == "Complete" && condition["status"] == "False" && condition["reason"] == "JobFailed" {
			message, _ := condition["message"].(string)
			if message == "" {
				message = "upgrade job failed"
			}
			return message
		}
	}
	return ""
}
