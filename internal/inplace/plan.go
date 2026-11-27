package inplace

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// desiredHashLength keeps DesiredHash a valid label value (63 at most).
	desiredHashLength = 16

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
	// ConfigImage writes the ConfigDir drop-ins on the host: any image with
	// a POSIX shell, cp, chmod, chown, mv and chroot (busybox). Tag included:
	// SUC only appends the version to an image without a tag.
	ConfigImage string
}

const (
	// AppliedLabel is set by SUC on the node, from the Plan's
	// postCompleteLabels, once the Plan's job completed: its value is the
	// DesiredHash of the state applied. It stays after the Plan is deleted.
	AppliedLabel = "cluster-api-k3s-suc-extension/applied"

	// filesMountPath is where SUC mounts the Secret of the drop-ins in the job.
	filesMountPath = "/run/k3s-suc/files"

	// keepBackups is how many copies of the drop-ins stay on the host: the
	// one taken before this update and the previous ones.
	keepBackups = "5"

	// writeFilesScript writes the drop-ins from the Secret into the host's
	// ConfigDir, atomically per file, after a copy of the current ones under
	// /var/lib/rancher/k3s-suc-backup, where only the keepBackups latest
	// copies are kept. With RESTART=true it then restarts k3s (files only);
	// otherwise k3s-upgrade restarts it next.
	writeFilesScript = `set -eu
src=` + filesMountPath + `
dst=/host` + ConfigDir + `
backups=/host/var/lib/rancher/k3s-suc-backup
backup=$backups/$(date +%Y%m%dT%H%M%S)
mkdir -p "$dst" "$backup"
cp -p "$dst"/* "$backup"/ 2>/dev/null || true
ls -1 "$backups" | sort -r | tail -n +$((` + keepBackups + ` + 1)) | while read -r old; do
  rm -rf "${backups:?}/$old"
done
for f in "$src"/*; do
  [ -f "$f" ] || continue
  name=$(basename "$f")
  cp "$f" "$dst/.$name.tmp"
  chmod 0644 "$dst/.$name.tmp"
  chown 0:0 "$dst/.$name.tmp"
  mv "$dst/.$name.tmp" "$dst/$name"
  echo "wrote $dst/$name"
done
if [ "${RESTART:-false}" = true ]; then
  chroot /host systemctl restart k3s
  echo "restarted k3s"
fi
`
)

// BuildPlan returns the Plan that brings one node to version with the
// ConfigDir drop-ins of the Secret BuildFilesSecret returns, in one k3s
// restart:
//   - version change: the drop-ins are written in the prepare step, then
//     k3s-upgrade replaces the binary and restarts k3s;
//   - drop-ins only: one step writes them and restarts k3s.
//
// SUC labels the node with AppliedLabel=hash once the job completed.
//
// No cordon nor drain: the clusters have a single node, where draining
// would only stop the workloads that k3s keeps running across its restart.
// Every taint is tolerated so that the job runs on a control plane node.
func BuildPlan(s PlanSettings, machineName string, node *corev1.Node, version, hash string, versionChange bool) *unstructured.Unstructured {
	hostname := node.Labels[hostnameLabel]
	if hostname == "" {
		hostname = node.Name
	}
	writeFiles := func(restart bool) map[string]any {
		return map[string]any{
			"image":   s.ConfigImage,
			"command": []any{"sh", "-c"},
			"args":    []any{writeFilesScript},
			"env":     []any{map[string]any{"name": "RESTART", "value": fmt.Sprint(restart)}},
		}
	}
	spec := map[string]any{
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
		"secrets": []any{
			map[string]any{"name": PlanName(machineName), "path": filesMountPath},
		},
		"postCompleteLabels": map[string]any{AppliedLabel: hash},
		"version":            version,
	}
	if versionChange {
		spec["prepare"] = writeFiles(false)
		spec["upgrade"] = map[string]any{"image": s.UpgradeImage}
	} else {
		spec["upgrade"] = writeFiles(true)
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
		"spec": spec,
	}}
	plan.SetGroupVersionKind(PlanGVK)
	return plan
}

// BuildFilesSecret returns the Secret holding the drop-ins, one key per file
// name, mounted by the Plan of the same name. SUC hashes it into the Plan:
// a change of content runs the job again.
func BuildFilesSecret(s PlanSettings, machineName string, files map[string]string) *corev1.Secret {
	data := make(map[string][]byte, len(files))
	for name, content := range files {
		data[name] = []byte(content)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PlanName(machineName),
			Namespace: s.Namespace,
			Labels: map[string]string{
				managedByLabel: managedByValue,
				machineLabel:   machineName,
			},
		},
		Data: data,
	}
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

// DesiredHash identifies the state a node must reach: k3s version and
// ConfigDir drop-ins. SUC labels the node with it once the Plan completed.
func DesiredHash(version string, files map[string]string) string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	fmt.Fprintf(h, "version=%s\n", version)
	for _, name := range names {
		fmt.Fprintf(h, "file=%s\n%d\n%s\n", name, len(files[name]), files[name])
	}
	return hex.EncodeToString(h.Sum(nil))[:desiredHashLength]
}
