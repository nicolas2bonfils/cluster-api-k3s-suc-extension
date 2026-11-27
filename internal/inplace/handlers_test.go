package inplace

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	ns          = "cortex"
	clusterName = "cortex"
	machineName = "cortex-cp-abcde"
	nodeName    = "cortex"
	oldVersion  = "v1.36.5+k3s1"
	newVersion  = "v1.37.1+k3s1"

	issuerPath = ConfigDir + "/service-account-issuer.yaml"
	ciliumPath = ConfigDir + "/cilium.yaml"
)

var settings = PlanSettings{
	Namespace:          "cattle-system",
	UpgradeImage:       "rancher/k3s-upgrade",
	ServiceAccountName: "system-upgrade-controller",
	ConfigImage:        "docker.io/library/busybox:1.37.0",
}

func managementScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = clusterv1.AddToScheme(s)
	return s
}

// workloadBuilder returns a workload cluster client that knows Plans only
// when SUC is installed.
func workloadBuilder(withSUC bool) *fake.ClientBuilder {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	b := fake.NewClientBuilder().WithScheme(s)
	if withSUC {
		s.AddKnownTypeWithName(PlanGVK, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(PlanGVK.GroupVersion().WithKind("PlanList"), &unstructured.UnstructuredList{})
		return b
	}
	// The fake client lists unknown unstructured kinds without error, where
	// the API server reports that the kind does not exist.
	return b.WithInterceptorFuncs(interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if list.GetObjectKind().GroupVersionKind().Group == PlanGVK.Group {
				return &meta.NoKindMatchError{GroupKind: PlanGVK.GroupKind(), SearchedVersions: []string{PlanGVK.Version}}
			}
			return c.List(ctx, list, opts...)
		},
	})
}

func machine(version string, withNode bool) *clusterv1.Machine {
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: machineName, Namespace: ns},
		Spec:       clusterv1.MachineSpec{ClusterName: clusterName, Version: version},
	}
	if withNode {
		m.Status.NodeRef = clusterv1.MachineNodeReference{Name: nodeName}
	}
	return m
}

func node(version string, ready corev1.ConditionStatus, labels ...string) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{hostnameLabel: nodeName}},
		Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: version},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}},
		},
	}
	for i := 0; i+1 < len(labels); i += 2 {
		n.Labels[labels[i]] = labels[i+1]
	}
	return n
}

// file is a 0644 root-owned drop-in, as the ClusterClass writes them.
func file(path, content string) map[string]any {
	return map[string]any{"path": path, "permissions": "0644", "owner": "root:root", "content": content}
}

// kthreesConfig returns a KThreesConfig as a hook request carries it.
func kthreesConfig(files ...map[string]any) runtime.RawExtension {
	list := make([]any, 0, len(files))
	for _, f := range files {
		list = append(list, f)
	}
	raw, _ := json.Marshal(map[string]any{
		"apiVersion": "bootstrap.cluster.x-k8s.io/v1beta2",
		"kind":       "KThreesConfig",
		"spec":       map[string]any{"files": list, "version": oldVersion},
	})
	return runtime.RawExtension{Raw: raw}
}

func handlers(mgmt, workload client.Client, workloadErr error) *Handlers {
	return &Handlers{
		Client: mgmt,
		Workload: func(_ context.Context, cluster types.NamespacedName) (client.Client, error) {
			if cluster != (types.NamespacedName{Namespace: ns, Name: clusterName}) {
				return nil, errors.New("unexpected cluster " + cluster.String())
			}
			if workloadErr != nil {
				return nil, workloadErr
			}
			return workload, nil
		},
		Plan:           settings,
		RetryAfter:     20 * time.Second,
		RestartTimeout: 10 * time.Minute,
		ImmutableKeys:  DefaultImmutableKeys,
	}
}

func getPlan(t *testing.T, c client.Client) (*unstructured.Unstructured, error) {
	t.Helper()
	plan := &unstructured.Unstructured{}
	plan.SetGroupVersionKind(PlanGVK)
	err := c.Get(context.Background(), client.ObjectKey{Namespace: settings.Namespace, Name: PlanName(machineName)}, plan)
	return plan, err
}

func getSecret(t *testing.T, c client.Client) (*corev1.Secret, error) {
	t.Helper()
	secret := &corev1.Secret{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: settings.Namespace, Name: PlanName(machineName)}, secret)
	return secret, err
}

func TestPlanName(t *testing.T) {
	if got := PlanName("cortex-cp-abcde"); got != "capi-cortex-cp-abcde" {
		t.Errorf("short name: got %q", got)
	}
	long := strings.Repeat("a", 80)
	got := PlanName(long)
	if len(got) != maxPlanNameLength {
		t.Errorf("long name: got length %d, want %d", len(got), maxPlanNameLength)
	}
	if got != PlanName(long) || got == PlanName(strings.Repeat("a", 79)+"b") {
		t.Errorf("long name must be deterministic and distinct per Machine: %q", got)
	}
}

func TestCheckVersionChange(t *testing.T) {
	for _, tc := range []struct {
		current, desired string
		ok               bool
	}{
		{"v1.36.5+k3s1", "v1.36.6+k3s1", true},
		{"v1.36.5+k3s1", "v1.37.1+k3s1", true},
		{"v1.36.5+k3s1", "v1.36.5+k3s2", true},
		{"v1.36.5+k3s1", "v1.38.0+k3s1", false},
		{"v1.37.1+k3s1", "v1.36.5+k3s1", false},
		{"v1.36.5+k3s1", "v1.36.4+k3s1", false},
		{"v1.36.5+k3s1", "v2.0.0+k3s1", false},
		{"v1.36.5+k3s1", "1.36.6+k3s1", false},
		{"v1.36.5+k3s1", "latest", false},
	} {
		if got := checkVersionChange(tc.current, tc.desired) == ""; got != tc.ok {
			t.Errorf("%s -> %s: accepted=%v, want %v", tc.current, tc.desired, got, tc.ok)
		}
	}
}

func TestCheckFilesChange(t *testing.T) {
	cilium := file(ciliumPath, "flannel-backend: none\n")
	issuer := file(issuerPath, "kube-apiserver-arg+:\n  - service-account-issuer=https://a\n")
	toFiles := func(files ...map[string]any) []k3sFile {
		parsed, _, err := bootstrapFiles(kthreesConfig(files...))
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	with := func(f map[string]any, key string, value any) map[string]any {
		out := map[string]any{}
		for k, v := range f {
			out[k] = v
		}
		out[key] = value
		return out
	}
	for name, tc := range map[string]struct {
		current, desired []map[string]any
		ok               bool
	}{
		"unchanged":                 {[]map[string]any{cilium}, []map[string]any{cilium}, true},
		"drop-in added":             {[]map[string]any{cilium}, []map[string]any{cilium, issuer}, true},
		"drop-in modified":          {[]map[string]any{cilium, issuer}, []map[string]any{cilium, with(issuer, "content", "kube-apiserver-arg+:\n  - service-account-issuer=https://b\n")}, true},
		"drop-in removed":           {[]map[string]any{cilium, issuer}, []map[string]any{cilium}, false},
		"node-ip added":             {nil, []map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.2\n")}, false},
		"node-ip changed":           {[]map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.2\n")}, []map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.3\n")}, false},
		"node-ip in any file":       {[]map[string]any{issuer}, []map[string]any{with(issuer, "content", "node-ip: 10.0.0.3\n")}, false},
		"cluster-cidr appended":     {[]map[string]any{issuer}, []map[string]any{with(issuer, "content", "cluster-cidr+: 10.9.0.0/16\n")}, false},
		"other key next to node-ip": {[]map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.2\n")}, []map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.2\nnode-external-ip: 1.2.3.4\n")}, true},
		"not YAML":                  {[]map[string]any{issuer}, []map[string]any{with(issuer, "content", "a: [\n")}, false},
		"outside config dir":        {nil, []map[string]any{file("/etc/systemd/system/k3s.service.d/10-mounts.conf", "[Unit]\n")}, false},
		"in a subdirectory":         {nil, []map[string]any{file(ConfigDir+"/sub/x.yaml", "a: b\n")}, false},
		"encoded":                   {nil, []map[string]any{with(issuer, "encoding", "base64")}, false},
		"from a Secret":             {nil, []map[string]any{with(issuer, "contentFrom", map[string]any{"secret": map[string]any{"name": "s", "key": "k"}})}, false},
		"other permissions":         {nil, []map[string]any{with(issuer, "permissions", "0600")}, false},
		"other owner":               {nil, []map[string]any{with(issuer, "owner", "k3s:k3s")}, false},
		"unchanged node-ip kept":    {[]map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.2\n")}, []map[string]any{file(ConfigDir+"/node-ip.yaml", "node-ip: 10.0.0.2\n"), issuer}, true},
	} {
		t.Run(name, func(t *testing.T) {
			reason := checkFilesChange(toFiles(tc.current...), toFiles(tc.desired...), DefaultImmutableKeys)
			if (reason == "") != tc.ok {
				t.Errorf("accepted=%v (reason %q), want %v", reason == "", reason, tc.ok)
			}
		})
	}
}

func TestDesiredHash(t *testing.T) {
	a := DesiredHash(newVersion, map[string]string{"a.yaml": "1", "b.yaml": "2"})
	if a != DesiredHash(newVersion, map[string]string{"b.yaml": "2", "a.yaml": "1"}) {
		t.Error("the hash must not depend on map order")
	}
	if len(a) != desiredHashLength {
		t.Errorf("hash length %d, a label value takes at most 63", len(a))
	}
	for name, other := range map[string]string{
		"version": DesiredHash(oldVersion, map[string]string{"a.yaml": "1", "b.yaml": "2"}),
		"content": DesiredHash(newVersion, map[string]string{"a.yaml": "1", "b.yaml": "3"}),
		"name":    DesiredHash(newVersion, map[string]string{"a.yaml": "1", "c.yaml": "2"}),
		"set":     DesiredHash(newVersion, map[string]string{"a.yaml": "1"}),
	} {
		if other == a {
			t.Errorf("a different %s must change the hash", name)
		}
	}
}

func canUpdate(t *testing.T, h *Handlers, current, desired string, currentConfig, desiredConfig runtime.RawExtension) (machineOps, bootstrapOps []jsonPatchOperation) {
	t.Helper()
	req := &runtimehooksv1.CanUpdateMachineRequest{}
	req.Current.Machine = *machine(current, true)
	req.Desired.Machine = *machine(desired, true)
	req.Current.BootstrapConfig = currentConfig
	req.Desired.BootstrapConfig = desiredConfig
	resp := &runtimehooksv1.CanUpdateMachineResponse{}
	h.DoCanUpdateMachine(context.Background(), req, resp)
	if resp.Status != runtimehooksv1.ResponseStatusSuccess {
		t.Fatalf("status %s: %s", resp.Status, resp.Message)
	}
	if p := resp.InfrastructureMachinePatch; p.PatchType != runtimehooksv1.JSONPatchType || string(p.Patch) != "[]" {
		t.Errorf("infra patch must be empty, got %s %s", p.PatchType, p.Patch)
	}
	if err := json.Unmarshal(resp.MachinePatch.Patch, &machineOps); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(resp.BootstrapConfigPatch.Patch, &bootstrapOps); err != nil {
		t.Fatal(err)
	}
	return machineOps, bootstrapOps
}

func TestCanUpdateMachineClaimsVersion(t *testing.T) {
	wc := workloadBuilder(true).Build()
	h := handlers(fake.NewClientBuilder().WithScheme(managementScheme()).Build(), wc, nil)
	cfg := kthreesConfig(file(ciliumPath, "a: b\n"))
	machineOps, bootstrapOps := canUpdate(t, h, oldVersion, newVersion, cfg, cfg)
	if len(machineOps) != 1 || machineOps[0].Op != "replace" || machineOps[0].Path != "/spec/version" || machineOps[0].Value != newVersion {
		t.Errorf("got %+v", machineOps)
	}
	if len(bootstrapOps) != 0 {
		t.Errorf("unchanged files must not be claimed, got %+v", bootstrapOps)
	}
}

func TestCanUpdateMachineClaimsFiles(t *testing.T) {
	wc := workloadBuilder(true).Build()
	h := handlers(fake.NewClientBuilder().WithScheme(managementScheme()).Build(), wc, nil)
	current := kthreesConfig(file(ciliumPath, "a: b\n"))
	desired := kthreesConfig(file(ciliumPath, "a: b\n"), file(issuerPath, "x: y\n"))

	machineOps, bootstrapOps := canUpdate(t, h, newVersion, newVersion, current, desired)
	if len(machineOps) != 0 {
		t.Errorf("same version: no machine patch, got %+v", machineOps)
	}
	if len(bootstrapOps) != 1 || bootstrapOps[0].Op != "add" || bootstrapOps[0].Path != "/spec/files" {
		t.Fatalf("got %+v", bootstrapOps)
	}
	if files, ok := bootstrapOps[0].Value.([]any); !ok || len(files) != 2 {
		t.Errorf("the patch must set the desired list, got %+v", bootstrapOps[0].Value)
	}

	// Together with a version change: both claimed.
	machineOps, bootstrapOps = canUpdate(t, h, oldVersion, newVersion, current, desired)
	if len(machineOps) != 1 || len(bootstrapOps) != 1 {
		t.Errorf("version and files must both be claimed, got %+v %+v", machineOps, bootstrapOps)
	}
}

func TestCanUpdateMachineClaimsNothing(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).Build()
	withSUC := workloadBuilder(true).Build()
	withoutSUC := workloadBuilder(false).Build()
	cilium := kthreesConfig(file(ciliumPath, "a: b\n"))
	withIssuer := kthreesConfig(file(ciliumPath, "a: b\n"), file(issuerPath, "x: y\n"))
	for name, tc := range map[string]struct {
		h                    *Handlers
		current, desired     string
		currentCfg, desiredC runtime.RawExtension
	}{
		"same version and files": {handlers(mgmt, withSUC, nil), oldVersion, oldVersion, cilium, cilium},
		"downgrade":              {handlers(mgmt, withSUC, nil), newVersion, oldVersion, cilium, cilium},
		"two minors":             {handlers(mgmt, withSUC, nil), "v1.35.9+k3s1", newVersion, cilium, cilium},
		"file removed":           {handlers(mgmt, withSUC, nil), oldVersion, oldVersion, withIssuer, cilium},
		"SUC not installed":      {handlers(mgmt, withoutSUC, nil), oldVersion, newVersion, cilium, withIssuer},
		"cluster unreachable":    {handlers(mgmt, nil, errors.New("connection refused")), oldVersion, newVersion, cilium, withIssuer},
	} {
		t.Run(name, func(t *testing.T) {
			machineOps, bootstrapOps := canUpdate(t, tc.h, tc.current, tc.desired, tc.currentCfg, tc.desiredC)
			if len(machineOps) != 0 || len(bootstrapOps) != 0 {
				t.Errorf("must claim nothing, got %+v %+v", machineOps, bootstrapOps)
			}
		})
	}
}

func updateMachine(t *testing.T, h *Handlers, cfg runtime.RawExtension) *runtimehooksv1.UpdateMachineResponse {
	t.Helper()
	req := &runtimehooksv1.UpdateMachineRequest{}
	req.Desired.Machine = *machine(newVersion, false)
	req.Desired.BootstrapConfig = cfg
	resp := &runtimehooksv1.UpdateMachineResponse{}
	h.DoUpdateMachine(context.Background(), req, resp)
	return resp
}

var desiredConfig = kthreesConfig(
	file(ciliumPath, "flannel-backend: none\n"),
	file(issuerPath, "kube-apiserver-arg+:\n  - service-account-issuer=https://hub\n"),
	file(ConfigDir+"/node-ip.yaml", "node-ip: 192.168.1.2\n"),
	file("/etc/systemd/system/k3s.service.d/10-mounts.conf", "[Unit]\n"),
)

var desiredFiles = map[string]string{
	"cilium.yaml":                 "flannel-backend: none\n",
	"service-account-issuer.yaml": "kube-apiserver-arg+:\n  - service-account-issuer=https://hub\n",
	"node-ip.yaml":                "node-ip: 192.168.1.2\n",
}

func TestUpdateMachineVersionAndFiles(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	wc := workloadBuilder(true).WithObjects(node(oldVersion, corev1.ConditionTrue)).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig)
	if resp.Status != runtimehooksv1.ResponseStatusSuccess || resp.RetryAfterSeconds != 20 {
		t.Fatalf("want in progress, got %s retry=%d: %s", resp.Status, resp.RetryAfterSeconds, resp.Message)
	}

	secret, err := getSecret(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	if len(secret.Data) != len(desiredFiles) {
		t.Errorf("the Secret must hold the ConfigDir drop-ins only (no systemd unit), got %v", keys(secret.Data))
	}
	for name, content := range desiredFiles {
		if string(secret.Data[name]) != content {
			t.Errorf("Secret %s = %q, want %q", name, secret.Data[name], content)
		}
	}

	plan, err := getPlan(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	spec := plan.Object["spec"].(map[string]any)
	if spec["version"] != newVersion || spec["serviceAccountName"] != "system-upgrade-controller" {
		t.Errorf("unexpected spec %+v", spec)
	}
	if img, _, _ := unstructured.NestedString(plan.Object, "spec", "upgrade", "image"); img != "rancher/k3s-upgrade" {
		t.Errorf("a version change upgrades with k3s-upgrade, got %q", img)
	}
	if img, _, _ := unstructured.NestedString(plan.Object, "spec", "prepare", "image"); img != settings.ConfigImage {
		t.Errorf("the drop-ins are written in the prepare step, got %q", img)
	}
	if env, _, _ := unstructured.NestedSlice(plan.Object, "spec", "prepare", "env"); len(env) != 1 || env[0].(map[string]any)["value"] != "false" {
		t.Errorf("the prepare step must not restart k3s, got %+v", env)
	}
	hash := DesiredHash(newVersion, desiredFiles)
	if v, _, _ := unstructured.NestedString(plan.Object, "spec", "postCompleteLabels", AppliedLabel); v != hash {
		t.Errorf("postCompleteLabels %s = %q, want %q", AppliedLabel, v, hash)
	}
	if plan.GetAnnotations()[planHashAnnotation] != hash {
		t.Errorf("hash annotation %q, want %q", plan.GetAnnotations()[planHashAnnotation], hash)
	}
	secrets, _, _ := unstructured.NestedSlice(plan.Object, "spec", "secrets")
	if len(secrets) != 1 || secrets[0].(map[string]any)["name"] != PlanName(machineName) {
		t.Errorf("the Plan must mount its Secret, got %+v", secrets)
	}
	values, _, _ := unstructured.NestedSlice(plan.Object, "spec", "nodeSelector", "matchExpressions")
	if len(values) != 1 || values[0].(map[string]any)["values"].([]any)[0] != nodeName {
		t.Errorf("node selector %+v", values)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(plan.Object, "spec", "cordon"); found {
		t.Errorf("a single node must not be cordoned")
	}
}

func TestUpdateMachineFilesOnly(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	wc := workloadBuilder(true).WithObjects(node(newVersion, corev1.ConditionTrue)).Build()

	if resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig); resp.RetryAfterSeconds == 0 {
		t.Fatalf("same version but drop-ins not applied: want in progress, got %s", resp.Message)
	}
	plan, err := getPlan(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(plan.Object, "spec", "prepare"); found {
		t.Error("drop-ins only: no prepare step")
	}
	if img, _, _ := unstructured.NestedString(plan.Object, "spec", "upgrade", "image"); img != settings.ConfigImage {
		t.Errorf("drop-ins only: the upgrade step writes them, got %q", img)
	}
	if env, _, _ := unstructured.NestedSlice(plan.Object, "spec", "upgrade", "env"); len(env) != 1 || env[0].(map[string]any)["value"] != "true" {
		t.Errorf("drop-ins only: the step restarts k3s, got %+v", env)
	}
}

func TestUpdateMachineKeepsPlanForSameHash(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	wc := workloadBuilder(true).WithObjects(node(oldVersion, corev1.ConditionTrue)).Build()
	h := handlers(mgmt, wc, nil)
	updateMachine(t, h, desiredConfig)

	// k3s-upgrade restarted k3s on the new version, SUC has not labelled the
	// node yet: the Plan must not be turned into a drop-ins-only Plan.
	n := &corev1.Node{}
	if err := wc.Get(context.Background(), client.ObjectKey{Name: nodeName}, n); err != nil {
		t.Fatal(err)
	}
	n.Status.NodeInfo.KubeletVersion = newVersion
	if err := wc.Update(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if resp := updateMachine(t, h, desiredConfig); resp.RetryAfterSeconds == 0 {
		t.Fatalf("not labelled yet: want in progress, got %s", resp.Message)
	}
	plan, err := getPlan(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	if img, _, _ := unstructured.NestedString(plan.Object, "spec", "upgrade", "image"); img != "rancher/k3s-upgrade" {
		t.Errorf("the Plan of the same hash must stay as created, got upgrade image %q", img)
	}
}

func TestUpdateMachineReplacesPlanForNewHash(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(newVersion, corev1.ConditionTrue)
	old := BuildPlan(settings, machineName, n, newVersion, "oldhash", false)
	old.SetAnnotations(map[string]string{planHashAnnotation: "oldhash"})
	oldSecret := BuildFilesSecret(settings, machineName, map[string]string{"cilium.yaml": "old\n"})
	wc := workloadBuilder(true).WithObjects(n, old, oldSecret).Build()

	updateMachine(t, handlers(mgmt, wc, nil), desiredConfig)
	plan, err := getPlan(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	hash := DesiredHash(newVersion, desiredFiles)
	if plan.GetAnnotations()[planHashAnnotation] != hash {
		t.Errorf("the Plan must be replaced for the new state, got hash %q", plan.GetAnnotations()[planHashAnnotation])
	}
	secret, err := getSecret(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["cilium.yaml"]) != desiredFiles["cilium.yaml"] || len(secret.Data) != len(desiredFiles) {
		t.Errorf("the Secret must hold the new drop-ins, got %v", keys(secret.Data))
	}
}

func TestUpdateMachineReportsJobFailure(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(oldVersion, corev1.ConditionTrue)
	hash := DesiredHash(newVersion, desiredFiles)
	plan := BuildPlan(settings, machineName, n, newVersion, hash, true)
	plan.SetAnnotations(map[string]string{planHashAnnotation: hash})
	_ = unstructured.SetNestedSlice(plan.Object, []any{map[string]any{
		"type": "Complete", "status": "False", "reason": "JobFailed", "message": "Job failed on Node cortex",
	}}, "status", "conditions")
	wc := workloadBuilder(true).WithObjects(n, plan).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig)
	if resp.Status != runtimehooksv1.ResponseStatusFailure || !strings.Contains(resp.Message, "Job failed on Node cortex") {
		t.Errorf("want failure with the job message, got %s: %s", resp.Status, resp.Message)
	}
}

func TestUpdateMachineDone(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	hash := DesiredHash(newVersion, desiredFiles)
	n := node(newVersion, corev1.ConditionTrue, AppliedLabel, hash)
	plan := BuildPlan(settings, machineName, n, newVersion, hash, false)
	secret := BuildFilesSecret(settings, machineName, desiredFiles)
	wc := workloadBuilder(true).WithObjects(n, plan, secret).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig)
	if resp.Status != runtimehooksv1.ResponseStatusSuccess || resp.RetryAfterSeconds != 0 {
		t.Fatalf("want done, got %s retry=%d: %s", resp.Status, resp.RetryAfterSeconds, resp.Message)
	}
	if _, err := getPlan(t, wc); !apierrors.IsNotFound(err) {
		t.Errorf("the Plan must be deleted once done, got %v", err)
	}
	if _, err := getSecret(t, wc); !apierrors.IsNotFound(err) {
		t.Errorf("the Secret must be deleted once done, got %v", err)
	}
	// Called again after completion: still done, nothing recreated.
	if resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig); resp.RetryAfterSeconds != 0 {
		t.Errorf("second call must stay done: %s", resp.Message)
	}
	if _, err := getPlan(t, wc); !apierrors.IsNotFound(err) {
		t.Errorf("no Plan must be recreated, got %v", err)
	}
}

func TestUpdateMachineNotDoneWithoutLabel(t *testing.T) {
	// The node runs the version but was never labelled with this state (an
	// earlier state, or a node upgraded by v0.1): the drop-ins are applied.
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	wc := workloadBuilder(true).WithObjects(node(newVersion, corev1.ConditionTrue, AppliedLabel, "other")).Build()

	if resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig); resp.RetryAfterSeconds == 0 {
		t.Fatalf("want in progress, got %s", resp.Message)
	}
	if _, err := getPlan(t, wc); err != nil {
		t.Errorf("a Plan must be created, got %v", err)
	}
}

func TestUpdateMachineRestartTimeout(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	hash := DesiredHash(newVersion, desiredFiles)
	n := node(newVersion, corev1.ConditionFalse, AppliedLabel, hash)
	plan := BuildPlan(settings, machineName, n, newVersion, hash, false)
	plan.SetAnnotations(map[string]string{planHashAnnotation: hash})
	plan.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-time.Hour)))
	wc := workloadBuilder(true).WithObjects(n, plan).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil), desiredConfig)
	if resp.Status != runtimehooksv1.ResponseStatusFailure || !strings.Contains(resp.Message, "k3s-suc-backup") {
		t.Errorf("applied but not Ready for an hour: want a failure naming the backup, got %s: %s", resp.Status, resp.Message)
	}
}

func TestUpdateMachineWaits(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	hash := DesiredHash(newVersion, desiredFiles)
	notReady := workloadBuilder(true).WithObjects(node(newVersion, corev1.ConditionFalse, AppliedLabel, hash)).Build()
	for name, h := range map[string]*Handlers{
		"applied but NotReady":    handlers(mgmt, notReady, nil),
		"API down during restart": handlers(mgmt, nil, errors.New("connection refused")),
	} {
		t.Run(name, func(t *testing.T) {
			resp := updateMachine(t, h, desiredConfig)
			if resp.Status != runtimehooksv1.ResponseStatusSuccess || resp.RetryAfterSeconds == 0 {
				t.Errorf("want in progress, got %s retry=%d: %s", resp.Status, resp.RetryAfterSeconds, resp.Message)
			}
		})
	}
}

func TestUpdateMachineWithoutNode(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, false)).Build()
	resp := updateMachine(t, handlers(mgmt, workloadBuilder(true).Build(), nil), desiredConfig)
	if resp.Status != runtimehooksv1.ResponseStatusFailure {
		t.Errorf("want failure, got %s: %s", resp.Status, resp.Message)
	}
}

func TestWriteFilesScriptRestartsOnlyWhenAsked(t *testing.T) {
	if !strings.Contains(writeFilesScript, `if [ "${RESTART:-false}" = true ]; then`) {
		t.Error("the restart must depend on RESTART")
	}
	if !strings.Contains(writeFilesScript, "dst=/host"+ConfigDir) {
		t.Error("the script must write into the host's ConfigDir")
	}
}

func TestWriteFilesScriptKeepsTheLatestBackups(t *testing.T) {
	if !strings.Contains(writeFilesScript, "tail -n +$(("+keepBackups+" + 1))") {
		t.Errorf("the script must keep the %s latest backups", keepBackups)
	}
	if !strings.Contains(writeFilesScript, `rm -rf "${backups:?}/$old"`) {
		t.Error("the pruning must stay under the backup directory")
	}
}

func keys(data map[string][]byte) []string {
	out := make([]string, 0, len(data))
	for k := range data {
		out = append(out, k)
	}
	return out
}
