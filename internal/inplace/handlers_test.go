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
)

var settings = PlanSettings{
	Namespace:          "cattle-system",
	UpgradeImage:       "rancher/k3s-upgrade",
	ServiceAccountName: "system-upgrade-controller",
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
	}
}

func getPlan(t *testing.T, c client.Client) (*unstructured.Unstructured, error) {
	t.Helper()
	plan := &unstructured.Unstructured{}
	plan.SetGroupVersionKind(PlanGVK)
	err := c.Get(context.Background(), client.ObjectKey{Namespace: settings.Namespace, Name: PlanName(machineName)}, plan)
	return plan, err
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

func TestDesiredHash(t *testing.T) {
	a := DesiredHash(newVersion, map[string]string{"a.yaml": "1", "b.yaml": "2"})
	if DesiredHash(newVersion, nil) == DesiredHash(oldVersion, nil) {
		t.Error("the version must change the hash")
	}
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

func canUpdate(t *testing.T, h *Handlers, current, desired string) []jsonPatchOperation {
	t.Helper()
	req := &runtimehooksv1.CanUpdateMachineRequest{}
	req.Current.Machine = *machine(current, true)
	req.Desired.Machine = *machine(desired, true)
	resp := &runtimehooksv1.CanUpdateMachineResponse{}
	h.DoCanUpdateMachine(context.Background(), req, resp)
	if resp.Status != runtimehooksv1.ResponseStatusSuccess {
		t.Fatalf("status %s: %s", resp.Status, resp.Message)
	}
	for _, p := range []runtimehooksv1.Patch{resp.InfrastructureMachinePatch, resp.BootstrapConfigPatch} {
		if p.PatchType != runtimehooksv1.JSONPatchType || string(p.Patch) != "[]" {
			t.Errorf("infra and bootstrap patches must be empty, got %s %s", p.PatchType, p.Patch)
		}
	}
	var ops []jsonPatchOperation
	if err := json.Unmarshal(resp.MachinePatch.Patch, &ops); err != nil {
		t.Fatal(err)
	}
	return ops
}

func TestCanUpdateMachineClaimsVersion(t *testing.T) {
	wc := workloadBuilder(true).Build()
	h := handlers(fake.NewClientBuilder().WithScheme(managementScheme()).Build(), wc, nil)
	ops := canUpdate(t, h, oldVersion, newVersion)
	if len(ops) != 1 || ops[0].Op != "replace" || ops[0].Path != "/spec/version" || ops[0].Value != newVersion {
		t.Errorf("got %+v", ops)
	}
}

func TestCanUpdateMachineClaimsNothing(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).Build()
	withSUC := workloadBuilder(true).Build()
	withoutSUC := workloadBuilder(false).Build()
	for name, tc := range map[string]struct {
		h                *Handlers
		current, desired string
	}{
		"same version":        {handlers(mgmt, withSUC, nil), oldVersion, oldVersion},
		"downgrade":           {handlers(mgmt, withSUC, nil), newVersion, oldVersion},
		"two minors":          {handlers(mgmt, withSUC, nil), "v1.35.9+k3s1", newVersion},
		"SUC not installed":   {handlers(mgmt, withoutSUC, nil), oldVersion, newVersion},
		"cluster unreachable": {handlers(mgmt, nil, errors.New("connection refused")), oldVersion, newVersion},
	} {
		t.Run(name, func(t *testing.T) {
			if ops := canUpdate(t, tc.h, tc.current, tc.desired); len(ops) != 0 {
				t.Errorf("must claim nothing, got %+v", ops)
			}
		})
	}
}

func updateMachine(t *testing.T, h *Handlers) *runtimehooksv1.UpdateMachineResponse {
	t.Helper()
	req := &runtimehooksv1.UpdateMachineRequest{}
	req.Desired.Machine = *machine(newVersion, false)
	resp := &runtimehooksv1.UpdateMachineResponse{}
	h.DoUpdateMachine(context.Background(), req, resp)
	return resp
}

var desiredHash = DesiredHash(newVersion, nil)

func TestUpdateMachineCreatesPlan(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	wc := workloadBuilder(true).WithObjects(node(oldVersion, corev1.ConditionTrue)).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil))
	if resp.Status != runtimehooksv1.ResponseStatusSuccess || resp.RetryAfterSeconds != 20 {
		t.Fatalf("want in progress, got %s retry=%d: %s", resp.Status, resp.RetryAfterSeconds, resp.Message)
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
		t.Errorf("upgrade image %q", img)
	}
	if v, _, _ := unstructured.NestedString(plan.Object, "spec", "postCompleteLabels", AppliedLabel); v != desiredHash {
		t.Errorf("postCompleteLabels %s = %q, want %q", AppliedLabel, v, desiredHash)
	}
	if plan.GetAnnotations()[planHashAnnotation] != desiredHash {
		t.Errorf("hash annotation %q, want %q", plan.GetAnnotations()[planHashAnnotation], desiredHash)
	}
	values, _, _ := unstructured.NestedSlice(plan.Object, "spec", "nodeSelector", "matchExpressions")
	if len(values) != 1 || values[0].(map[string]any)["values"].([]any)[0] != nodeName {
		t.Errorf("node selector %+v", values)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(plan.Object, "spec", "cordon"); found {
		t.Errorf("a single node must not be cordoned")
	}
}

func TestUpdateMachineReplacesPlanForNewHash(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(oldVersion, corev1.ConditionTrue)
	old := BuildPlan(settings, machineName, n, "v1.36.6+k3s1", "oldhash")
	old.SetAnnotations(map[string]string{planHashAnnotation: "oldhash"})
	wc := workloadBuilder(true).WithObjects(n, old).Build()

	if resp := updateMachine(t, handlers(mgmt, wc, nil)); resp.RetryAfterSeconds == 0 {
		t.Fatalf("want in progress: %s", resp.Message)
	}
	plan, err := getPlan(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	if v, _, _ := unstructured.NestedString(plan.Object, "spec", "version"); v != newVersion {
		t.Errorf("Plan version %q, want %q", v, newVersion)
	}
	if plan.GetAnnotations()[planHashAnnotation] != desiredHash {
		t.Errorf("the Plan must be replaced for the new state, got hash %q", plan.GetAnnotations()[planHashAnnotation])
	}
}

func TestUpdateMachineKeepsPlanForSameHash(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(newVersion, corev1.ConditionTrue)
	plan := BuildPlan(settings, machineName, n, newVersion, desiredHash)
	plan.SetAnnotations(map[string]string{planHashAnnotation: desiredHash})
	plan.SetLabels(map[string]string{"kept": "yes"})
	wc := workloadBuilder(true).WithObjects(n, plan).Build()

	// k3s already restarted on the new version, SUC has not labelled the
	// node yet: still in progress, the Plan untouched.
	if resp := updateMachine(t, handlers(mgmt, wc, nil)); resp.RetryAfterSeconds == 0 {
		t.Fatalf("not labelled yet: want in progress, got %s", resp.Message)
	}
	got, err := getPlan(t, wc)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetLabels()["kept"] != "yes" {
		t.Error("the Plan of the same hash must stay as it is")
	}
}

func TestUpdateMachineReportsJobFailure(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(oldVersion, corev1.ConditionTrue)
	plan := BuildPlan(settings, machineName, n, newVersion, desiredHash)
	plan.SetAnnotations(map[string]string{planHashAnnotation: desiredHash})
	_ = unstructured.SetNestedSlice(plan.Object, []any{map[string]any{
		"type": "Complete", "status": "False", "reason": "JobFailed", "message": "Job failed on Node cortex",
	}}, "status", "conditions")
	wc := workloadBuilder(true).WithObjects(n, plan).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil))
	if resp.Status != runtimehooksv1.ResponseStatusFailure || !strings.Contains(resp.Message, "Job failed on Node cortex") {
		t.Errorf("want failure with the job message, got %s: %s", resp.Status, resp.Message)
	}
}

func TestUpdateMachineDone(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(newVersion, corev1.ConditionTrue, AppliedLabel, desiredHash)
	wc := workloadBuilder(true).WithObjects(n, BuildPlan(settings, machineName, n, newVersion, desiredHash)).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil))
	if resp.Status != runtimehooksv1.ResponseStatusSuccess || resp.RetryAfterSeconds != 0 {
		t.Fatalf("want done, got %s retry=%d: %s", resp.Status, resp.RetryAfterSeconds, resp.Message)
	}
	if _, err := getPlan(t, wc); !apierrors.IsNotFound(err) {
		t.Errorf("the Plan must be deleted once done, got %v", err)
	}
	if resp := updateMachine(t, handlers(mgmt, wc, nil)); resp.RetryAfterSeconds != 0 {
		t.Errorf("second call must stay done: %s", resp.Message)
	}
	if _, err := getPlan(t, wc); !apierrors.IsNotFound(err) {
		t.Errorf("no Plan must be recreated, got %v", err)
	}
}

func TestUpdateMachineNotDoneWithoutLabel(t *testing.T) {
	// The node runs the version, but SUC did not label it with this state:
	// the job did not complete (or ran for another state).
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	wc := workloadBuilder(true).WithObjects(node(newVersion, corev1.ConditionTrue, AppliedLabel, "other")).Build()

	if resp := updateMachine(t, handlers(mgmt, wc, nil)); resp.RetryAfterSeconds == 0 {
		t.Fatalf("want in progress, got %s", resp.Message)
	}
	if _, err := getPlan(t, wc); err != nil {
		t.Errorf("a Plan must be created, got %v", err)
	}
}

func TestUpdateMachineRestartTimeout(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	n := node(newVersion, corev1.ConditionFalse, AppliedLabel, desiredHash)
	plan := BuildPlan(settings, machineName, n, newVersion, desiredHash)
	plan.SetAnnotations(map[string]string{planHashAnnotation: desiredHash})
	plan.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-time.Hour)))
	wc := workloadBuilder(true).WithObjects(n, plan).Build()

	resp := updateMachine(t, handlers(mgmt, wc, nil))
	if resp.Status != runtimehooksv1.ResponseStatusFailure || !strings.Contains(resp.Message, "not Ready") {
		t.Errorf("applied but not Ready for an hour: want a failure, got %s: %s", resp.Status, resp.Message)
	}
}

func TestUpdateMachineWaits(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, true)).Build()
	notReady := workloadBuilder(true).WithObjects(node(newVersion, corev1.ConditionFalse, AppliedLabel, desiredHash)).Build()
	for name, h := range map[string]*Handlers{
		"applied but NotReady":    handlers(mgmt, notReady, nil),
		"API down during restart": handlers(mgmt, nil, errors.New("connection refused")),
	} {
		t.Run(name, func(t *testing.T) {
			resp := updateMachine(t, h)
			if resp.Status != runtimehooksv1.ResponseStatusSuccess || resp.RetryAfterSeconds == 0 {
				t.Errorf("want in progress, got %s retry=%d: %s", resp.Status, resp.RetryAfterSeconds, resp.Message)
			}
		})
	}
}

func TestUpdateMachineWithoutNode(t *testing.T) {
	mgmt := fake.NewClientBuilder().WithScheme(managementScheme()).WithObjects(machine(newVersion, false)).Build()
	resp := updateMachine(t, handlers(mgmt, workloadBuilder(true).Build(), nil))
	if resp.Status != runtimehooksv1.ResponseStatusFailure {
		t.Errorf("want failure, got %s: %s", resp.Status, resp.Message)
	}
}
