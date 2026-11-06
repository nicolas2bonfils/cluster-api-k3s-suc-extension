# cluster-api-k3s-suc-extension

Cluster API Runtime Extension that updates k3s control plane nodes **in place**
(no replacement Machine) by driving
[system-upgrade-controller](https://github.com/rancher/system-upgrade-controller)
(SUC) in the workload cluster. Made for single-node clusters, where a
replacement needs a second machine.

## Hooks

- `CanUpdateMachine`: claims `Machine.spec.version` when the change is an
  upgrade k3s supports (same major, same or next minor) and SUC runs in the
  workload cluster. Anything else is left unclaimed, so Cluster API falls back
  to its rollout — which, with `maxSurge: 0` on one replica, blocks instead of
  deleting the node.
- `UpdateMachine`: creates a SUC `Plan` (`capi-<machine>`, in SUC's
  namespace) for the Machine's node with the desired version and the
  `rancher/k3s-upgrade` image, then reports progress until the node's kubelet
  runs that version and the node is Ready; the Plan is then deleted. A failed
  upgrade job (Plan condition `Complete=False/JobFailed`) is reported as a
  failure. All state is in the Plan and the Node: the extension can restart
  at any time, including while k3s restarts on the cluster that hosts it.

No cordon nor drain: on a single node they would only stop the workloads that
keep running while k3s restarts.

## Requirements

- Cluster API ≥ v1.12, core manager with `RuntimeSDK=true,InPlaceUpdates=true`.
- cluster-api-k3s with the in-place control plane support (PR #249), its
  control plane manager with `InPlaceUpdates=true`, and
  `rolloutStrategy.rollingUpdate.maxSurge: 0` on the `KThreesControlPlane`.
- SUC in every workload cluster (default namespace `cattle-system`, as
  Rancher's `system-upgrade-controller` chart; the jobs run with its
  ServiceAccount).
- cert-manager on the management cluster (serving certificate).

## Install

Each release (`v*` tag) publishes the image `ghcr.io/nicolas2bonfils/cluster-api-k3s-suc-extension:<tag>`
and, as release assets, `runtime-extension-components.yaml` and `metadata.yaml`. With
Cluster API Operator, in namespace `capi-k3s-suc-extension` (the components use it):

```yaml
apiVersion: operator.cluster.x-k8s.io/v1alpha2
kind: RuntimeExtensionProvider
metadata:
  name: k3s-suc
  namespace: capi-k3s-suc-extension
spec:
  version: v0.1.0
  fetchConfig:
    url: https://github.com/nicolas2bonfils/cluster-api-k3s-suc-extension/releases/latest/runtime-extension-components.yaml
```

Or `kubectl apply -f runtime-extension-components.yaml` from a release.

## Build

```sh
go test ./...
podman build -t <registry>/cluster-api-k3s-suc-extension:<tag> .
```

## Test against a real cluster

`internal/inplace/realcluster_test.go` (build tag `realcluster`) runs both
hooks against a single-node k3s with SUC installed, faking only the
management cluster. Run on 2026-10-14 against a Debian 13 VM, k3s installed by
`get.k3s.io` under systemd (as cluster-api-k3s does) with Rancher's
`system-upgrade-controller` chart 110.0.0: v1.36.5+k3s1 → v1.37.1+k3s1 in
30 s; the API was unreachable for about 10 s and reported as progress; a
witness pod kept its container (same ID, no restart); the Plan was deleted.

```sh
WORKLOAD_KUBECONFIG=... TARGET_VERSION=v1.37.1+k3s1 \
  go test -tags realcluster -run TestRealCluster -v -timeout 20m ./internal/inplace
```
