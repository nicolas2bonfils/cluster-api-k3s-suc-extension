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

## Build and deploy

```sh
go test ./...
podman build -t <registry>/cluster-api-k3s-suc-extension:<tag> .
kubectl apply -f config/extension.yaml   # set the image first
```

Pushing a `v*` tag builds and publishes `ghcr.io/<owner>/<repo>:<tag>`
(`.github/workflows/release.yaml`).
