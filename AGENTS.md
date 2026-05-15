# AGENTS.md

This file provides guidance to coding agents (e.g. Claude Code, claude.ai/code) when working with code in this repository.

## Repository purpose

Go module `kubeops.dev/petset` — a **fork of the Kubernetes StatefulSet controller as a CRD-based operator**, with a flexible per-shard pod placement policy. Defines two CRDs under `apps.k8s.appscode.com`:

- `PetSet` — like `apps/v1.StatefulSet` but as a CRD this operator reconciles.
- `PlacementPolicy` — declarative pod placement rules consumed when the controller spawns PetSet pods. Sample: `hack/samples/placementpolicy.yaml`.

The produced binary is `petset`.

This started life as a direct fork of `k8s.io/kubernetes/pkg/controller/statefulset` — much of `pkg/controller/petset/`, `pkg/controller/history/`, and `pkg/controller/controller_ref_manager*` mirror the upstream layout. **Stay close to upstream**; AppsCode additions live in `pkg/controller/placement.go` and the new `PlacementPolicy` CRD wiring.

## Architecture

- `cmd/petset/` — entry point.
- `pkg/cmds/` — Cobra root + run.
- `apis/apps/v1/` — Kubebuilder API types (`PetSet`, `PlacementPolicy`), `register.go`, `install/`, `fuzzer/`, generated `zz_generated.*.go`.
- `client/` — generated typed clientset.
- `crds/` — generated CRD YAMLs (`apps.k8s.appscode.com_petsets.yaml`, `apps.k8s.appscode.com_placementpolicies.yaml`) + `lib.go`.
- `pkg/controller/`:
  - `petset/` — main reconciler (mirrors `statefulset_controller.go` upstream).
  - `history/` — ControllerRevision history (also mirrors upstream).
  - `controller_ref_manager.go` / `controller_utils.go` — upstream-derived helpers.
  - `placement.go` — **the AppsCode addition**: applies `PlacementPolicy` rules when scheduling pods.
  - `tests/` — controller tests.
- `pkg/webhooks/apps/` — admission webhooks for `PetSet` and `PlacementPolicy`.
- `pkg/features/` — feature gates.
- `pkg/securitycontext/` — security-context helpers used by the controller.
- `pkg/util/` — shared utilities.
- `pkg/api/` — internal API conversion helpers.
- `Dockerfile.in` (PROD, distroless), `Dockerfile.dbg` (debian), `Dockerfile.ubi` (Red Hat certified) — three image variants.
- `hack/`, `Makefile` — AppsCode build harness.
- `vendor/` — checked-in deps.

CRD API group is `apps.k8s.appscode.com` (mirroring upstream `apps` group name but under AppsCode's domain to avoid collision with vanilla StatefulSet).

## Common commands

All Make targets run inside `ghcr.io/appscode/golang-dev` — Docker must be running.

- `make ci` — CI pipeline.
- `make build` / `make all-build` — build host or all-platform binaries.
- `make gen` — regenerate clientset + manifests. Run after changes to `apis/apps/v1/*_types.go`.
- `make manifests` — regenerate CRDs only.
- `make clientset` — regenerate `client/` only.
- `make fmt`, `make lint`, `make unit-tests` / `make test` — standard.
- `make verify` — `verify-gen verify-modules`; `go mod tidy && go mod vendor` must leave the tree clean.
- `make container` — build PROD, DBG, and UBI images.
- `make push` — push all three; `make docker-manifest` writes multi-arch manifests; `make release` is the full publish flow.
- `make push-to-kind` / `make deploy-to-kind` — load into Kind and Helm-install.
- `make install` / `make uninstall` / `make purge` — Helm install lifecycle.
- `make add-license` / `make check-license` — manage license headers.

Run a single Go test (requires a local Go toolchain):

```
go test ./pkg/controller/petset/... -run TestName -v
```

## Conventions

- Module path is `kubeops.dev/petset` (vanity URL). Imports must use that.
- License: Apache-2.0 (`LICENSE`); new files need the standard AppsCode header (`make add-license`).
- Sign off commits (`git commit -s`); contributions follow the DCO.
- Vendor directory is checked in — `go mod tidy && go mod vendor` must leave the tree clean (enforced by `verify-modules`).
- **Stay close to upstream `pkg/controller/statefulset`**: when bumping the kube vendor, rebase the `pkg/controller/petset/` and `pkg/controller/history/` packages against upstream changes. AppsCode-specific code belongs in `placement.go` and the API types under `apis/apps/v1/`.
- Do not hand-edit `zz_generated.*.go`, anything under `client/`, or `crds/` — change `apis/apps/v1/*_types.go` and re-run `make gen`.
- API group is `apps.k8s.appscode.com` (deliberately not the upstream `apps`) — don't rename without a coordinated migration.
- Three Dockerfiles, one binary — keep `Dockerfile.in`, `Dockerfile.dbg`, and `Dockerfile.ubi` in sync.
