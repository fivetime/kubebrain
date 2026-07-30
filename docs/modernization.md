# Project modernization

The repository uses Go 1.26 with the latest supported security patch toolchain.
Kubernetes and etcd dependencies were already current when this work began, so
the modernization deliberately avoids unnecessary application rewrites.

## Build and test

- `make build` builds the default TiKV-backed binary.
- `make badger` builds the optional Badger-backed binary.
- `make test`, `make coverage`, `make lint`, and `make vuln` provide consistent
  local checks.

## Containers

The supported container definition is `build/kubebrain.Dockerfile`. It uses a
multi-stage Go/Alpine build, supports TiKV and Badger build tags, emits a static
binary, and runs as the unprivileged user `65532:65532`.

## Automation

GitHub Actions separately handles build/test, lint and vulnerability checks,
CodeQL, dependency review, integration verification, and multi-architecture
image publishing. Dependabot maintains Go modules, Actions, and base images.

The integration workflow remains scheduled and manually dispatchable because it
creates a real kind/TiKV environment and is intentionally more expensive than
the regular pull-request checks.
