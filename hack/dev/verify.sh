#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain:dev}"
RUN_GO_TEST="${RUN_GO_TEST:-true}"
RUN_BASIC_SMOKE="${RUN_BASIC_SMOKE:-true}"
RUN_ETCD_CLIENT_COMPAT="${RUN_ETCD_CLIENT_COMPAT:-false}"
RUN_TIKV_PERSISTENCE_SMOKE="${RUN_TIKV_PERSISTENCE_SMOKE:-false}"
RUN_RESTART_PERSISTENCE_SMOKE="${RUN_RESTART_PERSISTENCE_SMOKE:-false}"
RUN_K3S_DATASTORE_SMOKE="${RUN_K3S_DATASTORE_SMOKE:-false}"
RUN_HA_SMOKE="${RUN_HA_SMOKE:-true}"
RUN_INCLUSTER_BALANCER_SMOKE="${RUN_INCLUSTER_BALANCER_SMOKE:-false}"
RUN_APISERVER_SMOKE="${RUN_APISERVER_SMOKE:-true}"
RUN_INCLUSTER_APISERVER_SMOKE="${RUN_INCLUSTER_APISERVER_SMOKE:-false}"
RUN_TLS_SMOKE="${RUN_TLS_SMOKE:-true}"
RUN_BACKUP_DRILL="${RUN_BACKUP_DRILL:-false}"
RUN_BACKUP_INTEGRITY_SMOKE="${RUN_BACKUP_INTEGRITY_SMOKE:-false}"
RUN_LEASE_BACKUP_SMOKE="${RUN_LEASE_BACKUP_SMOKE:-false}"
RUN_RESTORE_ROLLBACK_SMOKE="${RUN_RESTORE_ROLLBACK_SMOKE:-false}"
RUN_RESTORE_GUARD_SMOKE="${RUN_RESTORE_GUARD_SMOKE:-false}"
RUN_VERIFY_CONTENT_SMOKE="${RUN_VERIFY_CONTENT_SMOKE:-false}"
RUN_FAULT_SMOKE="${RUN_FAULT_SMOKE:-false}"
RUN_BACKEND_QUORUM_FAULT_SMOKE="${RUN_BACKEND_QUORUM_FAULT_SMOKE:-false}"
RUN_COUNTINDEX_FAILOVER_SMOKE="${RUN_COUNTINDEX_FAILOVER_SMOKE:-false}"
RUN_LEASE_EXPIRY_SMOKE="${RUN_LEASE_EXPIRY_SMOKE:-false}"
RUN_LEASE_FAULT_SMOKE="${RUN_LEASE_FAULT_SMOKE:-false}"
RUN_LEASE_RENEWAL_FAILOVER_SMOKE="${RUN_LEASE_RENEWAL_FAILOVER_SMOKE:-false}"
RUN_WATCH_SOAK="${RUN_WATCH_SOAK:-false}"
RUN_LOAD_SMOKE="${RUN_LOAD_SMOKE:-false}"
RUN_INCLUSTER_LOAD_SMOKE="${RUN_INCLUSTER_LOAD_SMOKE:-false}"
RUN_COMPACT_SOAK="${RUN_COMPACT_SOAK:-false}"
RUN_COMPACT_FAULT_SMOKE="${RUN_COMPACT_FAULT_SMOKE:-false}"
RUN_ROLLOUT_SMOKE="${RUN_ROLLOUT_SMOKE:-false}"
RUN_INCLUSTER_ROLLOUT_SMOKE="${RUN_INCLUSTER_ROLLOUT_SMOKE:-false}"
RUN_APISERVER_ROLLOUT_SMOKE="${RUN_APISERVER_ROLLOUT_SMOKE:-false}"
RUN_INCLUSTER_APISERVER_ROLLOUT_SMOKE="${RUN_INCLUSTER_APISERVER_ROLLOUT_SMOKE:-false}"
RUN_APISERVER_WATCH_SOAK="${RUN_APISERVER_WATCH_SOAK:-false}"
RUN_INCLUSTER_APISERVER_WATCH_SOAK="${RUN_INCLUSTER_APISERVER_WATCH_SOAK:-false}"
RUN_APISERVER_VERSION_MATRIX="${RUN_APISERVER_VERSION_MATRIX:-false}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

run_step() {
  local name="$1"
  shift

  echo
  echo "==> ${name}"
  "$@"
}

need go
need kubectl

cd "$ROOT_DIR"

if [ "$RUN_GO_TEST" = "true" ]; then
  run_step "go test ./..." go test ./...
fi

if [ "$RUN_BASIC_SMOKE" = "true" ]; then
  run_step "basic etcd client smoke" env ENDPOINT="$ENDPOINT" hack/dev/smoke-etcd-client.sh
fi

if [ "$RUN_ETCD_CLIENT_COMPAT" = "true" ]; then
  run_step "official etcd Kubernetes client compatibility" env ENDPOINT="$ENDPOINT" hack/etcd-client-compat/run.sh
fi

if [ "$RUN_TIKV_PERSISTENCE_SMOKE" = "true" ]; then
  run_step "TiKV persistence smoke" env ENDPOINT="kubebrain.kubebrain-dev.svc:3379" hack/dev/tikv-persistence-smoke.sh
fi

if [ "$RUN_RESTART_PERSISTENCE_SMOKE" = "true" ]; then
  run_step "replicated restart persistence smoke" env ENDPOINT="$ENDPOINT" hack/dev/restart-persistence-smoke.sh
fi

if [ "$RUN_K3S_DATASTORE_SMOKE" = "true" ]; then
  run_step "k3s datastore smoke" env ENDPOINT="$ENDPOINT" hack/dev/k3s-datastore-smoke.sh
fi

if [ "$RUN_HA_SMOKE" = "true" ]; then
  run_step "plain HA smoke" env ENDPOINT="$ENDPOINT" hack/dev/ha-smoke.sh
fi

if [ "$RUN_INCLUSTER_BALANCER_SMOKE" = "true" ]; then
  run_step "in-cluster client balancer smoke" hack/dev/incluster-balancer-smoke.sh
fi

if [ "$RUN_APISERVER_SMOKE" = "true" ]; then
  run_step "standalone kube-apiserver smoke" env ENDPOINT="http://${ENDPOINT}" hack/dev/apiserver-smoke.sh
fi

if [ "$RUN_INCLUSTER_APISERVER_SMOKE" = "true" ]; then
  run_step "in-cluster kube-apiserver smoke" hack/dev/incluster-apiserver-smoke.sh
fi

if [ "$RUN_TLS_SMOKE" = "true" ]; then
  run_step "TLS HA smoke" env IMAGE_NAME="$IMAGE_NAME" hack/dev/tls-smoke.sh
fi

if [ "$RUN_BACKUP_DRILL" = "true" ]; then
  run_step "logical backup restore drill" env ENDPOINT="$ENDPOINT" hack/backup/logical-drill.sh
fi

if [ "$RUN_BACKUP_INTEGRITY_SMOKE" = "true" ]; then
  run_step "logical backup integrity smoke" env ENDPOINT="$ENDPOINT" hack/backup/backup-integrity-smoke.sh
fi

if [ "$RUN_LEASE_BACKUP_SMOKE" = "true" ]; then
  run_step "lease-aware logical backup restore smoke" env ENDPOINT="$ENDPOINT" hack/backup/lease-restore-smoke.sh
fi

if [ "$RUN_RESTORE_ROLLBACK_SMOKE" = "true" ]; then
  run_step "logical restore rollback smoke" env ENDPOINT="$ENDPOINT" hack/backup/restore-rollback-smoke.sh
fi

if [ "$RUN_RESTORE_GUARD_SMOKE" = "true" ]; then
  run_step "logical restore overwrite guard smoke" env ENDPOINT="$ENDPOINT" hack/backup/restore-guard-smoke.sh
fi

if [ "$RUN_VERIFY_CONTENT_SMOKE" = "true" ]; then
  run_step "logical verify content smoke" env ENDPOINT="$ENDPOINT" hack/backup/verify-content-smoke.sh
fi

if [ "$RUN_FAULT_SMOKE" = "true" ]; then
  run_step "fault smoke" env ENDPOINT="$ENDPOINT" hack/dev/fault-smoke.sh
fi

if [ "$RUN_BACKEND_QUORUM_FAULT_SMOKE" = "true" ]; then
  run_step "backend quorum fault smoke" env ENDPOINT="$ENDPOINT" hack/dev/backend-quorum-fault-smoke.sh
fi

if [ "$RUN_COUNTINDEX_FAILOVER_SMOKE" = "true" ]; then
  run_step "count index failover smoke" env ENDPOINT="$ENDPOINT" hack/dev/countindex-failover-smoke.sh
fi

if [ "$RUN_LEASE_EXPIRY_SMOKE" = "true" ]; then
  run_step "lease expiry smoke" env ENDPOINT="$ENDPOINT" hack/dev/lease-expiry-smoke.sh
fi

if [ "$RUN_LEASE_FAULT_SMOKE" = "true" ]; then
  run_step "lease fault smoke" env ENDPOINT="$ENDPOINT" hack/dev/lease-fault-smoke.sh
fi

if [ "$RUN_LEASE_RENEWAL_FAILOVER_SMOKE" = "true" ]; then
  run_step "lease renewal failover smoke" env ENDPOINT="$ENDPOINT" hack/dev/lease-renewal-failover-smoke.sh
fi

if [ "$RUN_WATCH_SOAK" = "true" ]; then
  run_step "watch soak" env ENDPOINT="$ENDPOINT" hack/dev/watch-soak.sh
fi

if [ "$RUN_LOAD_SMOKE" = "true" ]; then
  run_step "load smoke" env ENDPOINT="$ENDPOINT" hack/dev/load-smoke.sh
fi

if [ "$RUN_INCLUSTER_LOAD_SMOKE" = "true" ]; then
  run_step "in-cluster load smoke" hack/dev/incluster-load-smoke.sh
fi

if [ "$RUN_COMPACT_SOAK" = "true" ]; then
  run_step "compact soak" env ENDPOINT="$ENDPOINT" hack/dev/compact-soak.sh
fi

if [ "$RUN_COMPACT_FAULT_SMOKE" = "true" ]; then
  run_step "compact fault smoke" env ENDPOINT="$ENDPOINT" hack/dev/compact-fault-smoke.sh
fi

if [ "$RUN_ROLLOUT_SMOKE" = "true" ]; then
  run_step "rollout smoke" env ENDPOINT="$ENDPOINT" hack/dev/rollout-smoke.sh
fi

if [ "$RUN_INCLUSTER_ROLLOUT_SMOKE" = "true" ]; then
  run_step "in-cluster rollout smoke" hack/dev/incluster-rollout-smoke.sh
fi

if [ "$RUN_APISERVER_ROLLOUT_SMOKE" = "true" ]; then
  run_step "apiserver rollout smoke" env ENDPOINT="http://${ENDPOINT}" hack/dev/apiserver-rollout-smoke.sh
fi

if [ "$RUN_INCLUSTER_APISERVER_ROLLOUT_SMOKE" = "true" ]; then
  run_step "in-cluster kube-apiserver rollout smoke" hack/dev/incluster-apiserver-rollout-smoke.sh
fi

if [ "$RUN_APISERVER_WATCH_SOAK" = "true" ]; then
  run_step "apiserver watch soak" env ENDPOINT="http://${ENDPOINT}" hack/dev/apiserver-watch-soak.sh
fi

if [ "$RUN_INCLUSTER_APISERVER_WATCH_SOAK" = "true" ]; then
  run_step "in-cluster kube-apiserver watch soak" hack/dev/incluster-apiserver-watch-soak.sh
fi

if [ "$RUN_APISERVER_VERSION_MATRIX" = "true" ]; then
  run_step "apiserver version matrix" env ENDPOINT="http://${ENDPOINT}" hack/dev/apiserver-version-matrix.sh
fi

echo
echo "All requested verification steps completed"
