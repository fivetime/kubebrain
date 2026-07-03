#!/usr/bin/env bash
# k3s-load-smoke.sh — drive a REAL k3s cluster (WITH agent, so pods actually
# schedule) backed by KubeBrain, put churn/scheduling load on it, and kill the
# KubeBrain leader mid-load to validate failover. Companion to
# k3s-datastore-smoke.sh, which runs control-plane-only (--disable-agent).
#
# Validates, end to end on the KubeBrain data plane:
#   - real pod scheduling (kubelet node Ready, Deployment pods Running)
#   - Service EndpointSlices populated (endpoint controller writes)
#   - events created (lease-TTL path) and Deployment->ReplicaSet reconcile
#   - configmap churn (create/patch/delete) uninterrupted
#   - KubeBrain leader kill mid-load: cluster keeps serving, node stays Ready,
#     and the kube-apiserver watch cache never stalls
#     ("Too large resource version" / "Unable to sync caches" stay 0 — the
#      progress-notify fix under load).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT_DIR"

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
K3S_BIN="${K3S_BIN:-k3s}"
DATA_DIR="${DATA_DIR:-/tmp/k3s-kubebrain-load}"
KUBECONFIG_FILE="${KUBECONFIG_FILE:-/tmp/k3s-kubebrain-load.yaml}"
LOG_FILE="${LOG_FILE:-/tmp/k3s-kubebrain-load.log}"
HTTPS_PORT="${HTTPS_PORT:-16453}"
LB_PORT="${LB_PORT:-16454}"
SKIP_TLS="${SKIP_TLS:-true}"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
KUBEBRAIN_DEPLOYMENT="${KUBEBRAIN_DEPLOYMENT:-kubebrain}"
CLEAN_K3S_DATASTORE="${CLEAN_K3S_DATASTORE:-true}"

SMOKE_NS="${SMOKE_NS:-load-smoke}"
REPLICAS="${REPLICAS:-30}"           # pods to schedule (keep < node max-pods 110)
CHURN_ROUNDS="${CHURN_ROUNDS:-5}"    # configmap create/patch/delete rounds
CHURN_OBJECTS="${CHURN_OBJECTS:-15}" # configmaps per round
DO_FAILOVER="${DO_FAILOVER:-true}"   # kill the KubeBrain leader mid-load

K3S_PID=""
trap 'status=$?; if [ -n "$K3S_PID" ] && kill -0 "$K3S_PID" 2>/dev/null; then kill "$K3S_PID" 2>/dev/null || true; wait "$K3S_PID" 2>/dev/null || true; fi; exit "$status"' EXIT

k() { KUBECONFIG="$KUBECONFIG_FILE" kubectl --insecure-skip-tls-verify="$SKIP_TLS" --request-timeout=20s "$@"; }
# kb() targets the cluster that RUNS KubeBrain (the kind cluster = the caller's
# default kubeconfig), NOT the k3s cluster under test.
kb() { kubectl -n "$KUBEBRAIN_NAMESPACE" "$@"; }

log() { echo "[k3s-load-smoke] $*"; }
fail() { echo "[k3s-load-smoke] FAIL: $*" >&2; exit 1; }

clean_datastore() {
  [ "$CLEAN_K3S_DATASTORE" = "true" ] || return 0
  log "clearing stale k3s data from KubeBrain (/bootstrap + /registry per-prefix)"
  env ENDPOINT="$ENDPOINT" PREFIX="/bootstrap" ACTION=delete TIMEOUT=30s go run ./hack/backup/cmd/prefix-tool >/dev/null 2>&1 || true
  # Per-resource prefixes (NOT one big /registry delete: a huge DeleteRange is a
  # single oversized TiKV txn and fails).
  local p
  for p in apiregistration.k8s.io/apiservices clusterrolebindings clusterroles \
    configmaps controllerrevisions csidrivers csinodes endpointslices events \
    flowschemas leases masterleases minions namespaces peerserverleases \
    priorityclasses replicasets deployments pods prioritylevelconfigurations \
    ranges rolebindings roles secrets serviceaccounts services health; do
    env ENDPOINT="$ENDPOINT" PREFIX="/registry/$p" ACTION=delete TIMEOUT=60s go run ./hack/backup/cmd/prefix-tool >/dev/null 2>&1 || true
  done
}

start_k3s() {
  rm -f "$KUBECONFIG_FILE" "$LOG_FILE"
  log "starting k3s WITH agent (datastore=http://${ENDPOINT}, data-dir=${DATA_DIR})"
  "$K3S_BIN" server \
    --datastore-endpoint="http://${ENDPOINT}" \
    --data-dir="$DATA_DIR" \
    --write-kubeconfig="$KUBECONFIG_FILE" \
    --write-kubeconfig-mode=0644 \
    --https-listen-port="$HTTPS_PORT" \
    --disable=traefik \
    --disable=servicelb \
    --disable=metrics-server \
    --disable-cloud-controller \
    --disable-network-policy \
    >"$LOG_FILE" 2>&1 &
  K3S_PID="$!"
}

wait_ready() {
  local what="$1" ; shift
  local deadline=$((SECONDS + ${TIMEOUT_S:-180}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    if [ -n "$K3S_PID" ] && ! kill -0 "$K3S_PID" 2>/dev/null; then tail -30 "$LOG_FILE" >&2 || true; fail "k3s exited while waiting for $what"; fi
    sleep 2
  done
  tail -30 "$LOG_FILE" >&2 || true
  fail "timed out waiting for $what"
}

kb_leader() {
  local p
  for p in $(kb get pods -o name 2>/dev/null | grep "${KUBEBRAIN_DEPLOYMENT}-"); do
    if kb logs "$p" 2>/dev/null | grep -q "start leading"; then echo "${p##*/}"; fi
  done | tail -1
}

stall_errors() {
  grep -c 'Too large resource version' "$LOG_FILE" 2>/dev/null || echo 0
}
sync_errors() {
  grep -c 'Unable to sync caches' "$LOG_FILE" 2>/dev/null || echo 0
}

main() {
  need_bins
  clean_datastore
  start_k3s
  wait_ready "apiserver /readyz" k get --raw=/readyz
  TIMEOUT_S=180 wait_ready "node Ready" bash -c 'KUBECONFIG='"$KUBECONFIG_FILE"' kubectl --insecure-skip-tls-verify='"$SKIP_TLS"' get nodes --no-headers 2>/dev/null | grep -q " Ready "'
  log "node Ready:"; k get nodes --no-headers | sed 's/^/  /'

  # ---- load: deployment + service ----
  k create namespace "$SMOKE_NS" >/dev/null
  k -n "$SMOKE_NS" create deployment web --image=nginx --replicas="$REPLICAS" >/dev/null
  k -n "$SMOKE_NS" expose deployment web --port=80 >/dev/null
  log "waiting for $REPLICAS pods to schedule + run"
  TIMEOUT_S=240 wait_ready "$REPLICAS pods Running" bash -c "KUBECONFIG=$KUBECONFIG_FILE kubectl --insecure-skip-tls-verify=$SKIP_TLS -n $SMOKE_NS get pods --no-headers 2>/dev/null | grep -c Running | grep -qx $REPLICAS"
  local rs eps evs
  rs=$(k -n "$SMOKE_NS" get rs --no-headers 2>/dev/null | wc -l)
  [ "$rs" -ge 1 ] || fail "deployment controller did not create a ReplicaSet"
  eps=$(k -n "$SMOKE_NS" get endpointslices -o jsonpath='{.items[*].endpoints[*].addresses[0]}' 2>/dev/null | wc -w)
  [ "$eps" -ge "$REPLICAS" ] || fail "endpointslices under-populated ($eps < $REPLICAS)"
  evs=$(k -n "$SMOKE_NS" get events --no-headers 2>/dev/null | wc -l)
  [ "$evs" -ge 1 ] || fail "no events recorded (lease-TTL path)"
  log "scheduled: pods=$REPLICAS rs=$rs endpoints=$eps events=$evs"

  # ---- churn + optional failover ----
  if [ "$DO_FAILOVER" = "true" ]; then
    local leader; leader="$(kb_leader)"
    [ -n "$leader" ] || fail "could not find KubeBrain leader"
    log "killing KubeBrain leader $leader mid-churn"
    kb delete pod "$leader" --wait=false >/dev/null 2>&1 || true
  fi
  local r i
  for r in $(seq 1 "$CHURN_ROUNDS"); do
    for i in $(seq 1 "$CHURN_OBJECTS"); do k -n "$SMOKE_NS" create configmap "cm-$r-$i" --from-literal=k=v >/dev/null 2>&1 || true; done
    for i in $(seq 1 "$CHURN_OBJECTS"); do k -n "$SMOKE_NS" patch configmap "cm-$r-$i" --type merge -p '{"data":{"k":"v2"}}' >/dev/null 2>&1 || true; done
    for i in $(seq 1 "$CHURN_OBJECTS"); do k -n "$SMOKE_NS" delete configmap "cm-$r-$i" --wait=false >/dev/null 2>&1 || true; done
    log "churn round $r/$CHURN_ROUNDS"
  done

  # ---- post checks ----
  if [ "$DO_FAILOVER" = "true" ]; then
    local newleader="" deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do newleader="$(kb_leader)"; [ -n "$newleader" ] && break; sleep 3; done
    [ -n "$newleader" ] || fail "no KubeBrain leader re-elected after kill"
    log "new KubeBrain leader: $newleader"
  fi
  k -n "$SMOKE_NS" create configmap post-load --from-literal=ok=yes >/dev/null || fail "write after load/failover failed"
  [ "$(k -n "$SMOKE_NS" get configmap post-load -o jsonpath='{.data.ok}')" = "yes" ] || fail "read-after-write failed"
  k get nodes --no-headers | grep -q " Ready " || fail "node not Ready after failover"
  local tlrv sync
  tlrv="$(stall_errors)"; sync="$(sync_errors)"
  [ "$tlrv" = "0" ] || fail "apiserver watch cache stalled: 'Too large resource version' x$tlrv"
  [ "$sync" = "0" ] || fail "controller caches failed to sync x$sync"

  log "PASS: real scheduling + endpoints + events + churn$([ "$DO_FAILOVER" = "true" ] && echo ' + leader failover') held; watch-cache stalls=0 sync-failures=0"
}

need_bins() {
  command -v "$K3S_BIN" >/dev/null 2>&1 || fail "k3s not found ($K3S_BIN)"
  command -v kubectl >/dev/null 2>&1 || fail "kubectl not found"
  command -v go >/dev/null 2>&1 || fail "go not found (needed for prefix-tool datastore clean)"
}

main "$@"
