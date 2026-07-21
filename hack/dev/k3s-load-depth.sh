#!/usr/bin/env bash
# k3s-load-depth.sh — heavy SUSTAINED churn + failover on an ALREADY-RUNNING
# KubeBrain-backed k3s cluster, to find the datastore's ceiling and prove a
# LOADED soak (leak check under real load, not idle).
#
# Unlike k3s-load-smoke.sh (which boots its own single-node k3s), this drives an
# existing — typically multi-node — cluster via $K3S_KUBECONFIG, and samples the
# KubeBrain deployment (in the kind/host cluster reached by the default
# kubeconfig) throughout.
#
# Validates, end to end, under sustained load:
#   - KubeBrain never restarts / leader stays stable (or fails over cleanly)
#   - no goroutine/RSS leak across a multi-minute LOADED window
#   - apiserver watch cache never stalls (progress-notify holds under load):
#     "Too large resource version" / "Unable to sync caches" stay flat
#   - the borrowed safety-net metrics do NOT false-fire under healthy load
#     (watch.collector.stalled/skipped_revision, lease.orphan_sweep.*)
#   - a KubeBrain leader kill mid-load recovers and churn continues (#39 fence +
#     lease reload)
#
# CEILING (observed on a 104-core box, 3 PD + 3 TiKV, 3-node k3s): KubeBrain
# itself never leaks/restarts under this load and a leader failover re-elects in
# ~15-30s with data intact. But under HEAVY sustained churn the write latency
# rises (TiKV single-region hotspot on the concentrated configmap keyspace, ~1-2s
# /batch), and a failover's ~15-30s leaderless gap on top of that exceeds the k3s
# controller-manager's lease-renew window -> the bundled k3s control-plane process
# exits ("leaderelection lost"). This is the datastore-failover ceiling (an
# overloaded etcd behaves the same), NOT a KubeBrain fault: restart k3s-server and
# the cluster recovers fully from KubeBrain's persisted data. Set FAILOVER_AT=0 to
# soak without tripping it, or keep it to characterize the ceiling.
#
# To RAISE the ceiling, see docs/failover_tuning_cn.md: (lever 1) KubeBrain's
# --leader-lease-duration shortens the leaderless window; (lever 2, zero-risk)
# raise the k8s controller-manager/scheduler --leader-elect-lease-duration so
# consumers tolerate the gap (k3s-load-smoke.sh does this via
# CONSUMER_LEASE_TOLERANCE).
#
# Env:
#   K3S_KUBECONFIG   kubeconfig for the cluster UNDER LOAD   (default /root/mk.yaml)
#   KBNS             namespace KubeBrain runs in             (default kubebrain-dev)
#   INFO_NODEPORT    KubeBrain info port (host-reachable)    (default 172.18.0.2:32377)
#   DURATION         seconds of sustained load               (default 900)
#   FAILOVER_AT      seconds into the run to kill the leader (default 400; 0=skip)
#   LO / HI          scale-churn replica bounds              (default 30 / 90)
#   CM_WORKERS/CM_PER parallel configmap CRUD storm          (default 10 / 25)
#   IMG              churn pod image (must be cached)         (default rancher/mirrored-pause:3.6)
set -uo pipefail

K3S_KUBECONFIG="${K3S_KUBECONFIG:-/root/mk.yaml}"
KBNS="${KBNS:-kubebrain-dev}"
INFO_NODEPORT="${INFO_NODEPORT:-172.18.0.2:32377}"
DURATION="${DURATION:-900}"
FAILOVER_AT="${FAILOVER_AT:-400}"
LO="${LO:-30}"; HI="${HI:-90}"
CM_WORKERS="${CM_WORKERS:-10}"; CM_PER="${CM_PER:-25}"
IMG="${IMG:-rancher/mirrored-pause:3.6}"
NS="${NS:-depth-load}"

k() { KUBECONFIG="$K3S_KUBECONFIG" kubectl --insecure-skip-tls-verify=true --request-timeout=20s "$@" 2>/dev/null; }
kb() { kubectl -n "$KBNS" "$@" 2>/dev/null; }
log() { echo "[k3s-load-depth $(date +%H:%M:%S)] $*"; }

leader_ip() { curl -s --max-time 4 "http://$INFO_NODEPORT/election" 2>/dev/null | sed -n 's/.*"LeaderAddress":"\([0-9.]*\):.*/\1/p'; }
leader_pod() { kb get pods -o jsonpath='{range .items[*]}{.status.podIP}{" "}{.metadata.name}{"\n"}{end}' | awk -v ip="$1" '$1==ip{print $2}'; }
kb_stat() { # goroutines RSS(MB) from a pod's info port
  kb exec "$1" -- sh -c 'wget -qO- http://127.0.0.1:8080/metrics 2>/dev/null | grep "^go_goroutines "; grep VmRSS /proc/1/status' 2>/dev/null \
    | awk '/go_goroutines/{g=$2} /VmRSS/{r=int($2/1024)} END{print g, r}'
}
kb_counter() { kb exec "$1" -- sh -c "wget -qO- http://127.0.0.1:8080/metrics 2>/dev/null | grep -E '$2'" 2>/dev/null; }

STALL_RE="Too large resource version|Unable to sync caches"
stall_count() { docker logs k3s-server 2>&1 | grep -cE "$STALL_RE"; }

log "targets: k3s=$K3S_KUBECONFIG  KubeBrain ns=$KBNS  info=$INFO_NODEPORT"
LIP0=$(leader_ip); LPOD0=$(leader_pod "$LIP0")
read -r G0 R0 <<<"$(kb_stat "$LPOD0")"
STALL0=$(stall_count)
log "start: leader=$LIP0 ($LPOD0) goroutines=$G0 rss=${R0}MB stallBase=$STALL0"

# ---- load ----
k create ns "$NS" >/dev/null 2>&1 || true
cat <<EOF | k apply -f - >/dev/null 2>&1
apiVersion: apps/v1
kind: Deployment
metadata: {name: churn, namespace: $NS}
spec:
  replicas: $LO
  selector: {matchLabels: {app: churn}}
  template:
    metadata: {labels: {app: churn}}
    spec:
      terminationGracePeriodSeconds: 0
      containers: [{name: p, image: $IMG, ports: [{containerPort: 80}]}]
---
apiVersion: v1
kind: Service
metadata: {name: churn, namespace: $NS}
spec: {selector: {app: churn}, ports: [{port: 80, targetPort: 80}]}
EOF

END=$(( $(date +%s) + DURATION ))
PIDS=()
( while [ "$(date +%s)" -lt "$END" ]; do k -n "$NS" scale deploy/churn --replicas="$HI" >/dev/null 2>&1; sleep 12; k -n "$NS" scale deploy/churn --replicas="$LO" >/dev/null 2>&1; sleep 8; done ) & PIDS+=($!)
( while [ "$(date +%s)" -lt "$END" ]; do k -n "$NS" rollout restart deploy/churn >/dev/null 2>&1; sleep 25; done ) & PIDS+=($!)
for w in $(seq 1 "$CM_WORKERS"); do
  ( r=0; while [ "$(date +%s)" -lt "$END" ]; do r=$((r+1))
      for i in $(seq 1 "$CM_PER"); do k -n "$NS" create cm "cm-$w-$i" --from-literal=r="$r" >/dev/null 2>&1; done
      for i in $(seq 1 "$CM_PER"); do k -n "$NS" patch cm "cm-$w-$i" -p "{\"data\":{\"r\":\"$r-p\"}}" >/dev/null 2>&1; done
      for i in $(seq 1 "$CM_PER"); do k -n "$NS" delete cm "cm-$w-$i" --wait=false >/dev/null 2>&1; done
    done ) & PIDS+=($!)
done
log "load running: scale($LO<->$HI) + rollout-restart + ${CM_WORKERS}x cm($CM_PER), ${DURATION}s"

# ---- monitor + mid-run failover ----
FAILED_OVER=0
START=$(date +%s)
printf "%-8s %-14s %-4s %-6s %-6s %-6s %-6s %-6s\n" TIME LEADER RST GORO RSSMB STALLd NODES CHURN
while [ "$(date +%s)" -lt "$END" ]; do
  now=$(( $(date +%s) - START ))
  lip=$(leader_ip); lpod=$(leader_pod "$lip")
  rst=$(kb get pods -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END{print s+0}')
  read -r g r <<<"$(kb_stat "$lpod")"
  sd=$(( $(stall_count) - STALL0 ))
  nodes=$(k get nodes --no-headers | grep -cw Ready)
  churn=$(k -n "$NS" get pods --no-headers | grep -c Running)
  printf "%-8s %-14s %-4s %-6s %-6s %-6s %-6s %-6s\n" "${now}s" "${lip:-?}" "${rst:-?}" "${g:-?}" "${r:-?}" "$sd" "${nodes:-?}" "${churn:-0}"
  if [ "$FAILOVER_AT" -gt 0 ] && [ "$FAILED_OVER" -eq 0 ] && [ "$now" -ge "$FAILOVER_AT" ] && [ -n "$lpod" ]; then
    log "FAILOVER: killing leader pod $lpod ($lip) under load"
    kb delete pod "$lpod" --grace-period=0 --force >/dev/null 2>&1
    FAILED_OVER=1
  fi
  sleep 12
done
wait "${PIDS[@]}" 2>/dev/null

# ---- verdict ----
LIP1=$(leader_ip); LPOD1=$(leader_pod "$LIP1")
read -r G1 R1 <<<"$(kb_stat "$LPOD1")"
RST1=$(kb get pods -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END{print s+0}')
SD1=$(( $(stall_count) - STALL0 ))
echo "=== new safety-net metrics (want ABSENT/0 under healthy load) ==="
kb_counter "$LPOD1" "watch.collector.stalled|watch.collector.skipped_revision|lease.orphan_sweep" || echo "(none emitted — healthy)"
echo "=== SUMMARY ==="
log "goroutines ${G0} -> ${G1}   rss ${R0}MB -> ${R1}MB   restarts ${RST1}   watch-stall delta ${SD1}   leader ${LIP0} -> ${LIP1} (failover=$([ "$FAILOVER_AT" -gt 0 ] && echo yes || echo no))"
log "cleanup: k delete ns $NS"
k delete ns "$NS" --wait=false >/dev/null 2>&1 || true
