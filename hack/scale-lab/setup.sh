#!/usr/bin/env bash
# One-command KWOK scale-lab bring-up for KubeBrain.
#
# Codifies the recipe validated at 33M keys / 8.8M pods (see docs/methodology.md
# and the campaign findings). Phases are independent and idempotent-ish so you
# can re-run any one after a reboot.
#
#   cp lab.env.example lab.env && $EDITOR lab.env
#   ./setup.sh all           # storage -> kubebrain -> kwok -> controlplane -> tools
#   ./setup.sh status
#   ./setup.sh teardown
#
# Individual phases: storage | kubebrain | kwok | controlplane | tools
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
ENVF="${LAB_ENV:-$HERE/lab.env}"
[[ -f "$ENVF" ]] || { echo "missing $ENVF — cp lab.env.example lab.env and edit it"; exit 1; }
# shellcheck disable=SC1090
source "$ENVF"

log() { echo -e "\033[1;36m[scale-lab]\033[0m $*"; }
rsh() { ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "root@$1" "${@:2}"; }

# ---------------------------------------------------------------------------
phase_storage() {
  log "storage: deploying PD+TiKV via tiup on $STORE_HOST (cluster=$TIUP_CLUSTER)"
  command -v tiup >/dev/null || { echo "install tiup first: curl --proto '=https' -tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh"; exit 1; }
  local topo="$HERE/config/tikv-topology.yaml"
  # Substitute hosts from lab.env into a temp topology.
  local tmp; tmp="$(mktemp)"
  sed -e "s/10\.224\.0\.13/$STORE_HOST/g" -e "s/10\.224\.0\.12/$CTRL_HOST/g" \
      -e "s/max-replicas: [0-9]*/max-replicas: $TIKV_MAX_REPLICAS/" "$topo" > "$tmp"
  if tiup cluster list 2>/dev/null | grep -q "^$TIUP_CLUSTER\b"; then
    log "storage: cluster $TIUP_CLUSTER already exists; skipping deploy (use 'tiup cluster destroy' to reset)"
  else
    tiup cluster deploy "$TIUP_CLUSTER" v8.5.3 "$tmp" --user root -y
  fi
  tiup cluster start "$TIUP_CLUSTER"
  rm -f "$tmp"
  log "storage: up. NOTE bare PD+TiKV has no GC safepoint pusher — KubeBrain leader"
  log "         self-pushes it (PR #16); no TiDB instance required."
}

# ---------------------------------------------------------------------------
phase_kubebrain() {
  log "kubebrain: building from $REPO and deploying to $BRAIN_HOST + replicas"
  ( cd "$REPO" && go build -o /tmp/kube-brain ./cmd )
  local pd="$STORE_HOST:2379"
  for h in "$BRAIN_HOST" ${BRAIN_REPLICAS:-}; do
    log "kubebrain: -> $h"
    scp -q /tmp/kube-brain "root@$h:$KB_BIN"
    rsh "$h" "chmod +x $KB_BIN; cat > /etc/systemd/system/kubebrain.service <<EOF
[Unit]
Description=KubeBrain (scale-lab)
After=network-online.target
[Service]
ExecStart=$KB_BIN --port=$KB_CLIENT_PORT --peer-port=$KB_PEER_PORT --info-port=$KB_INFO_PORT \\
  --pd-addrs=$pd --compatible-with-etcd=true \\
  --enable-count-index=true --count-index-max-keys=$KB_COUNT_INDEX_MAX \\
  --enable-storage-metrics=true --watch-progress-notify-interval=100ms \\
  --watch-cache-size=$KB_WATCH_CACHE --v=1
Restart=always
RestartSec=2
LimitNOFILE=4194304
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload && systemctl enable --now kubebrain"
  done
  log "kubebrain: waiting for a leader..."
  until rsh "$BRAIN_HOST" "curl -s -m2 http://localhost:$KB_INFO_PORT/status" 2>/dev/null | grep -q Revision; do sleep 2; done
  log "kubebrain: leader up on $BRAIN_HOST:$KB_CLIENT_PORT"
}

# ---------------------------------------------------------------------------
phase_kwok() {
  log "kwok: installing kwok controller on $CTRL_HOST ($KWOK_SHARDS shards)"
  rsh "$CTRL_HOST" "test -x /usr/local/bin/kwok || { echo 'install kwok: https://kwok.sigs.k8s.io/ (place /usr/local/bin/kwok)'; exit 1; }"
  scp -q "$HERE/config/kwok-stages-fast.yaml" "root@$CTRL_HOST:/etc/kwok-stages.yaml"
  # kwok@ template unit; enumerate shards explicitly (systemctl wildcards no-op for templates).
  rsh "$CTRL_HOST" "cat > /etc/systemd/system/kwok@.service <<EOF
[Unit]
Description=KWOK shard %i
After=network-online.target
[Service]
ExecStart=/usr/local/bin/kwok --kubeconfig=$KUBECONFIG_OUT --manage-all-nodes=false \\
  --manage-nodes-with-label-selector=kwok-shard=%i \\
  --node-lease-duration-seconds=$KWOK_NODE_LEASE_SECONDS --config=/etc/kwok-stages.yaml
Restart=always
RestartSec=2
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload"
  for i in $(seq 0 $((KWOK_SHARDS-1))); do rsh "$CTRL_HOST" "systemctl enable --now kwok@$i"; done
  log "kwok: $KWOK_SHARDS shards started on $CTRL_HOST"
}

# ---------------------------------------------------------------------------
phase_controlplane() {
  if [[ "$CONTROLPLANE" == "k3s" ]]; then
    log "controlplane: k3s server on $BRAIN_HOST pointing at KubeBrain (QPS tuned)"
    rsh "$BRAIN_HOST" "curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC=\"server \
      --datastore-endpoint=http://$BRAIN_HOST:$KB_CLIENT_PORT \
      --disable-cloud-controller --disable=traefik,servicelb,metrics-server,local-storage \
      --kube-controller-manager-arg=kube-api-qps=800 --kube-controller-manager-arg=kube-api-burst=1600 \
      --kube-controller-manager-arg=concurrent-deployment-syncs=50 --kube-controller-manager-arg=concurrent-replicaset-syncs=50 \
      --kube-scheduler-arg=kube-api-qps=800 --kube-scheduler-arg=kube-api-burst=1600 \
      --kube-apiserver-arg=etcd-count-metric-poll-period=0\" sh -"
    mkdir -p "$(dirname "$KUBECONFIG_OUT")"
    rsh "$BRAIN_HOST" "cat /etc/rancher/k3s/k3s.yaml" | sed "s/127.0.0.1/$BRAIN_HOST/" > "$KUBECONFIG_OUT"
    log "controlplane: k3s up; kubeconfig -> $KUBECONFIG_OUT"
  else
    log "controlplane: bare apiserver/kcm/scheduler is machine-specific (self-signed PKI,"
    log "  admission disables, cache flags). See docs/methodology.md §13.2 and the campaign"
    log "  findings; the tuned flags are in docs/scale-lab-10m-report.md."
  fi
}

# ---------------------------------------------------------------------------
phase_tools() {
  log "tools: building loadgen + probes into $HERE/bin/"
  mkdir -p "$HERE/bin"
  ( cd "$HERE/loadgen" && go build -o "$HERE/bin/loadgen" . )
  for p in bulk foload elogprobe qlat; do
    ( cd "$REPO" && go build -o "$HERE/bin/$p" "./hack/scale-lab/probes/$p" )
  done
  log "tools: built -> $(ls "$HERE/bin" | tr '\n' ' ')"
  cat <<EOF

Run examples:
  bin/loadgen -kubeconfig $KUBECONFIG_OUT -mode nodes    -count 1000
  bin/loadgen -kubeconfig $KUBECONFIG_OUT -mode workload -ns 100 -deploy 10 -replicas 10 -qps 800
  bin/loadgen -kubeconfig $KUBECONFIG_OUT -mode status
  bin/qlat      -endpoint $BRAIN_HOST:$KB_CLIENT_PORT          # write-latency (single vs 3-replica, #53)
  bin/elogprobe -endpoint $BRAIN_HOST:$KB_CLIENT_PORT          # event-log replay beyond ring (#52)
  bin/foload    -endpoint $BRAIN_HOST:$KB_CLIENT_PORT          # failover-under-load gap detection (#46)
EOF
}

# ---------------------------------------------------------------------------
phase_status() {
  log "status:"
  echo -n "  KubeBrain leader ($BRAIN_HOST): "; rsh "$BRAIN_HOST" "curl -s -m2 http://localhost:$KB_INFO_PORT/status" 2>/dev/null || echo unreachable; echo
  for h in ${BRAIN_REPLICAS:-}; do echo -n "  KB replica ($h): "; rsh "$h" "systemctl is-active kubebrain" 2>/dev/null; done
  echo -n "  TiKV/PD ($STORE_HOST): "; rsh "$STORE_HOST" "ss -tlnp 2>/dev/null | grep -oE ':(2379|20160)' | sort -u | tr '\n' ' '"; echo
  echo -n "  KWOK ($CTRL_HOST): "; rsh "$CTRL_HOST" "systemctl is-active kwok@0 2>/dev/null"; echo
}

phase_teardown() {
  log "teardown: stopping KubeBrain / KWOK (storage left to 'tiup cluster destroy $TIUP_CLUSTER')"
  for h in "$BRAIN_HOST" ${BRAIN_REPLICAS:-}; do rsh "$h" "systemctl disable --now kubebrain 2>/dev/null" || true; done
  for i in $(seq 0 $((KWOK_SHARDS-1))); do rsh "$CTRL_HOST" "systemctl disable --now kwok@$i 2>/dev/null" || true; done
  log "teardown: done. Storage kept (destroy explicitly): tiup cluster destroy $TIUP_CLUSTER"
}

case "${1:-all}" in
  storage)      phase_storage ;;
  kubebrain)    phase_kubebrain ;;
  kwok)         phase_kwok ;;
  controlplane) phase_controlplane ;;
  tools)        phase_tools ;;
  status)       phase_status ;;
  teardown)     phase_teardown ;;
  all)          phase_storage; phase_kubebrain; phase_controlplane; phase_kwok; phase_tools; phase_status ;;
  *) echo "usage: $0 {all|storage|kubebrain|kwok|controlplane|tools|status|teardown}"; exit 1 ;;
esac
