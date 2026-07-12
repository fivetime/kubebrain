#!/usr/bin/env bash
# Bring up a self-contained single-node KubeBrain stack for functional testing:
# KubeBrain (embedded badger backend, no TiKV/PD) + k3s control plane pointed at
# it + KWOK fake nodes. Everything runs on one host — enough for the full
# resource-coverage test, without a multi-machine TiKV cluster.
#
#   ./single-node-stack.sh up        # build + start everything
#   KUBECONFIG=/root/.kube-restest kubectl get nodes
#   ./resource-coverage.sh
#   ./single-node-stack.sh down      # stop + wipe
#
# Prereqs on PATH: go, k3s + kubectl binaries, kwok binary.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
KB_DATA=/data/kubebrain-restest
KUBECONFIG_OUT=/root/.kube-restest
STAGES=/etc/kwok-stages.yaml
log() { echo -e "\033[1;36m[stack]\033[0m $*"; }

up() {
  # KubeBrain is selected at BUILD TIME by tag: -tags badger gives the embedded
  # backend (default/no tag = tikv). Only --data-dir is needed, no PD/TiKV.
  log "building KubeBrain (badger backend)"
  ( cd "$REPO" && go build -tags badger -o /usr/local/bin/kube-brain ./cmd )
  mkdir -p "$KB_DATA"
  cat > /etc/systemd/system/kubebrain.service <<EOF
[Unit]
Description=KubeBrain (single-node functional-test, badger)
After=network-online.target
[Service]
ExecStart=/usr/local/bin/kube-brain --port=3379 --peer-port=3380 --info-port=8080 \\
  --data-dir=$KB_DATA --compatible-with-etcd=true --enable-count-index=true \\
  --watch-progress-notify-interval=100ms --watch-cache-size=200000 --v=1
Restart=always
RestartSec=2
LimitNOFILE=1048576
[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload && systemctl enable --now kubebrain
  until curl -s -m2 http://127.0.0.1:8080/status | grep -q Revision; do sleep 1; done
  log "KubeBrain up"

  # A leftover campaign apiserver on :6444 (kube-apiserver-pods etc.) blocks
  # k3s's supervisor listener — disable any such units first.
  for u in kube-apiserver-pods kube-controller-manager kube-scheduler kube-apiserver; do
    systemctl disable --now "$u" 2>/dev/null || true
  done
  pkill -9 -f 'kube-apiserver' 2>/dev/null || true

  # k3s control plane only (no agent); KWOK provides nodes. datastore-endpoint
  # points straight at KubeBrain over http (k3s treats it as etcd).
  log "installing k3s control plane -> KubeBrain"
  curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_DOWNLOAD=true INSTALL_K3S_EXEC="server \
    --datastore-endpoint=http://127.0.0.1:3379 --disable-agent \
    --disable=traefik,servicelb,metrics-server,local-storage --disable-helm-controller \
    --disable-cloud-controller --disable-network-policy --flannel-backend=none \
    --kube-apiserver-arg=etcd-count-metric-poll-period=0" sh -
  until KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl version 2>/dev/null | grep -q Server; do sleep 2; done
  cp /etc/rancher/k3s/k3s.yaml "$KUBECONFIG_OUT"
  log "k3s up; kubeconfig -> $KUBECONFIG_OUT"

  # KWOK: manage all nodes, needs an explicit stages config or it exits.
  cp "$HERE/../config/kwok-stages-fast.yaml" "$STAGES"
  systemctl reset-failed kwok-restest 2>/dev/null || true
  systemd-run --unit=kwok-restest /usr/local/bin/kwok --kubeconfig="$KUBECONFIG_OUT" \
    --manage-all-nodes=true --config="$STAGES" --cidr=10.244.0.0/16 --node-ip=10.244.0.1
  # Fake nodes: label type=kwok for nodeSelector; NO taint (or pods need
  # operator:Exists tolerations — resource-coverage.sh uses them anyway).
  for i in 0 1 2; do
    KUBECONFIG="$KUBECONFIG_OUT" kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Node
metadata:
  name: kwok-node-$i
  labels: {type: kwok, kubernetes.io/hostname: kwok-node-$i}
  annotations: {kwok.x-k8s.io/node: fake, node.alpha.kubernetes.io/ttl: "0"}
status:
  capacity: {cpu: "32", memory: 256Gi, pods: "110"}
  allocatable: {cpu: "32", memory: 256Gi, pods: "110"}
  nodeInfo: {kubeletVersion: fake}
EOF
  done
  for _ in $(seq 1 15); do
    [[ "$(KUBECONFIG="$KUBECONFIG_OUT" kubectl get nodes --no-headers 2>/dev/null | grep -c Ready)" == "3" ]] && break
    sleep 2
  done
  log "KWOK up; $(KUBECONFIG="$KUBECONFIG_OUT" kubectl get nodes --no-headers 2>/dev/null | grep -c Ready)/3 nodes Ready"
  log "ready — run: KUBECONFIG=$KUBECONFIG_OUT ./resource-coverage.sh"
}

down() {
  log "tearing down"
  systemctl disable --now kwok-restest 2>/dev/null || true
  [[ -x /usr/local/bin/k3s-uninstall.sh ]] && /usr/local/bin/k3s-uninstall.sh || systemctl disable --now k3s 2>/dev/null || true
  systemctl disable --now kubebrain 2>/dev/null || true
  rm -rf "$KB_DATA" "$KUBECONFIG_OUT"
  log "done"
}

case "${1:-up}" in
  up)   up ;;
  down) down ;;
  *) echo "usage: $0 {up|down}"; exit 1 ;;
esac
