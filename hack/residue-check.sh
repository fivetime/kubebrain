#!/usr/bin/env bash
# residue-check.sh — 真实应用安装/卸载/升级后的存储残留体检(全程只读)
#
# 典型工作流(在能连到 KubeBrain 3379 的机器上,如 control 节点):
#   ./residue-check.sh snapshot before-nginx     # 装应用前拍基线
#   helm install nginx ... / kubectl apply ...   # 装
#   ./residue-check.sh snapshot with-nginx       # (可选)装完拍一份,确认写入了什么
#   helm uninstall nginx / kubectl delete ...    # 卸
#   sleep 60                                     # 给 kcm 级联/GC 一点时间
#   ./residue-check.sh snapshot after-nginx      # 卸载后拍
#   ./residue-check.sh diff before-nginx after-nginx   # 残留 = 多出来的每一个 key
#   ./residue-check.sh health                    # GC 链路是否在消化墓碑
#
# 环境变量:
#   KB_ENDPOINTS  etcd 端点(默认 http://127.0.0.1:3379)
#   KB_INFO_PORT  info 端口(默认 8080,用于 /election 与 leader 指标)
#   KB_PREFIX     键前缀(默认 /registry)
#   SNAP_DIR      快照目录(默认 /var/tmp/kb-residue)
set -euo pipefail

KB_ENDPOINTS=${KB_ENDPOINTS:-http://127.0.0.1:3379}
KB_INFO_PORT=${KB_INFO_PORT:-8080}
KB_PREFIX=${KB_PREFIX:-/registry}
SNAP_DIR=${SNAP_DIR:-/var/tmp/kb-residue}
export ETCDCTL_API=3

ec() { etcdctl --endpoints="$KB_ENDPOINTS" --command-timeout=60s "$@"; }

usage() { grep '^#' "$0" | head -22 | sed 's/^# \{0,1\}//'; exit 1; }

snapshot() {
  local name=$1 out="$SNAP_DIR/$1.keys"
  mkdir -p "$SNAP_DIR"
  local rev count
  rev=$(ec endpoint status --write-out=fields | grep '"Revision"' | grep -oE '[0-9]+' | head -1)
  ec get "$KB_PREFIX/" --prefix --keys-only | grep -v '^$' | sort > "$out"
  count=$(wc -l < "$out")
  echo "revision=$rev keys=$count" > "$SNAP_DIR/$1.meta"
  echo "snapshot '$name': $count keys @ rev $rev -> $out"
}

diff_snaps() {
  local a="$SNAP_DIR/$1.keys" b="$SNAP_DIR/$2.keys"
  [ -f "$a" ] && [ -f "$b" ] || { echo "missing snapshot file(s): $a / $b" >&2; exit 2; }
  echo "=== 残留(在 '$2' 中新增、'$1' 中没有的 key)==="
  comm -13 "$a" "$b" | tee /dev/stderr | wc -l | xargs -I{} echo "--- 共 {} 个残留 key"
  echo
  echo "=== 消失(在 '$1' 中有、'$2' 中没有的 key,应≈应用自身的对象)==="
  comm -23 "$a" "$b" | wc -l | xargs -I{} echo "--- 共 {} 个"
  echo
  echo "=== 残留按类型汇总 ==="
  comm -13 "$a" "$b" | awk -F/ '{print "/"$2"/"$3}' | sort | uniq -c | sort -rn | head -20
  echo
  echo "提示:常见的合法'残留'= kube-root-ca.crt configmap、default serviceaccount(命名空间还在时由 kcm 补建)、"
  echo "     TTL 未到期的 events(默认 1h 自灭)。除此之外的任何 key 都值得追查:"
  echo "     常见真凶=finalizer 卡住的对象、孤儿 endpointslices(service 删除后)、Terminating 卡住的 namespace。"
}

health() {
  echo "=== leader 定位(compact/count_index/safepoint 是 leader-only 指标)==="
  local leader_host=""
  for ep in ${KB_ENDPOINTS//,/ }; do
    local host=${ep#http://}; host=${host%:*}
    local e; e=$(curl -s -m 3 "http://$host:$KB_INFO_PORT/election" 2>/dev/null || true)
    echo "  $host: $e"
    echo "$e" | grep -q '"IsLeader":true' && leader_host=$host
  done
  [ -n "$leader_host" ] || { echo "!! 没找到 leader(把所有副本的 IP 都列进 KB_ENDPOINTS 再试)"; exit 3; }
  echo
  echo "=== GC 链路(leader=$leader_host,间隔 30s 两次采样)==="
  local m="http://$leader_host:$KB_INFO_PORT/metrics"
  local c0 c1 sp0 sp1
  c0=$(curl -s "$m" | grep -a '^compact{' | awk '{print $2}')
  sp0=$(curl -s "$m" | grep -a '^storage_gc_safepoint' | awk '{print $2}')
  sleep 30
  c1=$(curl -s "$m" | grep -a '^compact{' | awk '{print $2}')
  sp1=$(curl -s "$m" | grep -a '^storage_gc_safepoint' | awk '{print $2}')
  echo "  compact 行计数: ${c0:-无} -> ${c1:-无}   (有删除流量时应增长;注意计数在 GC 轮末批量更新,安静集群两次相等正常)"
  echo "  storage_gc_safepoint: ${sp0:-无} -> ${sp1:-无}   (每 ~10min 推进一次,30s 内相等正常;间隔 >10min 复查仍不动=冻结,TiKV MVCC 在堆积)"
  curl -s "$m" | grep -a '^count_index_keys' | awk '{print "  count_index_keys: "$2"   (应≈集群真实对象数;恒 0=索引没建起来)"}'
  echo
  if command -v kubectl >/dev/null 2>&1; then
    echo "=== k8s 侧常见残留(kubectl 可用)==="
    echo "  Terminating 卡住的 namespace:"
    kubectl get ns --no-headers 2>/dev/null | awk '$2=="Terminating"{print "    "$1}' | head -10
    echo "  带 deletionTimestamp 卡住 >5min 的对象需人工排查 finalizer(kubectl get <res> -o json | jq '.items[]|select(.metadata.deletionTimestamp!=null)')"
  fi
}

case "${1:-}" in
  snapshot) [ -n "${2:-}" ] || usage; snapshot "$2";;
  diff)     [ -n "${3:-}" ] || usage; diff_snaps "$2" "$3";;
  health)   health;;
  *)        usage;;
esac
