#!/usr/bin/env bash
set -euo pipefail

# Destructive only to the exact disposable resources named below. The two
# clusters use independent PD/TiKV processes and memory-backed data dirs.
drill_tmp=$(mktemp -d /tmp/kb-native-pitr-full-restore.XXXXXX)
shared_dir="$drill_tmp/shared"
mkdir -p "$shared_dir"
chmod 0777 "$shared_dir"

names=(
  kb-native-pitr-src-pd-integration
  kb-native-pitr-src-tikv-integration
  kb-native-pitr-tgt-pd-integration
  kb-native-pitr-tgt-tikv-integration
  kb-native-pitr-br-integration
)
cleanup() {
  for name in "${names[@]}"; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
  find "$drill_tmp" -depth -delete
}
trap cleanup EXIT INT TERM

for name in "${names[@]}"; do
  if docker container inspect "$name" >/dev/null 2>&1; then
    echo "refusing to replace existing container $name" >&2
    exit 1
  fi
done

docker run -d --name "${names[0]}" --network host --tmpfs /data:rw,size=512m,mode=1777 pingcap/pd:v7.5.1 \
  --name=src-pd --data-dir=/data --client-urls=http://127.0.0.1:42379 --advertise-client-urls=http://127.0.0.1:42379 \
  --peer-urls=http://127.0.0.1:42380 --advertise-peer-urls=http://127.0.0.1:42380 --initial-cluster=src-pd=http://127.0.0.1:42380 --log-file= >/dev/null
docker run -d --name "${names[2]}" --network host --tmpfs /data:rw,size=512m,mode=1777 pingcap/pd:v7.5.1 \
  --name=tgt-pd --data-dir=/data --client-urls=http://127.0.0.1:43379 --advertise-client-urls=http://127.0.0.1:43379 \
  --peer-urls=http://127.0.0.1:43380 --advertise-peer-urls=http://127.0.0.1:43380 --initial-cluster=tgt-pd=http://127.0.0.1:43380 --log-file= >/dev/null

for port in 42379 43379; do
  for attempt in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$port/pd/api/v1/health" >/dev/null; then break; fi
    if [[ "$attempt" == 60 ]]; then echo "PD $port did not become healthy" >&2; exit 1; fi
    sleep 1
  done
done

docker run -d --name "${names[1]}" --network host --tmpfs /data:rw,size=64g,mode=1777 \
  -v "$shared_dir:$shared_dir" pingcap/tikv:v7.5.1 \
  --addr=127.0.0.1:42160 --advertise-addr=127.0.0.1:42160 --status-addr=127.0.0.1:20180 \
  --pd=127.0.0.1:42379 --data-dir=/data --log-file= >/dev/null
docker run -d --name "${names[3]}" --network host --tmpfs /data:rw,size=64g,mode=1777 \
  -v "$shared_dir:$shared_dir" pingcap/tikv:v7.5.1 \
  --addr=127.0.0.1:43160 --advertise-addr=127.0.0.1:43160 --status-addr=127.0.0.1:21180 \
  --pd=127.0.0.1:43379 --data-dir=/data --log-file= >/dev/null

for port in 20180 21180; do
  for attempt in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:$port/status" >/dev/null; then break; fi
    if [[ "$attempt" == 90 ]]; then echo "TiKV $port did not become healthy" >&2; exit 1; fi
    sleep 1
  done
done

for spec in "${names[0]} 42379" "${names[2]} 43379"; do
  read -r container port <<<"$spec"
  for attempt in $(seq 1 60); do
    docker exec "$container" /pd-ctl -u "http://127.0.0.1:$port" config set max-replicas 1 >"$drill_tmp/pdctl-$port" 2>&1 || true
    if rg -q Success "$drill_tmp/pdctl-$port"; then break; fi
    if [[ "$attempt" == 60 ]]; then cat "$drill_tmp/pdctl-$port" >&2; exit 1; fi
    sleep 1
  done
done

docker create --name "${names[4]}" pingcap/br:v7.5.1 >/dev/null
docker cp "${names[4]}:/br" "$drill_tmp/br"
chmod 0755 "$drill_tmp/br"
go build -o "$drill_tmp/kubebrain" ./cmd

TMPDIR="$shared_dir" \
KUBEBRAIN_NATIVE_PITR_SOURCE_PD=127.0.0.1:42379 \
KUBEBRAIN_NATIVE_PITR_TARGET_PD=127.0.0.1:43379 \
KUBEBRAIN_NATIVE_PITR_BR="$drill_tmp/br" \
KUBEBRAIN_NATIVE_PITR_SERVER="$drill_tmp/kubebrain" \
go test -count=1 -run '^TestNativeFullRestoreRealBR$' -v ./hack/backup/cmd/native-pitr-full-restore
