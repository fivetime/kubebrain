#!/usr/bin/env bash
set -euo pipefail

# Destructive only to the exact disposable resources named below. The two
# clusters use independent PD/TiKV processes and memory-backed data dirs.
topology_size=${KUBEBRAIN_NATIVE_PITR_TOPOLOGY_SIZE:-1}
if [[ "$topology_size" != 1 && "$topology_size" != 3 ]]; then
  echo "KUBEBRAIN_NATIVE_PITR_TOPOLOGY_SIZE must be 1 or 3" >&2
  exit 2
fi
fault_injection=${KUBEBRAIN_NATIVE_PITR_FAULT_INJECTION:-none}
test_name=${KUBEBRAIN_NATIVE_PITR_TEST:-TestNativeFullRestoreRealBR}
objectstore_integration=false
case "$test_name" in
  TestCanceledPutObjectLeavesNoRemoteArtifact|TestCommittedPutObjectResponseLossReconcilesRealS3|TestConditionalUploadRefusesConflictingRealS3Object|TestConditionalUploadRejectsMatchingMetadataCorruptRealS3Body|TestConditionalUploadRejectsInsufficientRealS3Retention|TestRestartRecoversReceiptAfterRealS3Commit)
    objectstore_integration=true
    ;;
esac
if [[ "$fault_injection" != none && "$fault_injection" != member-pause-store-resume && "$fault_injection" != preferred-member-pause-store-resume && "$fault_injection" != leader-member-pause-store-resume && "$fault_injection" != target-leader-member-pause-store-resume && "$fault_injection" != target-leader-member-pause-store-during-br-resume && "$fault_injection" != target-two-store-quorum-loss-resume && "$fault_injection" != target-store-enospc-resume && "$fault_injection" != target-store-reserve-enospc-recover && "$fault_injection" != target-two-store-enospc-resume && "$fault_injection" != target-pd-leader-enospc-resume && "$fault_injection" != target-two-pd-enospc-resume && "$fault_injection" != target-pd-network-quorum-loss-resume && "$fault_injection" != target-kubebrain-pd-network-isolation-resume && "$fault_injection" != target-pd-leader-store-enospc-resume ]]; then
  echo "KUBEBRAIN_NATIVE_PITR_FAULT_INJECTION must be none, member-pause-store-resume, preferred-member-pause-store-resume, leader-member-pause-store-resume, target-leader-member-pause-store-resume, target-leader-member-pause-store-during-br-resume, target-two-store-quorum-loss-resume, target-store-enospc-resume, target-store-reserve-enospc-recover, target-two-store-enospc-resume, target-pd-leader-enospc-resume, target-two-pd-enospc-resume, target-pd-network-quorum-loss-resume, target-kubebrain-pd-network-isolation-resume, or target-pd-leader-store-enospc-resume" >&2
  exit 2
fi
if [[ "$fault_injection" != none && "$topology_size" != 3 ]]; then
  echo "$fault_injection requires KUBEBRAIN_NATIVE_PITR_TOPOLOGY_SIZE=3" >&2
  exit 2
fi
etcdutl_bin=""
if [[ "$fault_injection" == target-two-pd-enospc-resume || "$test_name" == TestNativeLegacyLeaseHistorySnapshotRealCluster ]]; then
  etcdutl_bin=${KUBEBRAIN_ETCDUTL_BIN:-/root/etcd/bin/etcdutl}
  if [[ ! -x "$etcdutl_bin" ]]; then
    echo "$test_name requires executable KUBEBRAIN_ETCDUTL_BIN (default /root/etcd/bin/etcdutl)" >&2
    exit 1
  fi
fi
drill_tmp=$(mktemp -d /tmp/kb-native-pitr-full-restore.XXXXXX)
# The isolated-process profile drops privileges before exec. Allow traversal to
# the built binary without exposing a directory listing of receipts/artifacts.
chmod 0711 "$drill_tmp"
shared_dir="$drill_tmp/shared"
tikv_config="$PWD/hack/backup/native-pitr-tikv-integration.toml"
if [[ "$fault_injection" == target-store-enospc-resume || "$fault_injection" == target-two-store-enospc-resume || "$fault_injection" == target-pd-leader-store-enospc-resume ]]; then
  tikv_config="$PWD/hack/backup/native-pitr-tikv-enospc-integration.toml"
fi
if [[ "$fault_injection" == target-store-reserve-enospc-recover ]]; then
  tikv_config="$PWD/hack/backup/native-pitr-tikv-enospc-reserve-integration.toml"
fi
mkdir -p "$shared_dir"
chmod 0777 "$shared_dir"

names=()
mounted_dirs=()
network_partition_chain=""
source_pd_names=()
target_pd_names=()
target_pd_data_dirs=()
source_tikv_names=()
target_tikv_names=()
target_enospc_data_dirs=()
source_pd_endpoints=()
target_pd_endpoints=()
source_initial_cluster=()
target_initial_cluster=()
for index in $(seq 0 $((topology_size - 1))); do
  source_pd_names+=("kb-native-pitr-src-pd-$index-integration")
  target_pd_names+=("kb-native-pitr-tgt-pd-$index-integration")
  source_tikv_names+=("kb-native-pitr-src-tikv-$index-integration")
  target_tikv_names+=("kb-native-pitr-tgt-tikv-$index-integration")
  source_pd_endpoints+=("127.0.0.1:$((42379 + index * 10))")
  target_pd_endpoints+=("127.0.0.1:$((43379 + index * 10))")
  source_initial_cluster+=("src-pd-$index=http://127.0.0.1:$((42380 + index * 10))")
  target_initial_cluster+=("tgt-pd-$index=http://127.0.0.1:$((43380 + index * 10))")
done
br_container=kb-native-pitr-br-integration
minio_container=kb-native-pitr-minio-integration
mc_container=kb-native-pitr-mc-integration
names+=("${source_pd_names[@]}" "${target_pd_names[@]}" "${source_tikv_names[@]}" "${target_tikv_names[@]}" "$br_container" "$minio_container" "$mc_container")
source_pd_csv=$(IFS=,; echo "${source_pd_endpoints[*]}")
target_pd_csv=$(IFS=,; echo "${target_pd_endpoints[*]}")
source_initial_csv=$(IFS=,; echo "${source_initial_cluster[*]}")
target_initial_csv=$(IFS=,; echo "${target_initial_cluster[*]}")
for name in "${names[@]}"; do
  if docker container inspect "$name" >/dev/null 2>&1; then
    echo "refusing to replace existing container $name" >&2
    find "$drill_tmp" -depth -delete
    exit 1
  fi
done
cleanup() {
  cleanup_mount_failed=false
  if [[ -n "$network_partition_chain" ]]; then
    while iptables -w 5 -C OUTPUT -j "$network_partition_chain" 2>/dev/null; do
      iptables -w 5 -D OUTPUT -j "$network_partition_chain" >/dev/null 2>&1 || break
    done
    iptables -w 5 -F "$network_partition_chain" >/dev/null 2>&1 || true
    iptables -w 5 -X "$network_partition_chain" >/dev/null 2>&1 || true
  fi
  for name in "${names[@]}"; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
  for mount_dir in "${mounted_dirs[@]}"; do
    if mountpoint -q "$mount_dir" && ! umount "$mount_dir"; then
      echo "failed to unmount disposable integration path $mount_dir; preserving it for manual cleanup" >&2
      cleanup_mount_failed=true
    fi
  done
  if [[ "$cleanup_mount_failed" == false ]]; then
    find "$drill_tmp" -depth -delete
  fi
}
trap cleanup EXIT INT TERM

if [[ "$fault_injection" == target-pd-network-quorum-loss-resume || "$fault_injection" == target-kubebrain-pd-network-isolation-resume ]]; then
  if [[ $(id -u) != 0 ]] || ! command -v iptables >/dev/null 2>&1 || ! iptables -w 5 -S OUTPUT >/dev/null 2>&1; then
    echo "$fault_injection requires root and a usable host iptables OUTPUT chain" >&2
    exit 1
  fi
  network_partition_chain="KBPDNET$$"
  iptables -w 5 -N "$network_partition_chain"
  iptables -w 5 -I OUTPUT 1 -j "$network_partition_chain"
fi

target_enospc_count=0
if [[ "$fault_injection" == target-store-enospc-resume ]]; then target_enospc_count=1; fi
if [[ "$fault_injection" == target-store-reserve-enospc-recover ]]; then target_enospc_count=1; fi
if [[ "$fault_injection" == target-two-store-enospc-resume ]]; then target_enospc_count=2; fi
if [[ "$fault_injection" == target-pd-leader-store-enospc-resume ]]; then target_enospc_count=1; fi
for ((index=0; index<target_enospc_count; index++)); do
  target_enospc_data_dir="$drill_tmp/target-tikv-$index-data"
  mkdir -p "$target_enospc_data_dir"
  mount -t tmpfs -o size=768m,mode=1777 "kb-native-pitr-enospc-$index" "$target_enospc_data_dir"
  mounted_dirs+=("$target_enospc_data_dir")
  target_enospc_data_dirs+=("$target_enospc_data_dir")
done
if [[ "$fault_injection" == target-pd-leader-enospc-resume || "$fault_injection" == target-two-pd-enospc-resume || "$fault_injection" == target-pd-leader-store-enospc-resume ]]; then
  for index in $(seq 0 $((topology_size - 1))); do
    target_pd_data_dir="$drill_tmp/target-pd-$index-data"
    mkdir -p "$target_pd_data_dir"
    mount -t tmpfs -o size=512m,mode=1777 "kb-native-pitr-pd-enospc-$index" "$target_pd_data_dir"
    mounted_dirs+=("$target_pd_data_dir")
    target_pd_data_dirs+=("$target_pd_data_dir")
  done
fi

for index in $(seq 0 $((topology_size - 1))); do
  source_client_port=$((42379 + index * 10)); source_peer_port=$((42380 + index * 10))
  target_client_port=$((43379 + index * 10)); target_peer_port=$((43380 + index * 10))
  docker run -d --name "${source_pd_names[$index]}" --network host --tmpfs /data:rw,size=512m,mode=1777 pingcap/pd:v7.5.1 \
    --name="src-pd-$index" --data-dir=/data --client-urls="http://127.0.0.1:$source_client_port" --advertise-client-urls="http://127.0.0.1:$source_client_port" \
    --peer-urls="http://127.0.0.1:$source_peer_port" --advertise-peer-urls="http://127.0.0.1:$source_peer_port" --initial-cluster="$source_initial_csv" --log-file= >/dev/null
  target_pd_data_args=(--tmpfs /data:rw,size=512m,mode=1777)
  if [[ "$fault_injection" == target-pd-leader-enospc-resume || "$fault_injection" == target-two-pd-enospc-resume || "$fault_injection" == target-pd-leader-store-enospc-resume ]]; then
    target_pd_data_args=(-v "${target_pd_data_dirs[$index]}:/data")
  fi
  docker run -d --name "${target_pd_names[$index]}" --network host "${target_pd_data_args[@]}" pingcap/pd:v7.5.1 \
    --name="tgt-pd-$index" --data-dir=/data --client-urls="http://127.0.0.1:$target_client_port" --advertise-client-urls="http://127.0.0.1:$target_client_port" \
    --peer-urls="http://127.0.0.1:$target_peer_port" --advertise-peer-urls="http://127.0.0.1:$target_peer_port" --initial-cluster="$target_initial_csv" --log-file= >/dev/null
done

for endpoint in "${source_pd_endpoints[@]}" "${target_pd_endpoints[@]}"; do
  for attempt in $(seq 1 60); do
    if curl -fsS "http://$endpoint/pd/api/v1/health" >/dev/null; then break; fi
    if [[ "$attempt" == 60 ]]; then echo "PD $endpoint did not become healthy" >&2; exit 1; fi
    sleep 1
  done
done

for index in $(seq 0 $((topology_size - 1))); do
  source_tikv_port=$((42160 + index)); source_status_port=$((20180 + index))
  target_tikv_port=$((43160 + index)); target_status_port=$((21180 + index))
  docker run -d --name "${source_tikv_names[$index]}" --network host --tmpfs /data:rw,size=64g,mode=1777 \
    -e AWS_ACCESS_KEY_ID=kubebrain-drill -e AWS_SECRET_ACCESS_KEY=kubebrain-drill-secret \
    -v "$shared_dir:$shared_dir" -v "$tikv_config:/native-pitr-integration.toml:ro" pingcap/tikv:v7.5.1 \
    --config=/native-pitr-integration.toml --addr="127.0.0.1:$source_tikv_port" --advertise-addr="127.0.0.1:$source_tikv_port" \
    --status-addr="127.0.0.1:$source_status_port" --pd="$source_pd_csv" --data-dir=/data --log-file= >/dev/null
  target_data_args=(--tmpfs /data:rw,size=64g,mode=1777)
  if (( index < target_enospc_count )); then
    target_data_args=(-v "${target_enospc_data_dirs[$index]}:/data")
  fi
  docker run -d --name "${target_tikv_names[$index]}" --network host "${target_data_args[@]}" \
    -e AWS_ACCESS_KEY_ID=kubebrain-drill -e AWS_SECRET_ACCESS_KEY=kubebrain-drill-secret \
    -v "$shared_dir:$shared_dir" -v "$tikv_config:/native-pitr-integration.toml:ro" pingcap/tikv:v7.5.1 \
    --config=/native-pitr-integration.toml --addr="127.0.0.1:$target_tikv_port" --advertise-addr="127.0.0.1:$target_tikv_port" \
    --status-addr="127.0.0.1:$target_status_port" --pd="$target_pd_csv" --data-dir=/data --log-file= >/dev/null
done

status_ports=()
for index in $(seq 0 $((topology_size - 1))); do status_ports+=("$((20180 + index))" "$((21180 + index))"); done
for port in "${status_ports[@]}"; do
  for attempt in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:$port/status" >/dev/null; then break; fi
    if [[ "$attempt" == 90 ]]; then echo "TiKV $port did not become healthy" >&2; exit 1; fi
    sleep 1
  done
done

for spec in "${source_pd_names[0]} 42379" "${target_pd_names[0]} 43379"; do
  read -r container port <<<"$spec"
  for attempt in $(seq 1 60); do
    docker exec "$container" /pd-ctl -u "http://127.0.0.1:$port" config set max-replicas "$topology_size" >"$drill_tmp/pdctl-$port" 2>&1 || true
    if rg -q Success "$drill_tmp/pdctl-$port"; then break; fi
    if [[ "$attempt" == 60 ]]; then cat "$drill_tmp/pdctl-$port" >&2; exit 1; fi
    sleep 1
  done
done

for port in 42379 43379; do
  for attempt in $(seq 1 90); do
    up_stores=$(curl -fsS "http://127.0.0.1:$port/pd/api/v1/stores" | jq '[.stores[] | select(.store.state_name == "Up")] | length')
    if [[ "$up_stores" == "$topology_size" ]]; then break; fi
    if [[ "$attempt" == 90 ]]; then echo "PD $port saw $up_stores/$topology_size Up stores" >&2; exit 1; fi
    sleep 1
  done
done

for port in 42379 43379; do
  for attempt in $(seq 1 90); do
    fully_replicated=$(curl -fsS "http://127.0.0.1:$port/pd/api/v1/regions" | jq --argjson replicas "$topology_size" '(.count > 0) and ([.regions[] | ((.peers | length) == $replicas) and (((.pending_peers // []) | length) == 0)] | all)')
    if [[ "$fully_replicated" == true ]]; then break; fi
    if [[ "$attempt" == 90 ]]; then echo "PD $port regions did not reach $topology_size replicas without pending peers" >&2; exit 1; fi
    sleep 1
  done
done

docker create --name "$br_container" pingcap/br:v7.5.1 >/dev/null
docker cp "$br_container:/br" "$drill_tmp/br"
chmod 0755 "$drill_tmp/br"
go build -o "$drill_tmp/kubebrain" ./cmd
go build -o "$drill_tmp/native-pitr-admission-fence" ./hack/backup/cmd/native-pitr-admission-fence
go build -o "$drill_tmp/native-pitr-restore-plan" ./hack/backup/cmd/native-pitr-restore-plan
go build -o "$drill_tmp/native-pitr-source-capture" ./hack/backup/cmd/native-pitr-source-capture

log_env=()
fault_env=()
if [[ "$fault_injection" == target-two-store-quorum-loss-resume ]]; then
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_QUORUM_LOSS_CONTAINERS="${target_tikv_names[0]},${target_tikv_names[1]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_QUORUM_RECOVERY_ADDRESSES=127.0.0.1:21180,127.0.0.1:21181
  )
elif [[ "$fault_injection" == target-store-enospc-resume || "$fault_injection" == target-store-reserve-enospc-recover ]]; then
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_CONTAINER="${target_tikv_names[0]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_DATA_DIR="${target_enospc_data_dirs[0]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_STATUS=127.0.0.1:21180
  )
elif [[ "$fault_injection" == target-two-store-enospc-resume ]]; then
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_CONTAINERS="${target_tikv_names[0]},${target_tikv_names[1]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_DATA_DIRS="${target_enospc_data_dirs[0]},${target_enospc_data_dirs[1]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_STATUSES=127.0.0.1:21180,127.0.0.1:21181
  )
elif [[ "$fault_injection" == target-two-pd-enospc-resume ]]; then
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_CONTAINERS="$(IFS=,; echo "${target_pd_names[*]}")"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_DATA_DIRS="$(IFS=,; echo "${target_pd_data_dirs[*]}")"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_MEMBERS="tgt-pd-0,tgt-pd-1,tgt-pd-2"
    KUBEBRAIN_NATIVE_PITR_ETCDUTL="$etcdutl_bin"
  )
elif [[ "$fault_injection" == target-pd-network-quorum-loss-resume ]]; then
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_NETWORK_CHAIN="$network_partition_chain"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_NETWORK_CLIENT_PORTS=43379,43389
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_NETWORK_PEER_PORTS=43380,43390,43400
  )
elif [[ "$fault_injection" == target-kubebrain-pd-network-isolation-resume ]]; then
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_NETWORK_CHAIN="$network_partition_chain"
    KUBEBRAIN_NATIVE_PITR_ISOLATED_UID=65534
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_NETWORK_CLIENT_PORTS=43379,43389,43399
    KUBEBRAIN_NATIVE_PITR_TARGET_TIKV_STATUS_ADDRESSES=127.0.0.1:21180,127.0.0.1:21181,127.0.0.1:21182
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_CONTAINER="${target_pd_names[0]}"
  )
elif [[ "$fault_injection" == target-pd-leader-enospc-resume || "$fault_injection" == target-pd-leader-store-enospc-resume ]]; then
  leader_name=$(curl -fsS http://127.0.0.1:43379/pd/api/v1/leader | jq -er '.name | select(type == "string" and length > 0)')
  fault_pd_index=-1
  for index in $(seq 0 $((topology_size - 1))); do
    if [[ "$leader_name" == "tgt-pd-$index" ]]; then fault_pd_index=$index; break; fi
  done
  if [[ "$fault_pd_index" == -1 ]]; then
    echo "live target PD leader $leader_name does not map to a managed integration container" >&2
    exit 1
  fi
  echo "ENOSPC injection selected live target PD leader $leader_name" >&2
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_CONTAINER="${target_pd_names[$fault_pd_index]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_DATA_DIR="${target_pd_data_dirs[$fault_pd_index]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_ENDPOINT="127.0.0.1:$((43379 + fault_pd_index * 10))"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_ENOSPC_MEMBER="$leader_name"
  )
  if [[ "$fault_injection" == target-pd-leader-store-enospc-resume ]]; then
    fault_env+=(
      KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_CONTAINER="${target_tikv_names[0]}"
      KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_DATA_DIR="${target_enospc_data_dirs[0]}"
      KUBEBRAIN_NATIVE_PITR_TARGET_ENOSPC_STATUS=127.0.0.1:21180
    )
  fi
elif [[ "$fault_injection" == target-leader-member-pause-store-resume || "$fault_injection" == target-leader-member-pause-store-during-br-resume ]]; then
  leader_name=$(curl -fsS http://127.0.0.1:43379/pd/api/v1/leader | jq -er '.name | select(type == "string" and length > 0)')
  fault_pd_index=-1
  for index in $(seq 0 $((topology_size - 1))); do
    if [[ "$leader_name" == "tgt-pd-$index" ]]; then fault_pd_index=$index; break; fi
  done
  if [[ "$fault_pd_index" == -1 ]]; then
    echo "live target PD leader $leader_name does not map to a managed integration container" >&2
    exit 1
  fi
  echo "fault injection selected live target PD leader $leader_name" >&2
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_TARGET_FAULT_CONTAINERS="${target_pd_names[$fault_pd_index]},${target_tikv_names[0]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_RECOVERY_CONTAINER="${target_tikv_names[0]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_RECOVERY_ADDRESS=127.0.0.1:21180
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_RECOVERY_CONTAINER="${target_pd_names[$fault_pd_index]}"
    KUBEBRAIN_NATIVE_PITR_TARGET_PD_RECOVERY_ADDRESS="127.0.0.1:$((43379 + fault_pd_index * 10))"
  )
  if [[ "$fault_injection" == target-leader-member-pause-store-during-br-resume ]]; then
    fault_env+=(KUBEBRAIN_NATIVE_PITR_TARGET_FAULT_DURING_BR=true)
  fi
elif [[ "$fault_injection" != none ]]; then
  fault_pd_index=2
  if [[ "$fault_injection" == preferred-member-pause-store-resume ]]; then fault_pd_index=0; fi
  if [[ "$fault_injection" == leader-member-pause-store-resume ]]; then
    leader_name=$(curl -fsS http://127.0.0.1:42379/pd/api/v1/leader | jq -er '.name | select(type == "string" and length > 0)')
    fault_pd_index=-1
    for index in $(seq 0 $((topology_size - 1))); do
      if [[ "$leader_name" == "src-pd-$index" ]]; then fault_pd_index=$index; break; fi
    done
    if [[ "$fault_pd_index" == -1 ]]; then
      echo "live source PD leader $leader_name does not map to a managed integration container" >&2
      exit 1
    fi
    echo "fault injection selected live source PD leader $leader_name" >&2
  fi
  fault_env=(
    KUBEBRAIN_NATIVE_PITR_SOURCE_FAULT_CONTAINERS="${source_pd_names[$fault_pd_index]},${source_tikv_names[0]}"
    KUBEBRAIN_NATIVE_PITR_SOURCE_RECOVERY_CONTAINER="${source_tikv_names[0]}"
    KUBEBRAIN_NATIVE_PITR_SOURCE_RECOVERY_ADDRESS=127.0.0.1:20180
    KUBEBRAIN_NATIVE_PITR_SOURCE_PD_RECOVERY_CONTAINER="${source_pd_names[$fault_pd_index]}"
    KUBEBRAIN_NATIVE_PITR_SOURCE_PD_RECOVERY_ADDRESS="127.0.0.1:$((42379 + fault_pd_index * 10))"
    KUBEBRAIN_NATIVE_PITR_COLD_RESTART_DURING_FAULT=true
  )
fi
if [[ "$test_name" == TestNativeLogReplayRealBR || "$objectstore_integration" == true ]]; then
  docker run -d --name "$minio_container" --network host --tmpfs /data:rw,size=4g,mode=1777 \
    -e MINIO_ROOT_USER=kubebrain-drill -e MINIO_ROOT_PASSWORD=kubebrain-drill-secret \
    minio/minio:RELEASE.2025-04-22T22-12-26Z server /data --address=:49000 --console-address=:49001 >/dev/null
  for attempt in $(seq 1 60); do
    if curl -fsS http://127.0.0.1:49000/minio/health/live >/dev/null; then break; fi
    if [[ "$attempt" == 60 ]]; then echo "MinIO did not become healthy" >&2; exit 1; fi
    sleep 1
  done
  docker create --name "$mc_container" quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z >/dev/null
  docker cp "$mc_container:/usr/bin/mc" "$drill_tmp/mc"
  chmod 0755 "$drill_tmp/mc"
  export MC_HOST_drill=http://kubebrain-drill:kubebrain-drill-secret@127.0.0.1:49000
  if [[ "$objectstore_integration" == true ]]; then
    "$drill_tmp/mc" mb --with-lock drill/kubebrain-pitr >/dev/null
  else
    "$drill_tmp/mc" mb drill/kubebrain-pitr >/dev/null
  fi
  export AWS_ACCESS_KEY_ID=kubebrain-drill AWS_SECRET_ACCESS_KEY=kubebrain-drill-secret AWS_REGION=us-east-1
fi
if [[ "$test_name" == TestNativeLogReplayRealBR ]]; then
  go build -o "$drill_tmp/native-pitr-preflight" ./hack/backup/cmd/native-pitr-preflight
  go build -o "$drill_tmp/native-pitr-task-create" ./hack/backup/cmd/native-pitr-task-create
  go build -o "$drill_tmp/native-pitr-restoration-fence" ./hack/backup/cmd/native-pitr-restoration-fence
  go build -o "$drill_tmp/native-pitr-log-replay" ./hack/backup/cmd/native-pitr-log-replay
  go build -o "$drill_tmp/native-pitr-semantic-verify" ./hack/backup/cmd/native-pitr-semantic-verify
  log_env=(
    KUBEBRAIN_NATIVE_PITR_PREFLIGHT="$drill_tmp/native-pitr-preflight"
    KUBEBRAIN_NATIVE_PITR_TASK_CREATE="$drill_tmp/native-pitr-task-create"
    KUBEBRAIN_NATIVE_PITR_FENCE="$drill_tmp/native-pitr-restoration-fence"
    KUBEBRAIN_NATIVE_PITR_LOG_REPLAY="$drill_tmp/native-pitr-log-replay"
    KUBEBRAIN_NATIVE_PITR_SEMANTIC_VERIFY="$drill_tmp/native-pitr-semantic-verify"
    KUBEBRAIN_NATIVE_PITR_MC="$drill_tmp/mc"
    KUBEBRAIN_NATIVE_PITR_S3_ENDPOINT=http://127.0.0.1:49000
    KUBEBRAIN_NATIVE_PITR_S3_BUCKET=kubebrain-pitr
    KUBEBRAIN_NATIVE_PITR_S3_PREFIX=logs/restore-integration
  )
fi

if [[ "$test_name" == TestExplicitSnapshotGetter ]]; then
  KUBEBRAIN_TIKV_PD="$target_pd_csv" \
    go test -count=1 -run '^TestExplicitSnapshotGetter$' -v ./pkg/storage/tikv
elif [[ "$objectstore_integration" == true ]]; then
  if ! (cd hack/backup/objectstore && env \
    KUBEBRAIN_OBJECTSTORE_CANCEL_S3_ENDPOINT=http://127.0.0.1:49000 \
    KUBEBRAIN_OBJECTSTORE_CANCEL_S3_BUCKET=kubebrain-pitr \
    AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" AWS_REGION="$AWS_REGION" \
    go test -count=1 -run "^${test_name}$" -v ./internal/objectstore </dev/null); then
    docker logs "$minio_container" >&2 || true
    exit 1
  fi
elif ! env "${log_env[@]}" "${fault_env[@]}" \
  TMPDIR="$shared_dir" \
  KUBEBRAIN_NATIVE_PITR_SOURCE_PD="$source_pd_csv" \
  KUBEBRAIN_NATIVE_PITR_TARGET_PD="$target_pd_csv" \
  KUBEBRAIN_NATIVE_PITR_ETCDUTL="$etcdutl_bin" \
  KUBEBRAIN_NATIVE_PITR_BR="$drill_tmp/br" \
  KUBEBRAIN_NATIVE_PITR_SERVER="$drill_tmp/kubebrain" \
  KUBEBRAIN_NATIVE_PITR_ADMISSION="$drill_tmp/native-pitr-admission-fence" \
  KUBEBRAIN_NATIVE_PITR_RESTORE_PLAN="$drill_tmp/native-pitr-restore-plan" \
  KUBEBRAIN_NATIVE_PITR_SOURCE_CAPTURE="$drill_tmp/native-pitr-source-capture" \
  go test -count=1 -run "^${test_name}$" -v ./hack/backup/cmd/native-pitr-full-restore; then
  echo "source TiKV log follows" >&2
  docker logs "${source_tikv_names[0]}" >&2 || true
  exit 1
fi
