#!/usr/bin/env bash
# A fresh, bounded Linux Docker fixture; accepts no external database endpoints.
set -euo pipefail
if [[ $# -lt 1 || $# -gt 2 || "$1" != --allow-local-containers || ($# == 2 && "$2" != --race) ]]; then
  echo 'Usage: bash hack/backend-integration/run-real-local.sh --allow-local-containers [--race]' >&2
  exit 2
fi
race=()
if [[ $# == 2 ]]; then race=(-race); fi
# Preserve the report destination even when a signal trap runs inside a Docker
# command whose stdout is currently redirected to an ID/evidence file.
exec {report_fd}>&1
for tool in docker timeout jq curl openssl go mktemp; do
  command -v "$tool" >/dev/null || { echo "missing command: $tool" >&2; exit 1; }
done
[[ -S /var/run/docker.sock ]] || { echo 'local Docker socket required' >&2; exit 1; }
entry_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
root_dir="$(cd "$entry_dir/../.." && pwd -P)"
evidence="$(mktemp -d /tmp/kubebrain-real-protocol.XXXXXXXXXX)"
chmod 700 "$evidence"
owner="$(openssl rand -hex 16)"
network="kb-protocol-$owner"
pd_name="$network-pd"
tikv_name="$network-tikv"
label=io.kubebrain.local-protocol-owner
pd_image=pingcap/pd@sha256:b32c69d8b9cc08cead83649d54c58942c441492b459c4cf190cd8c4747bf85f3
tikv_image=pingcap/tikv@sha256:b88a0400113812aca789f31bfb793b7ce748710372f49ad13e68a59df19bc35e
docker_local() { timeout --signal=TERM --kill-after=5s 45s docker --host unix:///var/run/docker.sock "$@"; }
cleanup() {
  local result=$? failed=0 name inspect id
  trap - EXIT
  for name in "$tikv_name" "$pd_name"; do
    if inspect="$(docker_local container inspect "$name" 2>/dev/null)"; then
      id="$(jq -er --arg key "$label" --arg owner "$owner" \
        'select(length == 1) | .[0] | select(.Config.Labels[$key] == $owner) | .Id' <<<"$inspect")" || { failed=1; continue; }
      docker_local logs "$id" > "$evidence/$name.log" 2>&1 || failed=1
      if [[ "$result" != 0 ]]; then
        printf 'LOCAL_PROTOCOL_CONTAINER_LOG name=%s\n' "$name" >&"$report_fd"
        tail -n 100 "$evidence/$name.log" >&"$report_fd" || failed=1
      fi
      docker_local rm -f "$id" >/dev/null || failed=1
    else
      # A failed inspect is not proof of absence if the daemon is unavailable.
      inspect="$(docker_local container ls -a --format '{{.Names}}')" || { failed=1; continue; }
      if [[ $'\n'"$inspect"$'\n' == *$'\n'"$name"$'\n'* ]]; then failed=1; fi
    fi
  done
  if inspect="$(docker_local network inspect "$network" 2>/dev/null)"; then
    if id="$(jq -er --arg key "$label" --arg owner "$owner" \
      'select(length == 1) | .[0] | select(.Labels[$key] == $owner and (.Containers | type) == "object" and (.Containers | length) == 0) | .Id' <<<"$inspect")"; then
      # Earlier evidence/log errors still fail the run, but must not prevent
      # cleanup of an independently verified owned and empty network.
      docker_local network rm "$id" >/dev/null || failed=1
    else
      failed=1
    fi
  else
    inspect="$(docker_local network ls --format '{{.Name}}')" || failed=1
    if [[ $'\n'"$inspect"$'\n' == *$'\n'"$network"$'\n'* ]]; then failed=1; fi
  fi
  # Only this invocation's compiled executable, never logs or arbitrary entries.
  if [[ -f "$evidence/protocol.test" && ! -L "$evidence/protocol.test" ]]; then
    rm -- "$evidence/protocol.test" || failed=1
  fi
  printf 'LOCAL_PROTOCOL_END result=%s cleanup_failed=%s evidence=%s\n' "$result" "$failed" "$evidence" >&"$report_fd"
  if [[ "$failed" != 0 ]]; then exit 1; fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'LOCAL_PROTOCOL_START evidence=%s owner=%s\n' "$evidence" "$owner"
export GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8
cd "$root_dir"
timeout --signal=TERM --kill-after=10s 300s go test "${race[@]}" -c -o "$evidence/protocol.test" ./pkg/storage/tikv
for image in "$pd_image" "$tikv_image"; do
  docker_local image inspect "$image" >/dev/null # never pull a mutable tag
done
docker_local network create --internal --label "$label=$owner" "$network" > "$evidence/network-id"
# Let Docker allocate a non-overlapping subnet first, then make that allocation
# explicit. Older daemons reject static endpoint IPs on an implicit subnet.
# Release only our verified empty reservation by ID; a concurrent allocation
# makes the second create fail closed instead of selecting an unrelated network.
reservation="$(docker_local network inspect "$network")"
reservation_id="$(jq -er --arg key "$label" --arg owner "$owner" \
  'select(length == 1) | .[0] | select(.Labels[$key] == $owner and (.Containers | type) == "object" and (.Containers | length) == 0) | .Id' <<<"$reservation")"
subnet="$(jq -er '.[0].IPAM.Config | select(length == 1) | .[0].Subnet | select(test("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$"))' <<<"$reservation")"
gateway="$(jq -er '.[0].IPAM.Config | select(length == 1) | .[0].Gateway | select(test("^([0-9]{1,3}\\.){3}1$"))' <<<"$reservation")"
docker_local network rm "$reservation_id" >/dev/null
docker_local network create --internal --subnet "$subnet" --gateway "$gateway" \
  --label "$label=$owner" "$network" > "$evidence/network-id"
pd_ip="${gateway%.*}.2"
tikv_ip="${gateway%.*}.3"
pd_endpoint="$pd_ip:2379"
common=(--network "$network" --label "$label=$owner" --read-only --user 65532:65532
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --cpus 2
  --ulimit nofile=262144:262144
  --tmpfs /tmp:rw,nosuid,nodev,size=64m,uid=65532,gid=65532
  --tmpfs /data:rw,nosuid,nodev,size=2g,uid=65532,gid=65532)
docker_local create "${common[@]}" --ip "$pd_ip" --memory 1g --name "$pd_name" \
  --mount "type=bind,src=$entry_dir/local-pd.toml,dst=/fixture.toml,readonly" \
  "$pd_image" --name=pd --data-dir=/data --config=/fixture.toml \
  --client-urls=http://0.0.0.0:2379 --advertise-client-urls="http://$pd_ip:2379" \
  --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls="http://$pd_ip:2380" \
  --initial-cluster="pd=http://$pd_ip:2380" > "$evidence/pd-id"
pd_id="$(<"$evidence/pd-id")"
docker_local start "$pd_id" >/dev/null
# Advertise private bridge IPs reachable from the local host, never Docker-only DNS.
docker_local create "${common[@]}" --ip "$tikv_ip" --memory 2g --name "$tikv_name" \
  --mount "type=bind,src=$entry_dir/local-tikv.toml,dst=/fixture.toml,readonly" \
  "$tikv_image" --addr=0.0.0.0:20160 --advertise-addr="$tikv_ip:20160" --status-addr=0.0.0.0:20180 \
  --pd="$pd_endpoint" --data-dir=/data --config=/fixture.toml > "$evidence/tikv-id"
tikv_id="$(<"$evidence/tikv-id")"
docker_local start "$tikv_id" >/dev/null
deadline=$((SECONDS + 90))
until curl --noproxy '*' --fail --silent --show-error --max-time 3 "http://$pd_endpoint/pd/api/v1/stores" > "$evidence/stores.json" 2> "$evidence/readiness-error.log" &&
  jq -e '.count == 1 and .stores[0].store.state_name == "Up"' "$evidence/stores.json" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo 'isolated TiKV readiness timeout' >&2
    printf 'LOCAL_PROTOCOL_READINESS_FAILED pd_endpoint=%s\n' "$pd_endpoint" >&2
    tail -n 20 "$evidence/readiness-error.log" "$evidence/stores.json" >&2
    # Only fixture container state, not arbitrary inspect output or host config.
    for name in "$pd_name" "$tikv_name"; do
      docker_local container inspect "$name" | jq -c \
        '.[0] | {name: .Name, state: {status: .State.Status, exit_code: .State.ExitCode, oom_killed: .State.OOMKilled, error: .State.Error}}' >&2 || true
    done
    exit 1
  fi
  sleep 1
done
curl --noproxy '*' --fail --silent --show-error --max-time 5 \
  "http://$pd_endpoint/pd/api/v1/cluster" > "$evidence/cluster.json"
cluster_id="$(jq -Rser '[match("\"id\"\\s*:\\s*([0-9]+)"; "g").captures[0].string] | select(length == 1) | .[0]' "$evidence/cluster.json")"
verify_case_result() {
  local result="$1" name="$2" log="$3"
  if [[ "$result" != 0 ]]; then
    printf 'LOCAL_PROTOCOL_CASE_FAILED test=%s exit=%s\n' "$name" "$result" >&2
    tail -n 100 "$log" >&2
    return "$result"
  fi
  if ! grep -Fq -- "--- PASS: $name (" "$log" || grep -Eq -- '^--- (SKIP|FAIL):' "$log"; then
    printf 'LOCAL_PROTOCOL_CASE_UNPROVEN test=%s\n' "$name" >&2
    tail -n 100 "$log" >&2
    return 1
  fi
  printf 'LOCAL_PROTOCOL_CASE_PASSED test=%s\n' "$name"
}
for test_name in TestRealTiKVProtocolSmoke TestRealTiKVOnePCResponseLoss \
  TestRealTiKVOnePCCancelAfterResponseLoss TestRealTiKVBackendResolvesCancelledOnePC \
  TestRealTiKVBackendResolvesUndeliveredOnePC TestRealTiKVBackendRetriesCommittedOnePC \
  TestRealTiKVBackendRetriesUndeliveredOnePC TestRealTiKVBackendRegionSplitFallback \
  TestRealTiKVBackendNoRPCRetryCommittedOnePC TestRealTiKVBackendNoRPCRetryUndeliveredOnePC \
  TestRealTiKVReadBypassesPendingSecondaryCleanup; do
  nonce="$(openssl rand -hex 16)"
  protocol_mode=1pc
  if [[ "$test_name" == TestRealTiKVReadBypassesPendingSecondaryCleanup ]]; then protocol_mode=2pc; fi
  case_result=0
  KUBEBRAIN_TIKV_PROTOCOL_PD="$pd_endpoint" KUBEBRAIN_TIKV_PROTOCOL_CLUSTER_ID="$cluster_id" \
    KUBEBRAIN_TIKV_PROTOCOL_ALLOW_REGION_SPLIT=1 \
    KUBEBRAIN_TIKV_PROTOCOL_ENABLE_TEST_FAILPOINTS=1 \
    KUBEBRAIN_TIKV_PROTOCOL_PREFIX="kubebrain/protocol-smoke/$nonce/" KUBEBRAIN_TIKV_PROTOCOL_MODE="$protocol_mode" \
    timeout --signal=TERM --kill-after=10s 130s "$evidence/protocol.test" \
    -test.run="^$test_name$" -test.v -test.count=1 -test.timeout=120s > "$evidence/$test_name.log" 2>&1 || case_result=$?
  verify_case_result "$case_result" "$test_name" "$evidence/$test_name.log"
done
echo 'LOCAL_PROTOCOL_TESTS_PASSED acceptance=false'
