#!/usr/bin/env bash
set -euo pipefail

ACTION="${ACTION:-}"
ROTATION_ID="${ROTATION_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
KEY_SECRET="${KEY_SECRET:-}"
OLD_KEY_FIELD="${OLD_KEY_FIELD:-old-key}"
NEW_KEY_FIELD="${NEW_KEY_FIELD:-new-key}"
ENDPOINTS="${ENDPOINTS:-}"
OLD_TOKEN_FILE="${OLD_TOKEN_FILE:-}"
NEW_TOKEN_FILE="${NEW_TOKEN_FILE:-}"
JWT_TTL_SECONDS="${JWT_TTL_SECONDS:-}"
MAX_CLOCK_SKEW_SECONDS="${MAX_CLOCK_SKEW_SECONDS:-}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
DATE="${DATE:-date}"
TOKEN_PROBE="${TOKEN_PROBE:-kubebrain-jwt-token-probe}"
PROBE_CACERT="${PROBE_CACERT:-}"
PROBE_CERT="${PROBE_CERT:-}"
PROBE_KEY="${PROBE_KEY:-}"
PROBE_SERVER_NAME="${PROBE_SERVER_NAME:-}"
PROBE_RANGE_KEY="${PROBE_RANGE_KEY:-/kubebrain/jwt-rotation/probe}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10s}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"

die() { echo "$*" >&2; exit 1; }
usage() {
  cat >&2 <<'EOF'
Usage: ACTION=phase-a|phase-b|phase-c ROTATION_ID=<id> INSTANCE=<instance> \
  STATE_DIR=<durable-dir> KEY_SECRET=<immutable-secret> ENDPOINTS=<csv> \
  OLD_TOKEN_FILE=<file> NEW_TOKEN_FILE=<file-for-b/c> JWT_TTL_SECONDS=<n> \
  MAX_CLOCK_SKEW_SECONDS=<n> hack/production/validate-jwt-key-rotation.sh

TOKEN_PROBE is invoked as: TOKEN_PROBE --endpoint URL --token-file FILE
and must return zero only when that exact token is accepted. Phase C expects
the old-token invocation to fail, then rechecks the new token immediately.
EOF
  exit 2
}

for name in ACTION ROTATION_ID INSTANCE STATE_DIR KEY_SECRET ENDPOINTS OLD_TOKEN_FILE JWT_TTL_SECONDS MAX_CLOCK_SKEW_SECONDS; do
  [[ -n "${!name}" ]] || { echo "$name is required" >&2; usage; }
done
[[ "$ACTION" =~ ^phase-[abc]$ ]] || { echo "ACTION must be phase-a, phase-b, or phase-c" >&2; exit 2; }
for value_name in ROTATION_ID INSTANCE; do
  [[ "${!value_name}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "$value_name is invalid"
done
for value_name in KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET KEY_SECRET OLD_KEY_FIELD NEW_KEY_FIELD; do
  [[ "${!value_name}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "$value_name is invalid"
done
[[ "$OLD_KEY_FIELD" != "$NEW_KEY_FIELD" ]] || die "OLD_KEY_FIELD and NEW_KEY_FIELD must differ"
[[ "$EXPECTED_REPLICAS" =~ ^[1-9][0-9]*$ ]] && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
for value_name in JWT_TTL_SECONDS MAX_CLOCK_SKEW_SECONDS; do
  [[ "${!value_name}" =~ ^(0|[1-9][0-9]*)$ ]] && (( ${!value_name} <= 2147483647 )) || die "$value_name must be a canonical non-negative int32"
done
(( JWT_TTL_SECONDS > 0 )) || die "JWT_TTL_SECONDS must be positive"
[[ -f "$OLD_TOKEN_FILE" ]] || die "OLD_TOKEN_FILE does not exist"
if [[ "$ACTION" != phase-a ]]; then
  [[ -n "$NEW_TOKEN_FILE" && -f "$NEW_TOKEN_FILE" ]] || die "NEW_TOKEN_FILE is required for $ACTION"
fi
[[ -z "$PROBE_CACERT" || -f "$PROBE_CACERT" ]] || die "PROBE_CACERT does not exist"
[[ -z "$PROBE_CERT" || -f "$PROBE_CERT" ]] || die "PROBE_CERT does not exist"
[[ -z "$PROBE_KEY" || -f "$PROBE_KEY" ]] || die "PROBE_KEY does not exist"
[[ -n "$PROBE_RANGE_KEY" && "$PROBE_RANGE_KEY" != *[[:cntrl:]]* ]] || die "PROBE_RANGE_KEY is invalid"
[[ "$PROBE_SERVER_NAME" != *[[:space:],]* && "$PROBE_SERVER_NAME" != *[[:cntrl:]]* ]] || die "PROBE_SERVER_NAME is invalid"
[[ -z "$PROBE_CERT" && -z "$PROBE_KEY" || -n "$PROBE_CERT" && -n "$PROBE_KEY" ]] || die "PROBE_CERT and PROBE_KEY must be provided together"
[[ -z "$PROBE_CACERT" && -z "$PROBE_CERT" && -z "$PROBE_KEY" && -z "$PROBE_SERVER_NAME" || -n "$PROBE_CACERT" ]] || die "TLS probe options require PROBE_CACERT"
command -v "$JQ" >/dev/null || die "jq is required"
command -v "$KUBECTL" >/dev/null || die "kubectl is required"
command -v "$TOKEN_PROBE" >/dev/null || die "TOKEN_PROBE is required"

IFS=',' read -r -a endpoints <<<"$ENDPOINTS"
[[ ${#endpoints[@]} -eq EXPECTED_REPLICAS ]] || die "ENDPOINTS must contain exactly EXPECTED_REPLICAS entries"
declare -A seen_endpoint=()
for endpoint in "${endpoints[@]}"; do
  [[ -n "$endpoint" && "$endpoint" != *[[:space:],]* && "$endpoint" != *[[:cntrl:]]* ]] || die "ENDPOINTS contains an invalid endpoint"
  [[ -z "${seen_endpoint[$endpoint]:-}" ]] || die "ENDPOINTS contains a duplicate endpoint"
  seen_endpoint[$endpoint]=1
done
endpoints_json="$(printf '%s\n' "${endpoints[@]}" | "$JQ" -Rsc 'split("\n")[:-1]')"

umask 077
mkdir -p "$STATE_DIR"
token_capture_dir="$(mktemp -d)"
trap 'rm -rf "$token_capture_dir"' EXIT INT TERM
freeze_evidence() {
  local source="$1" name="$2" before after current destination
  before="$(sha256sum "$source" | awk '{print $1}')"
  destination="${token_capture_dir}/${name}"
  cp -- "$source" "$destination"; chmod 600 "$destination"
  after="$(sha256sum "$destination" | awk '{print $1}')"
  current="$(sha256sum "$source" | awk '{print $1}')"
  [[ "$before" =~ ^[a-f0-9]{64}$ && "$before" == "$after" && "$before" == "$current" ]] || die "$name evidence changed while being captured"
  printf '%s\n' "$destination"
}
OLD_TOKEN_FILE="$(freeze_evidence "$OLD_TOKEN_FILE" old-token)"
if [[ "$ACTION" != phase-a ]]; then NEW_TOKEN_FILE="$(freeze_evidence "$NEW_TOKEN_FILE" new-token)"; fi
if [[ -n "$PROBE_CACERT" ]]; then PROBE_CACERT="$(freeze_evidence "$PROBE_CACERT" ca)"; fi
if [[ -n "$PROBE_CERT" ]]; then
  PROBE_CERT="$(freeze_evidence "$PROBE_CERT" client-cert)"
  PROBE_KEY="$(freeze_evidence "$PROBE_KEY" client-key)"
fi
probe_args=(--probe-key "$PROBE_RANGE_KEY" --timeout "$PROBE_TIMEOUT")
[[ -z "$PROBE_CACERT" ]] || probe_args+=(--cacert "$PROBE_CACERT")
[[ -z "$PROBE_CERT" ]] || probe_args+=(--cert "$PROBE_CERT" --key "$PROBE_KEY")
[[ -z "$PROBE_SERVER_NAME" ]] || probe_args+=(--server-name "$PROBE_SERVER_NAME")
phase_a="${STATE_DIR}/${ROTATION_ID}.phase-a.json"
phase_b="${STATE_DIR}/${ROTATION_ID}.phase-b.json"
phase_c="${STATE_DIR}/${ROTATION_ID}.receipt.json"
kubectl_args=()
[[ -z "$KUBE_CONTEXT" ]] || kubectl_args+=(--context "$KUBE_CONTEXT")
[[ -z "$KUBECONFIG_PATH" ]] || kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")

digest_file() { sha256sum "$1" | awk '{print $1}'; }
read_stable() {
  local kind="$1" name="$2" first second
  first="$(mktemp)"; second="$(mktemp)"
  "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get "$kind" "$name" -o json >"$first"
  "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get "$kind" "$name" -o json >"$second"
  if [[ "$(digest_file "$first")" != "$(digest_file "$second")" ]]; then rm -f "$first" "$second"; die "$kind/$name changed during capture"; fi
  rm -f "$second"
  printf '%s\n' "$first"
}
publish() {
  local temporary="$1" destination="$2"
  sync -f "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    cmp -s "$temporary" "$destination" || { rm -f "$temporary"; die "refusing to overwrite existing evidence: $destination"; }
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$destination")"
}
probe_all() {
  local token="$1" endpoint
  for endpoint in "${endpoints[@]}"; do "$TOKEN_PROBE" "${probe_args[@]}" --endpoint "$endpoint" --token-file "$token"; done
}
probe_old_rejected_and_new_live() {
  local endpoint
  for endpoint in "${endpoints[@]}"; do
    if "$TOKEN_PROBE" "${probe_args[@]}" --endpoint "$endpoint" --token-file "$OLD_TOKEN_FILE"; then
      die "old token is still accepted by $endpoint after phase C"
    fi
    "$TOKEN_PROBE" "${probe_args[@]}" --endpoint "$endpoint" --token-file "$NEW_TOKEN_FILE" ||
      die "new token failed after old-token rejection at $endpoint; endpoint outage is not retirement proof"
  done
}
snapshot() {
  local sts secret secret_data_sha sts_file secret_file
  sts_file="$(read_stable statefulset "$KUBEBRAIN_STATEFULSET")"
  secret_file="$(read_stable secret "$KEY_SECRET")"
  sts="$($JQ -ceS --argjson replicas "$EXPECTED_REPLICAS" '
    select(.metadata.uid|type=="string" and length>0) |
    select(.metadata.generation|type=="number" and .>0 and .==floor) |
    select(.status.observedGeneration==.metadata.generation) |
    select(.spec.replicas==$replicas and .status.readyReplicas==$replicas and .status.updatedReplicas==$replicas) |
    select(.status.currentRevision|type=="string" and length>0) |
    select(.status.currentRevision==.status.updateRevision) |
    {uid:.metadata.uid,generation:.metadata.generation,revision:.status.updateRevision}' "$sts_file")" || { rm -f "$sts_file" "$secret_file"; die "StatefulSet is not fully rolled out and Ready"; }
  secret_data_sha="$($JQ -ceS '.data' "$secret_file" | sha256sum | awk '{print $1}')"
  [[ "$secret_data_sha" =~ ^[a-f0-9]{64}$ ]] || { rm -f "$sts_file" "$secret_file"; die "failed to digest key Secret data"; }
  secret="$($JQ -ceS --arg old "$OLD_KEY_FIELD" --arg new "$NEW_KEY_FIELD" --arg data_sha "$secret_data_sha" '
    select(.immutable==true) | select(.metadata.uid|type=="string" and length>0) |
    select(.metadata.resourceVersion|type=="string" and length>0) |
    select(.data|type=="object" and has($old) and has($new)) |
    select(.data[$old]|type=="string" and length>0) | select(.data[$new]|type=="string" and length>0) |
    {uid:.metadata.uid,resource_version:.metadata.resourceVersion,data_sha256:$data_sha}' "$secret_file")" || { rm -f "$sts_file" "$secret_file"; die "key Secret must be immutable and contain both non-empty key fields"; }
  rm -f "$sts_file" "$secret_file"
  "$JQ" -cnS --argjson sts "$sts" --argjson secret "$secret" '{statefulset:$sts,secret:$secret}'
}
validate_prior() {
  local file="$1" format="$2"
  [[ -f "$file" && ! -L "$file" && "$(stat -c %a "$file")" == 600 ]] || die "$format evidence is missing or unsafe"
  [[ "$(wc -c <"$file")" -le 1048576 ]] || die "$format evidence exceeds 1 MiB"
  "$JQ" -e --arg format "$format" --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --argjson endpoints "$endpoints_json" '
    .format==$format and .instance==$instance and .rotation_id==$rotation and .endpoints==$endpoints and
    (.observed_at_unix|type=="number" and .>0 and .==floor) and
    (.statefulset|keys==["generation","revision","uid"]) and
    (.statefulset.uid|type=="string" and length>0) and (.statefulset.revision|type=="string" and length>0) and
    (.secret|keys==["data_sha256","resource_version","uid"]) and
    (.secret.uid|type=="string" and length>0) and (.secret.resource_version|type=="string" and length>0) and
    (.secret.data_sha256|type=="string" and test("^[a-f0-9]{64}$")) and
    if $format=="kubebrain.jwt-key-rotation.phase-a.v1" then
      keys==["all_members_accept_old","endpoints","format","instance","observed_at_unix","old_token_sha256","rotation_id","secret","statefulset"] and
      .all_members_accept_old==true and (.old_token_sha256|test("^[a-f0-9]{64}$"))
    else
      keys==["all_members_accept_new","all_members_accept_old","earliest_retirement_at_unix","endpoints","format","instance","jwt_ttl_seconds","max_clock_skew_seconds","new_token_sha256","observed_at_unix","old_token_sha256","phase_a_sha256","rotation_id","secret","statefulset"] and
      .all_members_accept_old==true and .all_members_accept_new==true and
      (.phase_a_sha256|test("^[a-f0-9]{64}$")) and (.old_token_sha256|test("^[a-f0-9]{64}$")) and (.new_token_sha256|test("^[a-f0-9]{64}$")) and
      (.jwt_ttl_seconds|type=="number" and .>0 and .==floor) and (.max_clock_skew_seconds|type=="number" and .>=0 and .==floor) and
      (.earliest_retirement_at_unix==(.observed_at_unix+.jwt_ttl_seconds+.max_clock_skew_seconds))
    end' "$file" >/dev/null || die "$format evidence is invalid"
}
same_binding() {
  local prior="$1" current="$2"
  "$JQ" -e --argjson current "$current" --argjson endpoints "$endpoints_json" '
    .statefulset.uid==$current.statefulset.uid and
    .secret==$current.secret and .endpoints==$endpoints' "$prior" >/dev/null || die "rotation object binding changed between phases"
}
same_revision() {
  local prior="$1" current="$2"
  [[ "$("$JQ" -r '.statefulset.revision' "$prior")" == "$("$JQ" -r '.statefulset.revision' <<<"$current")" ]] ||
    die "existing phase receipt does not match the current StatefulSet revision"
}
validate_final() {
  [[ -f "$phase_c" && ! -L "$phase_c" && "$(stat -c %a "$phase_c")" == 600 ]] || die "final receipt is missing or unsafe"
  [[ "$(wc -c <"$phase_c")" -le 1048576 ]] || die "final receipt exceeds 1 MiB"
  "$JQ" -e --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --argjson endpoints "$endpoints_json" '
    keys==["completed_at_unix","endpoints","format","instance","new_token_accepted","new_token_sha256","old_token_rejected","old_token_sha256","phase_a_sha256","phase_b_sha256","rotation_id","secret","statefulset"] and
    .format=="kubebrain.jwt-key-rotation.receipt.v1" and .instance==$instance and .rotation_id==$rotation and .endpoints==$endpoints and
    .old_token_rejected==true and .new_token_accepted==true and
    (.completed_at_unix|type=="number" and .>0 and .==floor) and
    (.phase_a_sha256|test("^[a-f0-9]{64}$")) and (.phase_b_sha256|test("^[a-f0-9]{64}$")) and
    (.old_token_sha256|test("^[a-f0-9]{64}$")) and (.new_token_sha256|test("^[a-f0-9]{64}$")) and
    (.statefulset|keys==["generation","revision","uid"]) and (.secret|keys==["data_sha256","resource_version","uid"]) and
    (.secret.data_sha256|test("^[a-f0-9]{64}$"))' "$phase_c" >/dev/null || die "final receipt is invalid"
}

now="$($DATE +%s)"
[[ "$now" =~ ^[1-9][0-9]*$ ]] || die "current time is invalid"
current="$(snapshot)"
case "$ACTION" in
  phase-a)
    [[ ! -e "$phase_b" && ! -e "$phase_c" ]] || die "later phase evidence already exists"
    if [[ -e "$phase_a" ]]; then
      validate_prior "$phase_a" kubebrain.jwt-key-rotation.phase-a.v1
      same_binding "$phase_a" "$current"; same_revision "$phase_a" "$current"
      [[ "$("$JQ" -r '.old_token_sha256' "$phase_a")" == "$(digest_file "$OLD_TOKEN_FILE")" ]] || die "old token changed since phase A"
      probe_all "$OLD_TOKEN_FILE"
      echo "JWT key rotation $ACTION gate passed: instance=$INSTANCE rotation=$ROTATION_ID"
      exit 0
    fi
    probe_all "$OLD_TOKEN_FILE"
    tmp="$(mktemp "${STATE_DIR}/.${ROTATION_ID}.phase-a.XXXXXX")"
    "$JQ" -cnS --arg format kubebrain.jwt-key-rotation.phase-a.v1 --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --argjson endpoints "$endpoints_json" --argjson at "$now" --argjson evidence "$current" --arg old_token_sha "$(digest_file "$OLD_TOKEN_FILE")" \
      '{format:$format,instance:$instance,rotation_id:$rotation,endpoints:$endpoints,observed_at_unix:$at,statefulset:$evidence.statefulset,secret:$evidence.secret,old_token_sha256:$old_token_sha,all_members_accept_old:true}' >"$tmp"
    publish "$tmp" "$phase_a" ;;
  phase-b)
    validate_prior "$phase_a" kubebrain.jwt-key-rotation.phase-a.v1
    [[ ! -e "$phase_c" ]] || die "phase C evidence already exists"
    same_binding "$phase_a" "$current"
    if [[ -e "$phase_b" ]]; then
      validate_prior "$phase_b" kubebrain.jwt-key-rotation.phase-b.v1
      same_binding "$phase_b" "$current"; same_revision "$phase_b" "$current"
      [[ "$("$JQ" -r '.phase_a_sha256' "$phase_b")" == "$(digest_file "$phase_a")" ]] || die "phase B does not bind the current phase A receipt"
      [[ "$("$JQ" -r '.old_token_sha256' "$phase_b")" == "$(digest_file "$OLD_TOKEN_FILE")" && "$("$JQ" -r '.new_token_sha256' "$phase_b")" == "$(digest_file "$NEW_TOKEN_FILE")" ]] || die "token evidence changed since phase B"
      [[ "$("$JQ" -r '.jwt_ttl_seconds' "$phase_b")" == "$JWT_TTL_SECONDS" && "$("$JQ" -r '.max_clock_skew_seconds' "$phase_b")" == "$MAX_CLOCK_SKEW_SECONDS" ]] || die "retirement timing inputs changed"
      probe_all "$OLD_TOKEN_FILE"; probe_all "$NEW_TOKEN_FILE"
      echo "JWT key rotation $ACTION gate passed: instance=$INSTANCE rotation=$ROTATION_ID"
      exit 0
    fi
    a_revision="$($JQ -r '.statefulset.revision' "$phase_a")"
    b_revision="$($JQ -r '.statefulset.revision' <<<"$current")"
    [[ "$a_revision" != "$b_revision" ]] || die "phase B must use a new StatefulSet revision"
    [[ "$($JQ -r '.old_token_sha256' "$phase_a")" == "$(digest_file "$OLD_TOKEN_FILE")" ]] || die "phase B must reuse the phase A old token"
    probe_all "$OLD_TOKEN_FILE"; probe_all "$NEW_TOKEN_FILE"
    phase_b_observed_at="$($DATE +%s)"
    [[ "$phase_b_observed_at" =~ ^[1-9][0-9]*$ ]] || die "phase B completion time is invalid"
    (( phase_b_observed_at <= 9223372036854775807 - JWT_TTL_SECONDS - MAX_CLOCK_SKEW_SECONDS )) || die "phase B retirement time overflows int64"
    earliest=$(( phase_b_observed_at + JWT_TTL_SECONDS + MAX_CLOCK_SKEW_SECONDS ))
    tmp="$(mktemp "${STATE_DIR}/.${ROTATION_ID}.phase-b.XXXXXX")"
    "$JQ" -cnS --arg format kubebrain.jwt-key-rotation.phase-b.v1 --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --argjson endpoints "$endpoints_json" --arg a_sha "$(digest_file "$phase_a")" --arg old_sha "$(digest_file "$OLD_TOKEN_FILE")" --arg new_sha "$(digest_file "$NEW_TOKEN_FILE")" --argjson at "$phase_b_observed_at" --argjson earliest "$earliest" --argjson ttl "$JWT_TTL_SECONDS" --argjson skew "$MAX_CLOCK_SKEW_SECONDS" --argjson evidence "$current" \
      '{format:$format,instance:$instance,rotation_id:$rotation,endpoints:$endpoints,phase_a_sha256:$a_sha,observed_at_unix:$at,earliest_retirement_at_unix:$earliest,jwt_ttl_seconds:$ttl,max_clock_skew_seconds:$skew,statefulset:$evidence.statefulset,secret:$evidence.secret,old_token_sha256:$old_sha,new_token_sha256:$new_sha,all_members_accept_old:true,all_members_accept_new:true}' >"$tmp"
    publish "$tmp" "$phase_b" ;;
  phase-c)
    validate_prior "$phase_a" kubebrain.jwt-key-rotation.phase-a.v1
    validate_prior "$phase_b" kubebrain.jwt-key-rotation.phase-b.v1
    same_binding "$phase_b" "$current"
    [[ "$($JQ -r '.phase_a_sha256' "$phase_b")" == "$(digest_file "$phase_a")" ]] || die "phase B does not bind the current phase A receipt"
    [[ "$($JQ -r '.jwt_ttl_seconds' "$phase_b")" == "$JWT_TTL_SECONDS" && "$($JQ -r '.max_clock_skew_seconds' "$phase_b")" == "$MAX_CLOCK_SKEW_SECONDS" ]] || die "retirement timing inputs changed"
    [[ "$($JQ -r '.old_token_sha256' "$phase_b")" == "$(digest_file "$OLD_TOKEN_FILE")" ]] || die "phase C must verify the old token recorded by phase B"
    if [[ -e "$phase_c" ]]; then
      validate_final; same_binding "$phase_c" "$current"; same_revision "$phase_c" "$current"
      [[ "$("$JQ" -r '.phase_a_sha256' "$phase_c")" == "$(digest_file "$phase_a")" && "$("$JQ" -r '.phase_b_sha256' "$phase_c")" == "$(digest_file "$phase_b")" ]] || die "final receipt does not bind the current phase receipts"
      [[ "$("$JQ" -r '.old_token_sha256' "$phase_c")" == "$(digest_file "$OLD_TOKEN_FILE")" && "$("$JQ" -r '.new_token_sha256' "$phase_c")" == "$(digest_file "$NEW_TOKEN_FILE")" ]] || die "final receipt token evidence changed"
      probe_all "$NEW_TOKEN_FILE"; probe_old_rejected_and_new_live
      echo "JWT key rotation $ACTION gate passed: instance=$INSTANCE rotation=$ROTATION_ID"
      exit 0
    fi
    b_revision="$($JQ -r '.statefulset.revision' "$phase_b")"; c_revision="$($JQ -r '.statefulset.revision' <<<"$current")"
    [[ "$b_revision" != "$c_revision" ]] || die "phase C must use a new StatefulSet revision"
    earliest="$($JQ -r '.earliest_retirement_at_unix' "$phase_b")"
    (( now >= earliest )) || die "phase C is too early; earliest retirement is $earliest"
    probe_all "$NEW_TOKEN_FILE"
    probe_old_rejected_and_new_live
    tmp="$(mktemp "${STATE_DIR}/.${ROTATION_ID}.receipt.XXXXXX")"
    "$JQ" -cnS --arg format kubebrain.jwt-key-rotation.receipt.v1 --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --argjson endpoints "$endpoints_json" --arg a_sha "$(digest_file "$phase_a")" --arg b_sha "$(digest_file "$phase_b")" --arg old_sha "$(digest_file "$OLD_TOKEN_FILE")" --arg new_sha "$(digest_file "$NEW_TOKEN_FILE")" --argjson at "$now" --argjson evidence "$current" \
      '{format:$format,instance:$instance,rotation_id:$rotation,endpoints:$endpoints,phase_a_sha256:$a_sha,phase_b_sha256:$b_sha,completed_at_unix:$at,statefulset:$evidence.statefulset,secret:$evidence.secret,old_token_sha256:$old_sha,new_token_sha256:$new_sha,old_token_rejected:true,new_token_accepted:true}' >"$tmp"
    publish "$tmp" "$phase_c" ;;
esac
echo "JWT key rotation $ACTION gate passed: instance=$INSTANCE rotation=$ROTATION_ID"
