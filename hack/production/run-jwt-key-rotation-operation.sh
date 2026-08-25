#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
PUBLISHER_COMMAND="${PUBLISHER_COMMAND:-kubebrain-jwt-rotation-publisher}"; ROTATION_COMMAND="${ROTATION_COMMAND:-${ROOT_DIR}/hack/production/validate-jwt-key-rotation.sh}"
TOKEN_ISSUER_COMMAND="${TOKEN_ISSUER_COMMAND:-kubebrain-jwt-token-issuer}"; JWT_AUTH_CREDENTIAL_ROOT="${JWT_AUTH_CREDENTIAL_ROOT:-/var/run/secrets/kubebrain-jwt-auth}"
JWT_AUTH_USERNAME_FILE="${JWT_AUTH_USERNAME_FILE:-${JWT_AUTH_CREDENTIAL_ROOT}/username}"; JWT_AUTH_PASSWORD_FILE="${JWT_AUTH_PASSWORD_FILE:-${JWT_AUTH_CREDENTIAL_ROOT}/password}"
JQ="${JQ:-jq}"; DATE="${DATE:-date}"; SLEEP="${SLEEP:-sleep}"
MAX_PARAMETERS_BYTES=65536; MAX_EVIDENCE_BYTES=2097152

die() { echo "$*" >&2; exit 2; }
resolve() { if [[ "$1" == */* ]]; then [[ -f "$1" && -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and invalid"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations ]] || die "JWT key rotation executor is restricted to kubebrain-operations"
operation_is_positive_int64 "$LEASE_SECONDS" && (( LEASE_SECONDS >= 6 )) || die "LEASE_SECONDS must be an integer of at least 6"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
operation_is_positive_decimal_less_than_int "$heartbeat_interval" "$LEASE_SECONDS" || die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
command -v realpath >/dev/null && command -v sha256sum >/dev/null && command -v stat >/dev/null || die "realpath, sha256sum, and stat are required"
command -v sync >/dev/null || die "sync is required"
JQ="$(resolve "$JQ")" || die "jq is required"; DATE="$(resolve "$DATE")" || die "date is required"; SLEEP="$(resolve "$SLEEP")" || die "sleep is required"
PUBLISHER_COMMAND="$(resolve "$PUBLISHER_COMMAND")" || die "PUBLISHER_COMMAND is required and must be an executable file"; ROTATION_COMMAND="$(resolve "$ROTATION_COMMAND")" || die "ROTATION_COMMAND is required and must be an executable file"
TOKEN_ISSUER_COMMAND="$(resolve "$TOKEN_ISSUER_COMMAND")" || die "TOKEN_ISSUER_COMMAND is required and must be an executable file"
[[ -z "$OPERATIONCTL" ]] || OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"
workspace_root="$(realpath -e -- "$WORK_DIR")" || die "WORK_DIR cannot be resolved"
[[ "$workspace_root" == "$WORK_DIR" && -d "$workspace_root" && ! -L "$workspace_root" ]] || die "WORK_DIR must be a canonical non-symlink directory"
umask 077

operationctl=(); if [[ -n "$OPERATIONCTL" ]]; then operationctl=("$OPERATIONCTL"); else operationctl=(go run ./hack/production/cmd/operationctl); fi
kube_args=(--namespace "$OPERATION_NAMESPACE")
run_operationctl() { if [[ -n "$OPERATIONCTL" ]]; then "${operationctl[@]}" "${kube_args[@]}" "$@"; else (cd "$ROOT_DIR" && "${operationctl[@]}" "${kube_args[@]}" "$@"); fi; }
claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type JWTKeyRotation --lease "${LEASE_SECONDS}s")"
identity="$("$JQ" -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_sha256,.parameters_secret,.parameters_key,(.attempt|tostring)] | select(length == 11 and all(.[]; type == "string" and length > 0)) | @tsv' <<<"$claim")" || die "JWT key rotation claim identity is incomplete"
IFS=$'\t' read -r claimed_namespace name operation_id instance operation_type requester owner parameters_sha parameters_secret parameters_key attempt <<<"$identity"
[[ "$claimed_namespace" == kubebrain-operations && "$name" =~ ^jwt-key-rotate-[a-f0-9]{20}$ && "$operation_id" == "$name" && "$operation_type" == JWTKeyRotation && "$requester" == platform:jwt-key-rotation && "$owner" == "$WORKER_ID" && "$parameters_secret" == "${name}-parameters" && "$parameters_key" == parameters.json && "$parameters_sha" =~ ^[a-f0-9]{64}$ && "$attempt" =~ ^[1-5]$ && "$instance" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "JWT key rotation claim does not match the dedicated requester contract"

capture="$(mktemp -d)"; child=0; heartbeat_pid=0; fenced=false
cleanup() { [[ "$child" == 0 ]] || operation_kill_process_group "$child"; [[ "$heartbeat_pid" == 0 ]] || operation_kill_process_group "$heartbeat_pid"; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
digest() { sha256sum "$1" | cut -d ' ' -f1; }
secure_file() { local p="$1" max="$2" a; [[ -f "$p" && ! -L "$p" ]] || return 1; a="$(stat -Lc '%a:%h:%s' -- "$p")" || return 1; IFS=: read -r mode links size <<<"$a"; [[ "$mode" == 600 && "$links" == 1 && "$size" =~ ^[1-9][0-9]*$ ]] && (( size <= max )); }
retry() { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "$1" >/dev/null; echo "$1; JWT key rotation was requeued" >&2; exit 1; }
terminal_heartbeat() { run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { echo "final heartbeat failed; JWT key rotation worker was fenced" >&2; return 1; }; }

if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/managed-parameters.json"; run_operationctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" && ! -L "$PARAMETERS_INPUT" && "$(stat -Lc %s -- "$PARAMETERS_INPUT")" -le MAX_PARAMETERS_BYTES ]] || retry "operation parameters exceed ${MAX_PARAMETERS_BYTES} bytes"
[[ "$(digest "$PARAMETERS_INPUT")" == "$parameters_sha" ]] || retry "parameters digest mismatch"
cp -- "$PARAMETERS_INPUT" "$capture/parameters.json"; chmod 600 "$capture/parameters.json"
[[ "$(digest "$capture/parameters.json")" == "$parameters_sha" && "$(digest "$PARAMETERS_INPUT")" == "$parameters_sha" ]] || retry "parameters changed during capture"
PARAMETERS_INPUT="$capture/parameters.json"

schema='["data_kube_context","data_kubeconfig_path","endpoints","expected_replicas","jwt_ttl_seconds","key_mount_dir","key_secret","key_volume","kms_receipt_public_key_sha256","kubebrain_namespace","kubebrain_statefulset","max_clock_skew_seconds","new_key_export_receipt_sha256","new_key_field","new_key_material_key","new_key_sha256","new_key_version_id","old_key_export_receipt_sha256","old_key_field","old_key_material_key","old_key_sha256","old_key_version_id","probe_cacert_material_key","probe_cacert_sha256","probe_cert_material_key","probe_cert_sha256","probe_key_material_key","probe_key_sha256","probe_range_key","probe_server_name","receipt_output","request_id","sign_method","state_dir"]'
"$JQ" -e --argjson schema "$schema" '(keys|sort) == ($schema|sort) and .data_kube_context == "" and .data_kubeconfig_path == ""' "$PARAMETERS_INPUT" >/dev/null || die "JWT key rotation parameters must exactly match the in-cluster requester schema"
tsv="$("$JQ" -er '[.request_id,.state_dir,.receipt_output,.kubebrain_namespace,.kubebrain_statefulset,.key_secret,.old_key_field,.new_key_field,.old_key_material_key,.new_key_material_key,.old_key_version_id,.new_key_version_id,.kms_receipt_public_key_sha256,.old_key_export_receipt_sha256,.new_key_export_receipt_sha256,.old_key_sha256,.new_key_sha256,.key_volume,.key_mount_dir,.sign_method,(.expected_replicas|tostring),(.jwt_ttl_seconds|tostring),(.max_clock_skew_seconds|tostring),.probe_range_key,(if .probe_cacert_material_key=="" then "-" else .probe_cacert_material_key end),(if .probe_cert_material_key=="" then "-" else .probe_cert_material_key end),(if .probe_key_material_key=="" then "-" else .probe_key_material_key end),(if .probe_server_name=="" then "-" else .probe_server_name end),(if .probe_cacert_sha256=="" then "-" else .probe_cacert_sha256 end),(if .probe_cert_sha256=="" then "-" else .probe_cert_sha256 end),(if .probe_key_sha256=="" then "-" else .probe_key_sha256 end)] | select(length==31 and all(.[]; type=="string" and length>0)) | @tsv' "$PARAMETERS_INPUT")" || die "JWT key rotation parameters contain invalid values"
IFS=$'\t' read -r request_id state_dir receipt_output namespace statefulset key_secret old_field new_field old_material new_material old_version new_version kms_public_key_sha old_export_receipt_sha new_export_receipt_sha old_sha new_sha key_volume key_mount sign_method replicas ttl skew probe_range ca_material cert_material probe_key_material server ca_sha cert_sha probe_key_sha <<<"$tsv"
endpoints_json="$("$JQ" -cS '.endpoints' "$PARAMETERS_INPUT")"; endpoints="$("$JQ" -er 'join(",")' <<<"$endpoints_json")"
[[ "$namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ && "$statefulset" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ && "$key_secret" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "JWT data-plane object identity is invalid"
[[ "$key_secret" == "${operation_id}-keys" ]] || die "JWT key Secret does not match the immutable operation identity"
[[ "$request_id" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "JWT external request identity is invalid"
[[ "$old_material" == jwt-old-key && "$new_material" == jwt-new-key ]] || die "JWT key material fields do not match the broker allowlist"
key_version_re='^[A-Za-z0-9][A-Za-z0-9._:/@+=-]{0,511}$'
[[ "$old_version" =~ $key_version_re && "$new_version" =~ $key_version_re && "$old_version" != "$new_version" ]] || die "JWT key versions must be distinct canonical external KMS versions"
[[ "$kms_public_key_sha" =~ ^[a-f0-9]{64}$ && "$old_export_receipt_sha" =~ ^[a-f0-9]{64}$ && "$new_export_receipt_sha" =~ ^[a-f0-9]{64}$ && "$old_export_receipt_sha" != "$new_export_receipt_sha" ]] || die "JWT KMS export receipt provenance is invalid"
for value in "$old_field" "$new_field" "$key_volume"; do [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "JWT key field or volume identity is invalid"; done
[[ "$old_field" != "$new_field" && "$key_mount" == /* && "$key_mount" != / && "$(realpath -m -- "$key_mount")" == "$key_mount" ]] || die "JWT key mount binding is invalid"
[[ "$sign_method" == HS256 || "$sign_method" == RS256 || "$sign_method" == PS256 || "$sign_method" == ES256 || "$sign_method" == EdDSA ]] || die "JWT sign method is unsupported"
[[ "$old_sha" =~ ^[a-f0-9]{64}$ && "$new_sha" =~ ^[a-f0-9]{64}$ && -n "$probe_range" && "$probe_range" != *[[:cntrl:]]* ]] || die "JWT digest or probe range binding is invalid"
operation_is_positive_int64 "$replicas" && (( replicas <= 2147483647 )) && operation_is_positive_int64 "$ttl" && operation_is_nonnegative_int64 "$skew" && (( ttl + skew <= 2147483647 )) || die "JWT rotation numeric bounds are invalid"
"$JQ" -e --argjson replicas "$replicas" 'type=="array" and length==$replicas and length==(unique|length) and all(.[]; type=="string" and test("^https://[^[:space:]?#]+$") and (contains(",")|not) and length<=2048)' <<<"$endpoints_json" >/dev/null || die "JWT endpoint set is invalid"
[[ ( "$ca_material" == - && "$cert_material" == - && "$probe_key_material" == - && "$server" == - && "$ca_sha" == - && "$cert_sha" == - && "$probe_key_sha" == - ) || ( "$ca_material" == probe-ca && "$server" != - && "$server" != *[[:space:],]* && "$ca_sha" =~ ^[a-f0-9]{64}$ && ( ( "$cert_material" == - && "$probe_key_material" == - && "$cert_sha" == - && "$probe_key_sha" == - ) || ( "$cert_material" == probe-cert && "$probe_key_material" == probe-key && "$cert_sha" =~ ^[a-f0-9]{64}$ && "$probe_key_sha" =~ ^[a-f0-9]{64}$ ) ) ) ]] || die "JWT probe TLS material binding is invalid"

workspace_path() { local resolved; resolved="$(realpath -m -- "$1")" || die "$2 cannot be resolved"; [[ "$resolved" == "$workspace_root/"* ]] || die "$2 must resolve inside WORK_DIR"; printf '%s' "$resolved"; }
state_dir="$(workspace_path "$state_dir" state_dir)"; receipt_output="$(workspace_path "$receipt_output" receipt_output)"
[[ "$state_dir" == "$workspace_root/${name}.state" && "$receipt_output" == "$workspace_root/${name}.operation.receipt.json" ]] || die "JWT state and receipt paths do not match the requester contract"
mkdir -p -- "$state_dir"; [[ -d "$state_dir" && ! -L "$state_dir" ]] || die "JWT state directory is unsafe"

run_step() {
  local label="$1"; shift; local step_rc heartbeat_rc
  rm -f "$capture/child.done"; set -m; "$@" & child=$!
  (while true; do "$SLEEP" "$heartbeat_interval"; if ! run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null; then [[ -e "$capture/child.done" ]] || operation_kill_process_group "$child"; exit 75; fi; done) & heartbeat_pid=$!; set +m
  set +e; wait "$child"; step_rc=$?; : >"$capture/child.done"; operation_kill_process_group "$heartbeat_pid"; wait "$heartbeat_pid"; heartbeat_rc=$?; set -e
  child=0; heartbeat_pid=0; if [[ "$heartbeat_rc" == 75 ]]; then fenced=true; echo "operation heartbeat failed during ${label}" >&2; return 75; fi; return "$step_rc"
}
run_or_retry() { local label="$1"; shift; local rc=0; run_step "$label" "$@" || rc=$?; [[ "$fenced" == false ]] || exit 1; (( rc == 0 )) || { terminal_heartbeat || exit 1; retry "${label} exited ${rc}"; }; }
fetch_material_command() { local material_key="$1" material_version="$2" target="$3"; local version_args=(); [[ "$material_version" == - ]] || version_args=(--material-version "$material_version"); run_operationctl --action material --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --material-key "$material_key" "${version_args[@]}" >"$target"; }
fetch_material() { local material_key="$1" material_version="$2" target="$3" expected="$4" label="$5"; run_or_retry "fetch ${label} material" fetch_material_command "$material_key" "$material_version" "$target"; chmod 600 "$target"; secure_file "$target" 1048576 && [[ "$expected" =~ ^[a-f0-9]{64}$ && "$(digest "$target")" == "$expected" ]] || retry "${label} material digest mismatch"; }
old_source="$capture/key-old"; new_source="$capture/key-new"
fetch_material "$old_material" "$old_version" "$old_source" "$old_sha" "old JWT key"
fetch_material "$new_material" "$new_version" "$new_source" "$new_sha" "new JWT key"
	gate_tls_env=()
if [[ "$ca_material" != - ]]; then
  ca="$capture/tls-ca"; fetch_material "$ca_material" - "$ca" "$ca_sha" "probe CA"; gate_tls_env+=("PROBE_CACERT=$ca" "PROBE_SERVER_NAME=$server")
  if [[ "$cert_material" != - ]]; then cert="$capture/tls-cert"; probe_key="$capture/tls-key"; fetch_material "$cert_material" - "$cert" "$cert_sha" "probe certificate"; fetch_material "$probe_key_material" - "$probe_key" "$probe_key_sha" "probe private key"; gate_tls_env+=("PROBE_CERT=$cert" "PROBE_KEY=$probe_key"); else cert=-; probe_key=-; fi
else ca=-; cert=-; probe_key=-; [[ "$server" == - && "$ca_sha" == - && "$cert_sha" == - && "$probe_key_sha" == - ]] || die "plaintext probe parameters are inconsistent"; fi
issuer_endpoint="$("$JQ" -er '.[0]' <<<"$endpoints_json")" || die "JWT issuer endpoint is missing"
issuer_tls_args=(); if [[ "$ca" != - ]]; then issuer_tls_args+=(--cacert "$ca" --server-name "$server"); if [[ "$cert" != - ]]; then issuer_tls_args+=(--cert "$cert" --key "$probe_key"); fi; fi

issue_token() { local output="$1" label="$2"; if [[ -e "$output" ]]; then secure_file "$output" 1048576 || die "existing ${label} token is unsafe"; return; fi; run_or_retry "issue ${label} token" env JWT_TOKEN_OUTPUT="$output" "$TOKEN_ISSUER_COMMAND" --endpoint "$issuer_endpoint" --credential-root "$JWT_AUTH_CREDENTIAL_ROOT" --username-file "$JWT_AUTH_USERNAME_FILE" --password-file "$JWT_AUTH_PASSWORD_FILE" --output "$output" "${issuer_tls_args[@]}"; secure_file "$output" 1048576 || retry "${label} token issuer did not publish a secure token"; }

old_token="$state_dir/${name}.old.jwt"; b_token="$state_dir/${name}.phase-b-new.jwt"; c_token="$state_dir/${name}.phase-c-new.jwt"
a_publish="$state_dir/${name}.phase-a.publish.json"; b_publish="$state_dir/${name}.phase-b.publish.json"; c_publish="$state_dir/${name}.phase-c.publish.json"
a_gate="$state_dir/${name}.phase-a.json"; b_gate="$state_dir/${name}.phase-b.json"; c_gate="$state_dir/${name}.receipt.json"
publisher_common=(--operation-id "$operation_id" --instance "$instance" --namespace "$namespace" --statefulset "$statefulset" --key-secret "$key_secret" --old-key-field "$old_field" --new-key-field "$new_field" --old-key-source "$old_source" --new-key-source "$new_source" --old-key-sha256 "$old_sha" --new-key-sha256 "$new_sha" --key-volume "$key_volume" --key-mount-dir "$key_mount" --sign-method "$sign_method" --expected-replicas "$replicas" --jwt-ttl-seconds "$ttl")
gate_common=("ROTATION_ID=$operation_id" "INSTANCE=$instance" "STATE_DIR=$state_dir" "KUBEBRAIN_NAMESPACE=$namespace" "KUBEBRAIN_STATEFULSET=$statefulset" "KEY_SECRET=$key_secret" "OLD_KEY_FIELD=$old_field" "NEW_KEY_FIELD=$new_field" "ENDPOINTS=$endpoints" "OLD_TOKEN_FILE=$old_token" "JWT_TTL_SECONDS=$ttl" "MAX_CLOCK_SKEW_SECONDS=$skew" "EXPECTED_REPLICAS=$replicas" "PROBE_RANGE_KEY=$probe_range" "${gate_tls_env[@]}")

run_or_retry "phase A publish" "$PUBLISHER_COMMAND" --phase phase-a --receipt-output "$a_publish" "${publisher_common[@]}"
issue_token "$old_token" old
run_or_retry "phase A gate" env "${gate_common[@]}" ACTION=phase-a "$ROTATION_COMMAND"
run_or_retry "phase B publish" "$PUBLISHER_COMMAND" --phase phase-b --previous-receipt "$a_publish" --receipt-output "$b_publish" "${publisher_common[@]}"
issue_token "$b_token" phase-b-new
run_or_retry "phase B gate" env "${gate_common[@]}" NEW_TOKEN_FILE="$b_token" ACTION=phase-b "$ROTATION_COMMAND"
secure_file "$a_gate" "$MAX_EVIDENCE_BYTES" && secure_file "$b_gate" "$MAX_EVIDENCE_BYTES" || retry "phase A/B gate receipt is missing or unsafe"
"$JQ" -e --arg operation "$operation_id" --arg instance "$instance" --argjson endpoints "$endpoints_json" --arg a_sha "$(digest "$a_gate")" --arg old_token "$(digest "$old_token")" --arg new_token "$(digest "$b_token")" --argjson ttl "$ttl" --argjson skew "$skew" '
  keys==["all_members_accept_new","all_members_accept_old","earliest_retirement_at_unix","endpoints","format","instance","jwt_ttl_seconds","max_clock_skew_seconds","new_token_sha256","observed_at_unix","old_token_sha256","phase_a_sha256","rotation_id","secret","statefulset"] and
  .format=="kubebrain.jwt-key-rotation.phase-b.v1" and .rotation_id==$operation and .instance==$instance and .endpoints==$endpoints and .phase_a_sha256==$a_sha and .old_token_sha256==$old_token and .new_token_sha256==$new_token and
  .jwt_ttl_seconds==$ttl and .max_clock_skew_seconds==$skew and .all_members_accept_old==true and .all_members_accept_new==true and
  (.observed_at_unix|type=="number" and .>0 and .==floor) and .earliest_retirement_at_unix==(.observed_at_unix+$ttl+$skew) and
  (.statefulset|keys==["generation","revision","uid"]) and (.statefulset.uid|type=="string" and length>0) and (.statefulset.revision|type=="string" and length>0) and
  (.secret|keys==["data_sha256","resource_version","uid"]) and (.secret.uid|type=="string" and length>0) and (.secret.resource_version|type=="string" and length>0) and (.secret.data_sha256|test("^[a-f0-9]{64}$"))' "$b_gate" >/dev/null || retry "phase B gate receipt is invalid"
earliest="$("$JQ" -er '.earliest_retirement_at_unix | select(type=="number" and .>0 and .==floor)' "$b_gate")" || retry "phase B gate receipt is invalid"
wait_until() { local now; while true; do now="$("$DATE" +%s)"; [[ "$now" =~ ^[1-9][0-9]*$ ]] || return 2; (( now >= earliest )) && return 0; "$SLEEP" "$((earliest-now > 5 ? 5 : earliest-now))"; done; }
run_or_retry "JWT retirement wait" wait_until
phase_c_rc=0
run_step "phase C publish" "$PUBLISHER_COMMAND" --phase phase-c --previous-receipt "$b_publish" --receipt-output "$c_publish" "${publisher_common[@]}" || phase_c_rc=$?
[[ "$fenced" == false ]] || exit 1
if (( phase_c_rc != 0 )); then
  # A timeout may race with a rollout that actually converged. Reconcile once
  # before deciding that the unreceipted phase must be rolled back.
  phase_c_recheck_rc=0
  run_step "phase C convergence recheck" "$PUBLISHER_COMMAND" --phase phase-c --previous-receipt "$b_publish" --receipt-output "$c_publish" "${publisher_common[@]}" || phase_c_recheck_rc=$?
  [[ "$fenced" == false ]] || exit 1
  if (( phase_c_recheck_rc != 0 )); then
    rollback_rc=0
    run_step "phase C rollback to B" "$PUBLISHER_COMMAND" --phase phase-c --rollback-phase-c-to-b --previous-receipt "$b_publish" --receipt-output "$c_publish" "${publisher_common[@]}" || rollback_rc=$?
    [[ "$fenced" == false ]] || exit 1
    terminal_heartbeat || exit 1
    (( rollback_rc == 0 )) || retry "phase C publish failed and rollback to B exited ${rollback_rc}"
    retry "phase C publish exited ${phase_c_rc}; overlap phase B was restored"
  fi
fi
issue_token "$c_token" phase-c-new
run_or_retry "phase C gate" env "${gate_common[@]}" NEW_TOKEN_FILE="$c_token" ACTION=phase-c "$ROTATION_COMMAND"

evidence=("$a_publish" "$b_publish" "$c_publish" "$a_gate" "$b_gate" "$c_gate"); evidence_sha=()
for i in "${!evidence[@]}"; do
  source="${evidence[$i]}"; secure_file "$source" "$MAX_EVIDENCE_BYTES" || retry "JWT rotation evidence is missing or unsafe"
  before="$(digest "$source")"; frozen="$capture/evidence-$i"; cp -- "$source" "$frozen"; chmod 600 "$frozen"
  [[ "$(digest "$frozen")" == "$before" && "$(digest "$source")" == "$before" ]] || retry "JWT rotation evidence changed during capture"
  evidence_sha+=("$before")
done
a_publish_sha="${evidence_sha[0]}"; b_publish_sha="${evidence_sha[1]}"; c_publish_sha="${evidence_sha[2]}"; a_gate_sha="${evidence_sha[3]}"; b_gate_sha="${evidence_sha[4]}"; c_gate_sha="${evidence_sha[5]}"
operation_uid="$("$JQ" -er '.uid | select(type=="string" and length>0)' <<<"$claim")" || die "JWT key rotation claim UID is missing"
validate_composite() {
  "$JQ" -e --arg request "$request_id" --arg operation "$operation_id" --arg instance "$instance" --arg uid "$operation_uid" --argjson attempt "$attempt" --arg parameters "$parameters_sha" --arg old_version "$old_version" --arg new_version "$new_version" --arg ap "$a_publish_sha" --arg bp "$b_publish_sha" --arg cp "$c_publish_sha" --arg ag "$a_gate_sha" --arg bg "$b_gate_sha" --arg cg "$c_gate_sha" '
    keys == ["attempt","completed_at_unix","format","instance","new_key_version_id","old_key_version_id","operation_id","operation_uid","parameters_sha256","phase_a_gate_receipt_sha256","phase_a_publish_receipt_sha256","phase_b_gate_receipt_sha256","phase_b_publish_receipt_sha256","phase_c_gate_receipt_sha256","phase_c_publish_receipt_sha256","request_id"] and
    .format=="kubebrain.jwt-key-rotation.operation.receipt.v3" and .request_id==$request and .operation_id==$operation and .instance==$instance and .operation_uid==$uid and (.attempt|type=="number" and .>=1 and .<=$attempt and .==floor) and .parameters_sha256==$parameters and .old_key_version_id==$old_version and .new_key_version_id==$new_version and
    .phase_a_publish_receipt_sha256==$ap and .phase_b_publish_receipt_sha256==$bp and .phase_c_publish_receipt_sha256==$cp and .phase_a_gate_receipt_sha256==$ag and .phase_b_gate_receipt_sha256==$bg and .phase_c_gate_receipt_sha256==$cg and
    (.completed_at_unix|type=="number" and .>0 and .==floor)' "$receipt_output" >/dev/null
}
if [[ -e "$receipt_output" ]]; then secure_file "$receipt_output" "$MAX_EVIDENCE_BYTES" && validate_composite || retry "existing composite receipt drifted"; else
  composite="$capture/composite.json"
  "$JQ" -cnS --arg format kubebrain.jwt-key-rotation.operation.receipt.v3 --arg request "$request_id" --arg operation "$operation_id" --arg instance "$instance" --arg uid "$operation_uid" --argjson attempt "$attempt" --arg parameters "$parameters_sha" --arg old_version "$old_version" --arg new_version "$new_version" --arg ap "$a_publish_sha" --arg bp "$b_publish_sha" --arg cp "$c_publish_sha" --arg ag "$a_gate_sha" --arg bg "$b_gate_sha" --arg cg "$c_gate_sha" --argjson completed "$("$DATE" +%s)" '{format:$format,request_id:$request,operation_id:$operation,instance:$instance,operation_uid:$uid,attempt:$attempt,parameters_sha256:$parameters,old_key_version_id:$old_version,new_key_version_id:$new_version,phase_a_publish_receipt_sha256:$ap,phase_b_publish_receipt_sha256:$bp,phase_c_publish_receipt_sha256:$cp,phase_a_gate_receipt_sha256:$ag,phase_b_gate_receipt_sha256:$bg,phase_c_gate_receipt_sha256:$cg,completed_at_unix:$completed}' >"$composite"
  chmod 600 "$composite"; sync -f "$composite"; ln "$composite" "$receipt_output" || retry "composite receipt no-clobber publish failed"; sync -f "$state_dir"; validate_composite || retry "published composite receipt is invalid"
fi
receipt_sha="$(digest "$receipt_output")"; terminal_heartbeat || exit 1
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "JWT key rotation completed" >/dev/null
trap - EXIT INT TERM; rm -rf -- "$capture"
