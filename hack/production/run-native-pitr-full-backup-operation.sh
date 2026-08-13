#!/usr/bin/env bash
set -euo pipefail

WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-/usr/local/bin/kubebrain-operationctl}"
BACKUP_COMMAND="${BACKUP_COMMAND:-/usr/local/bin/kubebrain-native-pitr-full-backup}"
BR_BINARY="${BR_BINARY:-/usr/local/bin/br}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
ENCRYPTION_DIR="${ENCRYPTION_DIR:-/var/run/secrets/kubebrain-native-pitr-encryption}"
JQ="${JQ:-jq}"
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ && "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); fi
[[ "$HEARTBEAT_INTERVAL_SECONDS" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] &&
  awk -v heartbeat="$HEARTBEAT_INTERVAL_SECONDS" -v lease="$LEASE_SECONDS" 'BEGIN { exit !(heartbeat > 0 && heartbeat < lease) }' ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS"
[[ -x "$OPERATIONCTL" && -x "$BACKUP_COMMAND" && -x "$BR_BINARY" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
sha() { sha256sum "$1" | cut -d ' ' -f1; }

claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRFullBackup --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "native PITR claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^native-pitr-full-[a-f0-9]{20}$ && "$operation_id" == "$name" &&
  "$instance" == kubebrain && "$type" == NativePITRFullBackup && "$requester" == platform:native-pitr-full-backup &&
  "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" && "$key" == parameters.json &&
  "$attempt" =~ ^[1-9][0-9]*$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "native PITR claim identity is invalid"
[[ "$name" == "native-pitr-full-${expected_sha:0:20}" ]] || die "native PITR operation name does not bind the parameter digest"

capture="$(mktemp -d "$WORK_DIR/native-pitr-full.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || kill "$child" 2>/dev/null || true; [[ $heartbeat == 0 ]] || kill "$heartbeat" 2>/dev/null || true; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
finalize_heartbeat() {
  local hrc=0
  if [[ $heartbeat != 0 ]]; then
    kill "$heartbeat" 2>/dev/null || true
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    [[ $hrc != 75 ]] || { echo "operation heartbeat failed; native PITR worker was fenced" >&2; return 1; }
  fi
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; native PITR worker was fenced" >&2; return 1;
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
$JQ -e '(keys==["backup_ts","pd_addrs","storage_prefix"] or
  keys==["backup_ts","cipher_method","encryption_key_id","pd_addrs","storage_prefix"]) and
  (.backup_ts|type=="string" and test("^[1-9][0-9]*$")) and
  (.storage_prefix|type=="string" and startswith("s3://")) and
  (.pd_addrs|type=="array" and length>0 and length<=32 and all(.[]; type=="string" and length>0 and (contains(",")|not))) and
  ((has("cipher_method")|not) or (.cipher_method=="aes256-ctr" and
    (.encryption_key_id|type=="string" and test("^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$"))))' "$params" >/dev/null || die "native PITR parameter schema is invalid"
backup_ts="$($JQ -r .backup_ts "$params")"; storage_prefix="$($JQ -r .storage_prefix "$params")"
pd_addrs="$($JQ -r '.pd_addrs|join(",")' "$params")"
encryption_args=()
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  encryption_key_id="$($JQ -r .encryption_key_id "$params")"
  [[ -f "$ENCRYPTION_DIR/key" && -f "$ENCRYPTION_DIR/key-id" ]] || die "native PITR encryption key and key-id files are required"
  mounted_key_id="$(<"$ENCRYPTION_DIR/key-id")"
  [[ "$mounted_key_id" == "$encryption_key_id" ]] || die "native PITR encryption key version does not match operation parameters"
  encryption_args=(--crypter-method=aes256-ctr --encryption-key-id="$encryption_key_id" --encryption-key-file="$ENCRYPTION_DIR/key")
fi
attestation="$WORK_DIR/${name}.native-pitr-full-backup-attestation.json"
[[ ! -e "$attestation" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR attestation already exists; inspect before retry" >/dev/null; exit 1; }
for file in ca.crt tls.crt tls.key; do [[ -f "$TLS_DIR/$file" ]] || die "native PITR TLS file $file is required"; done

"$BACKUP_COMMAND" --br-binary="$BR_BINARY" --pd-addrs="$pd_addrs" --storage-prefix="$storage_prefix" \
  --backup-ts="$backup_ts" --ca="$TLS_DIR/ca.crt" --cert="$TLS_DIR/tls.crt" --key="$TLS_DIR/tls.key" \
  "${encryption_args[@]}" \
  --attestation-output="$attestation" >"$capture/backup.log" 2>&1 & child=$!
( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || kill "$child" 2>/dev/null || true; exit 75; }; done ) & heartbeat=$!
set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
[[ $rc == 0 && -s "$attestation" ]] || { finalize_heartbeat || exit 1; runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR full backup exited ${rc}; inspect immutable prefix before a new operation" >/dev/null; exit 1; }
receipt_sha="$(sha "$attestation")"; finalize_heartbeat || exit 1
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "native PITR full backup completed" >/dev/null
