#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

ACTION="${ACTION:-}"
ROTATION_ID="${ROTATION_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
INFO_ENDPOINT="${INFO_ENDPOINT:-}"
INFO_SERVER_NAME="${INFO_SERVER_NAME:-}"
OLD_INFO_CACERT="${OLD_INFO_CACERT:-}"
OLD_INFO_CERT="${OLD_INFO_CERT:-}"
NEW_INFO_CACERT="${NEW_INFO_CACERT:-}"
NEW_INFO_CERT="${NEW_INFO_CERT:-}"
REQUIRE_OLD_CA_REJECTION="${REQUIRE_OLD_CA_REJECTION:-false}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
POD_SELECTOR="${POD_SELECTOR:-app.kubernetes.io/name=kubebrain}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
KUBECTL="${KUBECTL:-kubectl}"
OPENSSL="${OPENSSL:-openssl}"
JQ="${JQ:-jq}"
LN="${LN:-ln}"
SYNC="${SYNC:-sync}"
MKTEMP="${MKTEMP:-mktemp}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
MAX_CREDENTIAL_BYTES=1048576
MAX_STATE_BYTES=2097152
MAX_RECEIPT_BYTES=1048576

for variable in ACTION ROTATION_ID INSTANCE STATE_DIR INFO_ENDPOINT INFO_SERVER_NAME OLD_INFO_CACERT OLD_INFO_CERT NEW_INFO_CACERT NEW_INFO_CERT; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
command -v "$LN" >/dev/null || { echo "ln is required" >&2; exit 2; }
command -v "$SYNC" >/dev/null || { echo "sync is required" >&2; exit 2; }
command -v "$MKTEMP" >/dev/null || { echo "mktemp is required" >&2; exit 2; }
[[ "$ACTION" == begin || "$ACTION" == complete || "$ACTION" == verify ]] || { echo "ACTION must be begin, complete, or verify" >&2; exit 2; }
[[ "$ROTATION_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || { echo "ROTATION_ID is invalid" >&2; exit 2; }
[[ "$INSTANCE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || { echo "INSTANCE is invalid" >&2; exit 2; }
[[ "$KUBEBRAIN_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || { echo "KUBEBRAIN_NAMESPACE is invalid" >&2; exit 2; }
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) ||
  { echo "EXPECTED_REPLICAS must be a canonical positive int32" >&2; exit 2; }
[[ "$REQUIRE_OLD_CA_REJECTION" == true || "$REQUIRE_OLD_CA_REJECTION" == false ]] ||
  { echo "REQUIRE_OLD_CA_REJECTION must be true or false" >&2; exit 2; }
if [[ "$INFO_ENDPOINT" != https://* || "$INFO_ENDPOINT" == *[[:cntrl:]]* ||
  "$INFO_ENDPOINT" == *\"* || "$INFO_ENDPOINT" == *\\* || "$INFO_ENDPOINT" == *@* ||
  "$INFO_ENDPOINT" == *\?* || "$INFO_ENDPOINT" == *\#* ]]; then
  echo "INFO_ENDPOINT must be an HTTPS authority without credentials, path, query, fragment, or unsafe characters" >&2
  exit 2
fi
info_address="${INFO_ENDPOINT#https://}"
[[ "$info_address" != */* ]] || { echo "INFO_ENDPOINT must not contain a path" >&2; exit 2; }
info_port=""
if [[ "$info_address" =~ ^\[[0-9A-Fa-f:]+\]:([1-9][0-9]{0,4})$ ]]; then
  info_port="${BASH_REMATCH[1]}"
elif [[ "$info_address" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?:([1-9][0-9]{0,4})$ ]]; then
  info_port="${BASH_REMATCH[2]}"
fi
[[ -n "$info_port" ]] && (( 10#$info_port <= 65535 )) ||
  { echo "INFO_ENDPOINT must include a valid DNS/IP authority and explicit port 1..65535" >&2; exit 2; }
[[ "$INFO_SERVER_NAME" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$ ]] ||
  { echo "INFO_SERVER_NAME must be a DNS name" >&2; exit 2; }
for variable in OLD_INFO_CACERT OLD_INFO_CERT NEW_INFO_CACERT NEW_INFO_CERT; do
  [[ -f "${!variable}" ]] || { echo "${variable} does not exist: ${!variable}" >&2; exit 2; }
  credential_size="$(stat -Lc '%s' -- "${!variable}")" || { echo "cannot stat ${variable}" >&2; exit 2; }
  [[ "$credential_size" =~ ^[0-9]+$ ]] && (( credential_size > 0 && credential_size <= MAX_CREDENTIAL_BYTES )) ||
    { echo "${variable} must contain 1..${MAX_CREDENTIAL_BYTES} bytes" >&2; exit 2; }
done

kubectl_args=()
[[ -z "$KUBE_CONTEXT" ]] || kubectl_args+=(--context "$KUBE_CONTEXT")
[[ -z "$KUBECONFIG_PATH" ]] || kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")

umask 077
mkdir -p "$STATE_DIR"
state_file="${STATE_DIR}/${ROTATION_ID}.info.state"
receipt_file="${RECEIPT_OUTPUT:-${STATE_DIR}/${ROTATION_ID}.info.receipt.json}"
if [[ "$ACTION" != begin ]]; then
  state_size="$(stat -Lc '%s' -- "$state_file")" || { echo "cannot stat info rotation state" >&2; exit 1; }
  [[ "$state_size" =~ ^[0-9]+$ ]] && (( state_size > 0 && state_size <= MAX_STATE_BYTES )) ||
    { echo "info rotation state must contain 1..${MAX_STATE_BYTES} bytes" >&2; exit 1; }
fi
if [[ "$ACTION" == verify ]]; then
  receipt_size="$(stat -Lc '%s' -- "$receipt_file")" || { echo "cannot stat info rotation receipt" >&2; exit 1; }
  [[ "$receipt_size" =~ ^[0-9]+$ ]] && (( receipt_size > 0 && receipt_size <= MAX_RECEIPT_BYTES )) ||
    { echo "info rotation receipt must contain 1..${MAX_RECEIPT_BYTES} bytes" >&2; exit 1; }
fi
credential_tmp_dir="$("$MKTEMP" -d)"
publication_tmp=""
cleanup() {
  rm -rf -- "$credential_tmp_dir"
  [[ -z "$publication_tmp" ]] || rm -f -- "$publication_tmp"
}
trap cleanup EXIT INT TERM

freeze_file() {
  local source="$1" name="$2" destination before after
  before="$(sha256sum "$source" | cut -d ' ' -f1)" || return 1
  destination="${credential_tmp_dir}/${name}"
  cp -- "$source" "$destination"
  chmod 600 "$destination"
  after="$(sha256sum "$source" | cut -d ' ' -f1)" || return 1
  [[ "$before" =~ ^[a-f0-9]{64}$ && "$after" == "$before" && "$(sha256sum "$destination" | cut -d ' ' -f1)" == "$before" ]] || return 1
  printf '%s\n' "$destination"
}
publish_no_replace() {
  local source="$1" destination="$2" label="$3"
  if ! "$SYNC" -f "$source"; then
    rm -f -- "$source"
    publication_tmp=""
    echo "cannot sync ${label} before publication" >&2
    return 1
  fi
  if ! "$LN" -- "$source" "$destination"; then
    rm -f -- "$source"
    publication_tmp=""
    echo "${label} already exists; refusing to overwrite" >&2
    return 1
  fi
  rm -f -- "$source"
  publication_tmp=""
  if ! "$SYNC" -f "$(dirname -- "$destination")"; then
    echo "cannot sync ${label} directory after publication" >&2
    return 1
  fi
}
new_publication_tmp() {
  local destination="$1" directory basename
  directory="$(dirname -- "$destination")"
  basename="$(basename -- "$destination")"
  publication_tmp="$("$MKTEMP" "${directory}/.${basename}.tmp.XXXXXX")" || return 1
  chmod 600 "$publication_tmp"
}

OLD_INFO_CACERT="$(freeze_file "$OLD_INFO_CACERT" old-ca)" || { echo "OLD_INFO_CACERT changed while being captured" >&2; exit 1; }
OLD_INFO_CERT="$(freeze_file "$OLD_INFO_CERT" old-cert)" || { echo "OLD_INFO_CERT changed while being captured" >&2; exit 1; }
NEW_INFO_CACERT="$(freeze_file "$NEW_INFO_CACERT" new-ca)" || { echo "NEW_INFO_CACERT changed while being captured" >&2; exit 1; }
NEW_INFO_CERT="$(freeze_file "$NEW_INFO_CERT" new-cert)" || { echo "NEW_INFO_CERT changed while being captured" >&2; exit 1; }
state_input="$state_file"
receipt_input="$receipt_file"
if [[ "$ACTION" != begin ]]; then
  state_input="$(freeze_file "$state_file" info-state)" || { echo "info rotation state changed while being captured" >&2; exit 1; }
fi
if [[ "$ACTION" == verify ]]; then
  receipt_input="$(freeze_file "$receipt_file" info-receipt)" || { echo "info rotation receipt changed while being captured" >&2; exit 1; }
fi

certificate_fingerprint() {
  local digest
  digest="$("$OPENSSL" x509 -in "$1" -outform DER | sha256sum | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

presented_fingerprint() {
  local cacert="$1" digest
  digest="$("$OPENSSL" s_client -connect "$info_address" -servername "$INFO_SERVER_NAME" \
    -CAfile "$cacert" -verify_return_error </dev/null 2>/dev/null |
    "$OPENSSL" x509 -outform DER | sha256sum | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

pod_snapshot() {
  "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get pods -l "$POD_SELECTOR" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.containerStatuses[?(@.name=="kubebrain")].restartCount}{"\t"}{.status.containerStatuses[?(@.name=="kubebrain")].ready}{"\n"}{end}' |
    LC_ALL=C sort
}

validate_snapshot() {
  local snapshot="$1" count
  count="$(sed '/^$/d' <<<"$snapshot" | wc -l | tr -d ' ')"
  [[ "$count" == "$EXPECTED_REPLICAS" ]] || { echo "expected ${EXPECTED_REPLICAS} KubeBrain Pods, got ${count}" >&2; exit 1; }
  awk -F '\t' 'NF != 4 || $1 == "" || $2 == "" || $3 !~ /^[0-9]+$/ || $4 != "true" { exit 1 }' <<<"$snapshot" ||
    { echo "KubeBrain Pod snapshot contains invalid identity, restart count, or readiness" >&2; exit 1; }
}

old_fingerprint="$(certificate_fingerprint "$OLD_INFO_CERT")" || { echo "failed to fingerprint OLD_INFO_CERT" >&2; exit 2; }
new_fingerprint="$(certificate_fingerprint "$NEW_INFO_CERT")" || { echo "failed to fingerprint NEW_INFO_CERT" >&2; exit 2; }
[[ "$old_fingerprint" != "$new_fingerprint" ]] || { echo "old and new info certificates must differ" >&2; exit 2; }

case "$ACTION" in
  begin)
    [[ ! -e "$state_file" ]] || { echo "info rotation state already exists" >&2; exit 1; }
    observed="$(presented_fingerprint "$OLD_INFO_CACERT")" || { echo "old info TLS handshake failed" >&2; exit 1; }
    [[ "$observed" == "$old_fingerprint" ]] || { echo "info endpoint does not present OLD_INFO_CERT" >&2; exit 1; }
    snapshot="$(pod_snapshot)"
    validate_snapshot "$snapshot"
    new_publication_tmp "$state_file" || { echo "cannot create private info rotation state" >&2; exit 1; }
    tmp="$publication_tmp"
    printf 'kubebrain.info-certificate-rotation.state.v1\t%s\t%s\t%s\t%s\t%s\n%s\n' \
      "$INSTANCE" "$ROTATION_ID" "$INFO_ENDPOINT" "$old_fingerprint" "$new_fingerprint" "$snapshot" >"$tmp"
    chmod 600 "$tmp"
    publish_no_replace "$tmp" "$state_file" "info rotation state" || exit 1
    echo "info certificate rotation begin gate passed: instance=${INSTANCE} rotation=${ROTATION_ID}"
    ;;
  complete|verify)
    [[ -f "$state_file" ]] || { echo "info rotation state is missing" >&2; exit 1; }
    if [[ "$ACTION" == complete ]]; then
      [[ ! -e "$receipt_file" ]] || { echo "info rotation receipt already exists" >&2; exit 1; }
    else
      [[ -f "$receipt_file" ]] || { echo "info rotation receipt is missing" >&2; exit 1; }
    fi
    IFS=$'\t' read -r version state_instance state_rotation state_endpoint state_old state_new <"$state_input"
    [[ "$version" == kubebrain.info-certificate-rotation.state.v1 && "$state_instance" == "$INSTANCE" &&
      "$state_rotation" == "$ROTATION_ID" && "$state_endpoint" == "$INFO_ENDPOINT" &&
      "$state_old" == "$old_fingerprint" && "$state_new" == "$new_fingerprint" ]] ||
      { echo "info rotation state does not match this operation" >&2; exit 1; }
    before_snapshot="$(tail -n +2 "$state_input")"
    validate_snapshot "$before_snapshot"
    observed="$(presented_fingerprint "$NEW_INFO_CACERT")" || { echo "new info TLS handshake failed" >&2; exit 1; }
    [[ "$observed" == "$new_fingerprint" ]] || { echo "info endpoint does not present NEW_INFO_CERT" >&2; exit 1; }
    old_ca_rejected=false
    if ! presented_fingerprint "$OLD_INFO_CACERT" >/dev/null; then
      old_ca_rejected=true
    fi
    [[ "$REQUIRE_OLD_CA_REJECTION" == false || "$old_ca_rejected" == true ]] ||
      { echo "old info CA still verifies the endpoint after cutover" >&2; exit 1; }
    after_snapshot="$(pod_snapshot)"
    validate_snapshot "$after_snapshot"
    [[ "$after_snapshot" == "$before_snapshot" ]] || { echo "KubeBrain Pods changed during info certificate rotation" >&2; exit 1; }
    if [[ "$ACTION" == complete ]]; then
      completed_at="$(date +%s)"
      operation_is_nonnegative_int64 "$completed_at" || { echo "invalid completion timestamp" >&2; exit 1; }
      new_publication_tmp "$receipt_file" || { echo "cannot create private info rotation receipt" >&2; exit 1; }
      tmp="$publication_tmp"
      "$JQ" -cnS --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --arg endpoint "$INFO_ENDPOINT" \
        --arg old "$old_fingerprint" --arg new "$new_fingerprint" --argjson replicas "$EXPECTED_REPLICAS" \
        --argjson completed "$completed_at" --argjson rejectionRequired "$REQUIRE_OLD_CA_REJECTION" \
        --argjson oldCARejected "$old_ca_rejected" \
        '{format:"kubebrain.info-certificate-rotation.receipt.v1",instance:$instance,rotation_id:$rotation,
          info_endpoint:$endpoint,replicas:$replicas,old_certificate_sha256:$old,new_certificate_sha256:$new,
          pods_unchanged:true,old_ca_rejection_required:$rejectionRequired,old_ca_rejected:$oldCARejected,
          completed_at_unix:$completed}' >"$tmp"
      chmod 600 "$tmp"
      publish_no_replace "$tmp" "$receipt_file" "info rotation receipt" || exit 1
      echo "info certificate rotation completion gate passed: instance=${INSTANCE} rotation=${ROTATION_ID} receipt=${receipt_file}"
    else
      "$JQ" -e --arg instance "$INSTANCE" --arg rotation "$ROTATION_ID" --arg endpoint "$INFO_ENDPOINT" \
        --arg old "$old_fingerprint" --arg new "$new_fingerprint" --argjson replicas "$EXPECTED_REPLICAS" \
        --argjson rejectionRequired "$REQUIRE_OLD_CA_REJECTION" '
        keys == ["completed_at_unix","format","info_endpoint","instance","new_certificate_sha256","old_ca_rejected","old_ca_rejection_required","old_certificate_sha256","pods_unchanged","replicas","rotation_id"] and
        .format == "kubebrain.info-certificate-rotation.receipt.v1" and .instance == $instance and
        .rotation_id == $rotation and .info_endpoint == $endpoint and .replicas == $replicas and
        .old_certificate_sha256 == $old and .new_certificate_sha256 == $new and .pods_unchanged == true and
        .old_ca_rejection_required == $rejectionRequired and (.old_ca_rejected | type == "boolean") and
        (($rejectionRequired | not) or .old_ca_rejected == true) and
        (.completed_at_unix | type == "number" and . > 0 and . == floor)' "$receipt_input" >/dev/null ||
        { echo "info rotation receipt does not match verified evidence" >&2; exit 1; }
      echo "info certificate rotation receipt verification passed: instance=${INSTANCE} rotation=${ROTATION_ID} receipt=${receipt_file}"
    fi
    ;;
esac
