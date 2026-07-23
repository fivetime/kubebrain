#!/usr/bin/env bash
set -euo pipefail

ACTION="${ACTION:-}"
ROTATION_ID="${ROTATION_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
POD_SELECTOR="${POD_SELECTOR:-app.kubernetes.io/name=kubebrain}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
ENDPOINT="${ENDPOINT:-}"
KUBECTL="${KUBECTL:-kubectl}"
ETCDCTL="${ETCDCTL:-etcdctl}"
OPENSSL="${OPENSSL:-openssl}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"

usage() {
  cat >&2 <<'EOF'
Usage:
  ACTION=begin|overlap|complete ROTATION_ID=<id> INSTANCE=<instance> \
  STATE_DIR=<durable-dir> ENDPOINT=https://host:2379 \
  OLD_CACERT=<file> OLD_CERT=<file> OLD_KEY=<file> \
  NEW_CACERT=<file> NEW_CERT=<file> NEW_KEY=<file> \
  [OVERLAP_CACERT=<file>] [RECEIPT_OUTPUT=<file>] \
    hack/production/validate-certificate-rotation.sh

begin records the exact Pod identities and proves the old credentials work.
overlap proves old and new credentials both work through the overlap CA bundle.
complete proves the new credentials work, the old client certificate is rejected,
and no KubeBrain Pod was replaced or restarted. It then publishes a receipt.
EOF
  exit 2
}

for variable in ACTION ROTATION_ID INSTANCE STATE_DIR ENDPOINT OLD_CACERT OLD_CERT OLD_KEY NEW_CACERT NEW_CERT NEW_KEY; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    usage
  fi
done
if [[ ! "$ACTION" =~ ^(begin|overlap|complete)$ ]]; then
  echo "ACTION must be begin, overlap, or complete" >&2
  exit 2
fi
if [[ ! "$ROTATION_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "ROTATION_ID must contain only letters, digits, dot, underscore, and hyphen" >&2
  exit 2
fi
if [[ ! "$INSTANCE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "INSTANCE must contain only letters, digits, dot, underscore, and hyphen" >&2
  exit 2
fi
if [[ "$ENDPOINT" == *[$'\t\r\n\"\\']* ]]; then
  echo "ENDPOINT must not contain control characters, quotes, or backslashes" >&2
  exit 2
fi
if ! [[ "$EXPECTED_REPLICAS" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_REPLICAS must be a positive integer" >&2
  exit 2
fi
for variable in OLD_CACERT OLD_CERT OLD_KEY NEW_CACERT NEW_CERT NEW_KEY; do
  if [[ ! -f "${!variable}" ]]; then
    echo "${variable} does not exist: ${!variable}" >&2
    exit 2
  fi
done
if [[ -n "${OVERLAP_CACERT:-}" && ! -f "$OVERLAP_CACERT" ]]; then
  echo "OVERLAP_CACERT does not exist: ${OVERLAP_CACERT}" >&2
  exit 2
fi

umask 077
mkdir -p "$STATE_DIR"
state_file="${STATE_DIR}/${ROTATION_ID}.state"
overlap_file="${STATE_DIR}/${ROTATION_ID}.overlap"
receipt_file="${RECEIPT_OUTPUT:-${STATE_DIR}/${ROTATION_ID}.receipt.json}"

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi
if [[ -n "$KUBECONFIG_PATH" ]]; then
  kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")
fi

credential_tmp_dir=""
cleanup_rotation_credentials() {
  [[ -z "$credential_tmp_dir" ]] || rm -rf "$credential_tmp_dir"
}
trap cleanup_rotation_credentials EXIT INT TERM

credential_file_digest() {
  local path="$1" digest
  digest="$(sha256sum "$path" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

freeze_credential() {
  local source="$1" name="$2" destination source_digest captured_digest current_digest
  source_digest="$(credential_file_digest "$source")" || return 1
  destination="${credential_tmp_dir}/${name}"
  cp "$source" "$destination"
  chmod 600 "$destination"
  captured_digest="$(credential_file_digest "$destination")" || return 1
  current_digest="$(credential_file_digest "$source")" || return 1
  [[ "$captured_digest" == "$source_digest" && "$current_digest" == "$source_digest" ]] || return 1
  printf '%s\n' "$destination"
}

credential_tmp_dir="$(mktemp -d)"
OLD_CACERT="$(freeze_credential "$OLD_CACERT" old-ca)" ||
  { echo "OLD_CACERT changed while being captured" >&2; exit 1; }
OLD_CERT="$(freeze_credential "$OLD_CERT" old-cert)" ||
  { echo "OLD_CERT changed while being captured" >&2; exit 1; }
OLD_KEY="$(freeze_credential "$OLD_KEY" old-key)" ||
  { echo "OLD_KEY changed while being captured" >&2; exit 1; }
NEW_CACERT="$(freeze_credential "$NEW_CACERT" new-ca)" ||
  { echo "NEW_CACERT changed while being captured" >&2; exit 1; }
NEW_CERT="$(freeze_credential "$NEW_CERT" new-cert)" ||
  { echo "NEW_CERT changed while being captured" >&2; exit 1; }
NEW_KEY="$(freeze_credential "$NEW_KEY" new-key)" ||
  { echo "NEW_KEY changed while being captured" >&2; exit 1; }
if [[ -n "${OVERLAP_CACERT:-}" ]]; then
  OVERLAP_CACERT="$(freeze_credential "$OVERLAP_CACERT" overlap-ca)" ||
    { echo "OVERLAP_CACERT changed while being captured" >&2; exit 1; }
fi

fingerprint() {
  local value
  value="$("$OPENSSL" x509 -in "$1" -noout -fingerprint -sha256 |
    sed 's/^sha256 Fingerprint=//I; s/://g' |
    tr '[:upper:]' '[:lower:]')" ||
    { echo "failed to read certificate SHA-256 fingerprint: ${1}" >&2; exit 2; }
  [[ "$value" =~ ^[a-f0-9]{64}$ ]] ||
    { echo "certificate SHA-256 fingerprint is invalid: ${1}" >&2; exit 2; }
  printf '%s\n' "$value"
}

health() {
  local cacert="$1" cert="$2" key="$3"
  ETCDCTL_API=3 "$ETCDCTL" \
    --endpoints="$ENDPOINT" \
    --cacert="$cacert" \
    --cert="$cert" \
    --key="$key" \
    endpoint health >/dev/null
}

pod_snapshot() {
  "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get pods \
    -l "$POD_SELECTOR" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.containerStatuses[?(@.name=="kubebrain")].restartCount}{"\t"}{.status.containerStatuses[?(@.name=="kubebrain")].ready}{"\n"}{end}' |
    LC_ALL=C sort
}

validate_snapshot() {
  local snapshot="$1" count
  count="$(sed '/^$/d' <<<"$snapshot" | wc -l | tr -d ' ')"
  if [[ "$count" != "$EXPECTED_REPLICAS" ]]; then
    echo "expected ${EXPECTED_REPLICAS} KubeBrain Pods, got ${count}" >&2
    exit 1
  fi
  if awk -F '\t' 'NF != 4 || $2 == "" || $3 !~ /^[0-9]+$/ || $4 != "true" { exit 1 }' <<<"$snapshot"; then
    return
  fi
  echo "KubeBrain Pod snapshot contains missing identity, restart count, or readiness" >&2
  exit 1
}

validate_state_schema() {
  awk -F '\t' -v expected="$EXPECTED_REPLICAS" '
    NR == 1 {
      if (NF != 6 || $1 != "kubebrain.certificate-rotation.state.v1") {
        bad = "HEADER row must have 6 fields"
        exit 1
      }
      header++
      next
    }
    $0 == "" {
      bad = "empty Pod row"
      exit 1
    }
    {
      if (NF != 4 || $1 == "" || $2 == "" || $3 !~ /^[0-9]+$/ || $4 != "true") {
        bad = "Pod row must have name, uid, restart count, and ready=true"
        exit 1
      }
      pods++
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (header != 1 || pods != expected) {
        printf("schema counts mismatch: header=%d pods=%d expected=%d\n",
          header, pods, expected) > "/dev/stderr"
        exit 1
      }
    }
  ' "$state_file" || { echo "rotation state has invalid schema" >&2; exit 1; }
}

validate_overlap_marker() {
  awk -F '\t' -v instance="$INSTANCE" -v rotation="$ROTATION_ID" '
    NR == 1 {
      if (NF != 3 ||
        $1 != "kubebrain.certificate-rotation.overlap.v1" ||
        $2 != instance || $3 != rotation) {
        bad = "overlap marker row does not match the operation"
        exit 1
      }
      rows++
      next
    }
    {
      bad = "unexpected extra overlap marker row"
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (rows != 1) {
        print "overlap marker must contain exactly one row" > "/dev/stderr"
        exit 1
      }
    }
  ' "$overlap_file" || { echo "rotation overlap marker has invalid schema" >&2; exit 1; }
}

read_state_header() {
  local expected_old_fingerprint expected_new_fingerprint
  expected_old_fingerprint="$(fingerprint "$OLD_CERT")"
  expected_new_fingerprint="$(fingerprint "$NEW_CERT")"
  validate_state_schema
  IFS=$'\t' read -r state_version state_instance state_rotation state_endpoint \
    state_old_fingerprint state_new_fingerprint <"$state_file"
  if [[ "$state_version" != "kubebrain.certificate-rotation.state.v1" ||
    "$state_instance" != "$INSTANCE" ||
    "$state_rotation" != "$ROTATION_ID" ||
    "$state_endpoint" != "$ENDPOINT" ||
    "$state_old_fingerprint" != "$expected_old_fingerprint" ||
    "$state_new_fingerprint" != "$expected_new_fingerprint" ]]; then
    echo "rotation state does not match the requested operation" >&2
    exit 1
  fi
}

assert_pods_unchanged() {
  local before current
  before="$(tail -n +2 "$state_file")"
  current="$(pod_snapshot)"
  validate_snapshot "$current"
  if [[ "$current" != "$before" ]]; then
    echo "KubeBrain Pods were replaced, restarted, or changed readiness during certificate rotation" >&2
    diff -u <(printf '%s\n' "$before") <(printf '%s\n' "$current") >&2 || true
    exit 1
  fi
}

atomic_publish() {
  local temporary="$1" destination="$2"
  sync -f "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    if cmp -s "$temporary" "$destination"; then
      rm -f "$temporary"
      return
    fi
    if [[ "$destination" == "$receipt_file" && -n "${old_fingerprint:-}" && -n "${new_fingerprint:-}" ]] &&
      validate_existing_receipt "$old_fingerprint" "$new_fingerprint"; then
      rm -f "$temporary"
      return
    fi
    echo "refusing to overwrite existing evidence: ${destination}" >&2
    rm -f "$temporary"
    exit 1
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$destination")"
}

require_jq() {
  command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }
}

validate_existing_receipt() {
  local old_fingerprint="$1" new_fingerprint="$2"
  "$JQ" -e \
    --arg instance "$INSTANCE" \
    --arg rotation "$ROTATION_ID" \
    --arg endpoint "$ENDPOINT" \
    --arg old "$old_fingerprint" \
    --arg new "$new_fingerprint" \
    --argjson replicas "$EXPECTED_REPLICAS" \
    'keys == ["completed_at_unix","endpoint","format","instance","new_certificate_sha256","old_certificate_rejected","old_certificate_sha256","pods_unchanged","replicas","rotation_id"] and
     .format == "kubebrain.certificate-rotation.receipt.v1" and
     .instance == $instance and .rotation_id == $rotation and
     .endpoint == $endpoint and .replicas == $replicas and
     (.old_certificate_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
     (.new_certificate_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
     .old_certificate_sha256 == $old and .new_certificate_sha256 == $new and
     .pods_unchanged == true and .old_certificate_rejected == true and
     (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
    "$receipt_file" >/dev/null
}

case "$ACTION" in
  begin)
    snapshot="$(pod_snapshot)"
    validate_snapshot "$snapshot"
    health "$OLD_CACERT" "$OLD_CERT" "$OLD_KEY"
    old_fingerprint="$(fingerprint "$OLD_CERT")"
    new_fingerprint="$(fingerprint "$NEW_CERT")"
    temporary="$(mktemp "${STATE_DIR}/.${ROTATION_ID}.state.XXXXXX")"
    printf 'kubebrain.certificate-rotation.state.v1\t%s\t%s\t%s\t%s\t%s\n%s\n' \
      "$INSTANCE" "$ROTATION_ID" "$ENDPOINT" "$old_fingerprint" "$new_fingerprint" \
      "$snapshot" >"$temporary"
    atomic_publish "$temporary" "$state_file"
    echo "certificate rotation begin gate passed: instance=${INSTANCE} rotation=${ROTATION_ID}"
    ;;
  overlap)
    [[ -f "$state_file" ]] || { echo "begin evidence is missing" >&2; exit 1; }
    [[ -n "${OVERLAP_CACERT:-}" && -f "$OVERLAP_CACERT" ]] ||
      { echo "OVERLAP_CACERT is required for overlap" >&2; exit 2; }
    read_state_header
    assert_pods_unchanged
    health "$OVERLAP_CACERT" "$OLD_CERT" "$OLD_KEY"
    health "$OVERLAP_CACERT" "$NEW_CERT" "$NEW_KEY"
    temporary="$(mktemp "${STATE_DIR}/.${ROTATION_ID}.overlap.XXXXXX")"
    printf 'kubebrain.certificate-rotation.overlap.v1\t%s\t%s\n' \
      "$INSTANCE" "$ROTATION_ID" >"$temporary"
    atomic_publish "$temporary" "$overlap_file"
    echo "certificate rotation overlap gate passed: instance=${INSTANCE} rotation=${ROTATION_ID}"
    ;;
  complete)
    [[ -f "$state_file" ]] || { echo "begin evidence is missing" >&2; exit 1; }
    [[ -f "$overlap_file" ]] || { echo "overlap evidence is missing" >&2; exit 1; }
    read_state_header
    validate_overlap_marker
    assert_pods_unchanged
    health "$NEW_CACERT" "$NEW_CERT" "$NEW_KEY"
    require_jq
    if health "$NEW_CACERT" "$OLD_CERT" "$OLD_KEY"; then
      echo "old client certificate is still accepted after CA cutover" >&2
      exit 1
    fi
    if ! health "$NEW_CACERT" "$NEW_CERT" "$NEW_KEY"; then
      echo "new credentials failed after the old-credential rejection check; refusing to treat an endpoint outage as certificate rejection" >&2
      exit 1
    fi
    old_fingerprint="$(fingerprint "$OLD_CERT")"
    new_fingerprint="$(fingerprint "$NEW_CERT")"
    if [[ -e "$receipt_file" ]]; then
      if ! validate_existing_receipt "$old_fingerprint" "$new_fingerprint"; then
        echo "existing receipt does not match the completed rotation" >&2
        exit 1
      fi
      echo "certificate rotation completion gate passed: instance=${INSTANCE} rotation=${ROTATION_ID} receipt=${receipt_file}"
      exit 0
    fi
    completed_at="$(date +%s)"
    temporary="$(mktemp "${STATE_DIR}/.${ROTATION_ID}.receipt.XXXXXX")"
    "$JQ" -cnS \
      --arg instance "$INSTANCE" \
      --arg rotation "$ROTATION_ID" \
      --arg endpoint "$ENDPOINT" \
      --arg old "$old_fingerprint" \
      --arg new "$new_fingerprint" \
      --argjson replicas "$EXPECTED_REPLICAS" \
      --argjson completed_at "$completed_at" \
      '{format:"kubebrain.certificate-rotation.receipt.v1",instance:$instance,
        rotation_id:$rotation,endpoint:$endpoint,replicas:$replicas,
        old_certificate_sha256:$old,new_certificate_sha256:$new,
        pods_unchanged:true,old_certificate_rejected:true,
        completed_at_unix:$completed_at}' >"$temporary"
    atomic_publish "$temporary" "$receipt_file"
    echo "certificate rotation completion gate passed: instance=${INSTANCE} rotation=${ROTATION_ID} receipt=${receipt_file}"
    ;;
esac
