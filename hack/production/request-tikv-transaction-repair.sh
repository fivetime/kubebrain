#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

ALERT_INPUT="${ALERT_INPUT:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
ENDPOINT="${ENDPOINT:-}"
MIN_FIRING_SECONDS="${MIN_FIRING_SECONDS:-120}"
NOW_UNIX="${NOW_UNIX:-$(date +%s)}"
KUBECTL="${KUBECTL:-kubectl}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
JQ="${JQ:-jq}"
DATE="${DATE:-date}"
MAX_ALERT_INPUT_BYTES=1048576
MAX_IDENTITY_RESPONSE_BYTES=4096
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
MAX_UINT64=18446744073709551615

die() { echo "$*" >&2; exit 1; }
is_positive_uint64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,19}$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$MAX_UINT64" ]]; then
    return 1
  fi
}
[[ -f "$ALERT_INPUT" ]] || die "ALERT_INPUT is required and must exist"
command -v stat >/dev/null || die "stat is required"
alert_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ ]] && ((size <= MAX_ALERT_INPUT_BYTES)); }
identity_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ ]] && ((size <= MAX_IDENTITY_RESPONSE_BYTES)); }
alert_size_is_valid "$ALERT_INPUT" || die "alert payload exceeds ${MAX_ALERT_INPUT_BYTES} bytes"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required (use in-cluster for service-account credentials)"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
operation_is_positive_int64 "$MIN_FIRING_SECONDS" || die "MIN_FIRING_SECONDS must be a positive int64"
operation_is_positive_int64 "$NOW_UNIX" || die "NOW_UNIX must be a positive int64 Unix timestamp"
for value in "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$OPERATION_NAMESPACE"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "resource identity must be a DNS label"
done
command -v "$JQ" >/dev/null || die "jq is required"
[[ "$("$JQ" -jn --arg value "$MAX_UINT64" '$value | tonumber | tostring' 2>/dev/null)" == "$MAX_UINT64" ]] ||
  die "jq must preserve unsigned 64-bit decimal identities"
command -v sha256sum >/dev/null || die "sha256sum is required"
[[ -x "$OPERATIONCTL" ]] || die "OPERATIONCTL must be executable"
temp_dir="$(mktemp -d)"
trap 'rm -rf -- "$temp_dir"' EXIT
frozen_alert="$temp_dir/alert.json"
cp -- "$ALERT_INPUT" "$frozen_alert"; chmod 600 "$frozen_alert"
alert_size_is_valid "$frozen_alert" && alert_size_is_valid "$ALERT_INPUT" || die "alert payload exceeds ${MAX_ALERT_INPUT_BYTES} bytes"

context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }

alert="$($JQ -cer --arg ns "$KUBEBRAIN_NAMESPACE" --arg sts "$KUBEBRAIN_STATEFULSET" '
  [.alerts[] | select(
    .status == "firing" and
    .labels.alertname == "KubeBrainTransactionPathUnavailableWithHealthyTiKVControlPlane" and
    .labels.namespace == $ns and .labels.statefulset == $sts
  )] | select(length == 1) | .[0] |
  select(.fingerprint | type == "string" and test("^[a-f0-9]{16,64}$")) |
  select(.startsAt | type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T"))' "$frozen_alert")" ||
  die "payload must contain exactly one matching firing transaction-path alert"
fingerprint="$($JQ -r '.fingerprint' <<<"$alert")"
starts_at="$($JQ -r '.startsAt' <<<"$alert")"
started_unix="$($DATE -u -d "$starts_at" +%s 2>/dev/null)" || die "alert startsAt is invalid"
operation_is_positive_int64 "$started_unix" || die "alert startsAt must resolve to a positive int64 Unix timestamp"
(( started_unix <= NOW_UNIX )) || die "alert startsAt is in the future"
(( NOW_UNIX - started_unix >= MIN_FIRING_SECONDS )) || die "alert has not fired for the required duration"

kb_uid_file="$temp_dir/kubebrain-statefulset.uid"
kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}' >"$kb_uid_file" || die "cannot read KubeBrain StatefulSet identity"
chmod 600 "$kb_uid_file"; identity_size_is_valid "$kb_uid_file" || die "identity response exceeds ${MAX_IDENTITY_RESPONSE_BYTES} bytes"
kb_uid="$(<"$kb_uid_file")"
cluster_identity_file="$temp_dir/tidbcluster.identity"
kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o 'jsonpath={.metadata.uid}{"\t"}{.status.clusterID}' >"$cluster_identity_file" || die "cannot read TidbCluster identity"
chmod 600 "$cluster_identity_file"; identity_size_is_valid "$cluster_identity_file" || die "identity response exceeds ${MAX_IDENTITY_RESPONSE_BYTES} bytes"
cluster_identity="$(<"$cluster_identity_file")"
tidb_uid="${cluster_identity%%$'\t'*}"
cluster_id="${cluster_identity#*$'\t'}"
[[ -n "$kb_uid" && -n "$tidb_uid" ]] && is_positive_uint64 "$cluster_id" ||
  die "live instance identity must contain a positive uint64 cluster ID"

occurrence_id="$(printf '%s\n%s\n' "$fingerprint" "$starts_at" | sha256sum | cut -c1-20)"
operation_name="tikv-repair-${occurrence_id}"
secret_name="${operation_name}-parameters"
parameters_file="$temp_dir/parameters.json"
$JQ -cnS \
  --arg endpoint "$ENDPOINT" --arg kbns "$KUBEBRAIN_NAMESPACE" --arg kbsts "$KUBEBRAIN_STATEFULSET" \
  --arg tidbns "$TIDB_NAMESPACE" --arg tidb "$TIDB_CLUSTER" --arg kbuid "$kb_uid" \
  --arg tidbuid "$tidb_uid" --argjson cluster "$cluster_id" --arg fingerprint "$fingerprint" \
  --arg starts_at "$starts_at" --arg occurrence "$occurrence_id" '
  {alert_fingerprint:$fingerprint,alert_occurrence_id:$occurrence,alert_starts_at:$starts_at,
   endpoint:$endpoint,kubebrain_namespace:$kbns,kubebrain_statefulset:$kbsts,
   tidb_namespace:$tidbns,tidb_cluster:$tidb,
   expected_kubebrain_statefulset_uid:$kbuid,expected_tidb_cluster_uid:$tidbuid,
   expected_cluster_id:$cluster,required_failed_probes:3,probe_interval_seconds:5,
   probe_timeout_seconds:10,pod_ready_timeout_seconds:300,repair_cooldown_seconds:3600}' >"$parameters_file"
[[ "$(wc -c <"$parameters_file")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"

existing_secret_file="$temp_dir/existing-secret.response"
if kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_secret_file" 2>/dev/null; then
  chmod 600 "$existing_secret_file"
  identity_response_size="$(stat -Lc '%s' -- "$existing_secret_file")" || die "cannot inspect existing Secret response"
  [[ "$identity_response_size" =~ ^[0-9]+$ && "$identity_response_size" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing_data="$(<"$existing_secret_file")"
  immutable="${existing_data%%$'\t'*}"
  encoded="${existing_data#*$'\t'}"
  [[ "$immutable" == "true" ]] || die "existing parameter Secret is not immutable"
  existing_sha="$(printf '%s' "$encoded" | base64 -d | sha256sum | cut -d ' ' -f1)"
  [[ "$existing_sha" == "$parameters_sha" ]] || die "existing parameter Secret identity drifted"
else
  kctl -n "$OPERATION_NAMESPACE" create secret generic "$secret_name" \
    --from-file="parameters.json=$parameters_file" --dry-run=client -o json |
    "$JQ" '.immutable=true' |
    kctl -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi

operationctl_args=(--namespace "$OPERATION_NAMESPACE")
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || operationctl_args+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${operationctl_args[@]}" --action submit --name "$operation_name" \
  --operation-id "$operation_name" --requested-by alertmanager:transaction-path-policy \
  --instance "$KUBEBRAIN_STATEFULSET" --type TiKVTransactionRepair \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret_name" \
  --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified unapproved Pending TiKVTransactionRepair ${OPERATION_NAMESPACE}/${operation_name}"
