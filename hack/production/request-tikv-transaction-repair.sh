#!/usr/bin/env bash
set -euo pipefail

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

die() { echo "$*" >&2; exit 1; }
[[ -f "$ALERT_INPUT" ]] || die "ALERT_INPUT is required and must exist"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required (use in-cluster for service-account credentials)"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$MIN_FIRING_SECONDS" =~ ^[1-9][0-9]*$ && "$NOW_UNIX" =~ ^[1-9][0-9]*$ ]] || die "firing duration and current time must be positive integers"
for value in "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$OPERATION_NAMESPACE"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "resource identity must be a DNS label"
done
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
[[ -x "$OPERATIONCTL" ]] || die "OPERATIONCTL must be executable"

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
  select(.startsAt | type == "string" and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T"))' "$ALERT_INPUT")" ||
  die "payload must contain exactly one matching firing transaction-path alert"
fingerprint="$($JQ -r '.fingerprint' <<<"$alert")"
starts_at="$($JQ -r '.startsAt' <<<"$alert")"
started_unix="$($DATE -u -d "$starts_at" +%s 2>/dev/null)" || die "alert startsAt is invalid"
(( started_unix <= NOW_UNIX )) || die "alert startsAt is in the future"
(( NOW_UNIX - started_unix >= MIN_FIRING_SECONDS )) || die "alert has not fired for the required duration"

kb_uid="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}')"
cluster_identity="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o 'jsonpath={.metadata.uid}{"\t"}{.status.clusterID}')"
tidb_uid="${cluster_identity%%$'\t'*}"
cluster_id="${cluster_identity#*$'\t'}"
[[ -n "$kb_uid" && -n "$tidb_uid" && "$cluster_id" =~ ^[1-9][0-9]*$ ]] || die "live instance identity is incomplete"

occurrence_id="$(printf '%s\n%s\n' "$fingerprint" "$starts_at" | sha256sum | cut -c1-20)"
operation_name="tikv-repair-${occurrence_id}"
secret_name="${operation_name}-parameters"
temp_dir="$(mktemp -d)"
trap 'rm -rf -- "$temp_dir"' EXIT
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
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"

if existing_data="$(kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' 2>/dev/null)"; then
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
  --parameters-key parameters.json --max-attempts 1 >/dev/null
echo "created or verified unapproved Pending TiKVTransactionRepair ${OPERATION_NAMESPACE}/${operation_name}"
