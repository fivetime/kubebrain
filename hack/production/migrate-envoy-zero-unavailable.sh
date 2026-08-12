#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NAMESPACE="${NAMESPACE:-kubebrain-system}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain-envoy}"
PROFILE_DIR="${PROFILE_DIR:-$ROOT_DIR/deploy/production/envoy}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-240s}"
COHORT_TIMEOUT_SECONDS="${COHORT_TIMEOUT_SECONDS:-60}"

die() {
  echo "$*" >&2
  exit 1
}

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required; the current context is never accepted implicitly"
[[ "$COHORT_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ && "$COHORT_TIMEOUT_SECONDS" -le 600 ]] || \
  die "COHORT_TIMEOUT_SECONDS must be an integer in [1,600]"
command -v "$KUBECTL" >/dev/null 2>&1 || die "kubectl is required: $KUBECTL"
"$KUBECTL" config get-contexts "$KUBE_CONTEXT" --no-headers >/dev/null 2>&1 || \
  die "KUBE_CONTEXT does not exist: $KUBE_CONTEXT"
[[ -d "$PROFILE_DIR" ]] || die "Envoy profile directory does not exist: $PROFILE_DIR"

kctl=("$KUBECTL" --context "$KUBE_CONTEXT" --namespace "$NAMESPACE")

deployment_state() {
  "${kctl[@]}" get deployment "$DEPLOYMENT" -o \
    'jsonpath={.spec.replicas}{"\t"}{.status.readyReplicas}{"\t"}{.status.availableReplicas}{"\t"}{.spec.strategy.rollingUpdate.maxUnavailable}{"\t"}{.spec.strategy.rollingUpdate.maxSurge}{"\n"}'
}

required_hostnames() {
  "${kctl[@]}" get deployment "$DEPLOYMENT" -o \
    'jsonpath={range .spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[*]}{.topologyKey}{"\n"}{end}'
}

require_three_available() {
  local state replicas ready available max_unavailable max_surge
  state="$(deployment_state)" || die "failed to read Deployment state"
  IFS=$'\t' read -r replicas ready available max_unavailable max_surge <<<"$state"
  [[ "$replicas" == 3 && "$ready" == 3 && "$available" == 3 ]] || \
    die "migration requires replicas=ready=available=3; got: $state"
  [[ "$max_unavailable" == 0 || "$max_unavailable" == 1 ]] || \
    die "migration only accepts current maxUnavailable=0 or 1; got: $state"
  [[ "$max_surge" == 1 ]] || die "migration requires maxSurge=1; got: $state"
}

wait_for_new_pod_cohort() {
  local rows row_count ready_count node_count hash_count first_hash deadline
  deadline=$((SECONDS + COHORT_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    rows="$("${kctl[@]}" get pods \
      -l app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain \
      -o 'jsonpath={range .items[*]}{.metadata.uid}{"\t"}{.spec.nodeName}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.metadata.labels.pod-template-hash}{"\n"}{end}')" || \
      die "failed to read Envoy Pod cohort"
    row_count="$(awk 'NF { count++ } END { print count + 0 }' <<<"$rows")"
    ready_count="$(awk -F $'\t' '$3 == "True" { count++ } END { print count + 0 }' <<<"$rows")"
    node_count="$(awk -F $'\t' 'NF { seen[$2]=1 } END { for (value in seen) count++; print count + 0 }' <<<"$rows")"
    hash_count="$(awk -F $'\t' 'NF { seen[$4]=1 } END { for (value in seen) count++; print count + 0 }' <<<"$rows")"
    first_hash="$(awk -F $'\t' 'NF { print $4; exit }' <<<"$rows")"
    if [[ "$row_count" -eq 3 && "$ready_count" -eq 3 && "$node_count" -eq 3 && \
      "$hash_count" -eq 1 && -n "$first_hash" ]]; then
      return
    fi
    sleep 1
  done
  die "timed out waiting for exactly three Ready same-revision Envoy Pods on distinct nodes; got: $rows"
}

require_three_available
canonical_manifest="$("$KUBECTL" kustomize "$PROFILE_DIR")" || die "failed to render canonical Envoy profile"
manifest_namespaces="$(sed -n 's/^  namespace: //p' <<<"$canonical_manifest" | sort -u)"
[[ "$manifest_namespaces" == "$NAMESPACE" ]] || \
  die "canonical Envoy profile namespace does not match target $NAMESPACE: $manifest_namespaces"
[[ "$(grep -c '^      maxUnavailable: 0$' <<<"$canonical_manifest")" -eq 1 ]] || \
  die "canonical Envoy profile must contain exactly one maxUnavailable: 0"
for marker in 'minDomains: 3' 'nodeTaintsPolicy: Honor' 'pod-template-hash'; do
  grep -q "$marker" <<<"$canonical_manifest" || die "canonical Envoy profile is missing: $marker"
done

legacy_required="$(required_hostnames)" || die "failed to inspect legacy pod anti-affinity"
if [[ -n "$legacy_required" ]]; then
  [[ "$legacy_required" == "kubernetes.io/hostname" ]] || \
    die "refusing to migrate unexpected required pod anti-affinity: $legacy_required"
  migration_manifest="$(sed 's/^      maxUnavailable: 0$/      maxUnavailable: 1/' <<<"$canonical_manifest")"
  [[ "$(grep -c '^      maxUnavailable: 1$' <<<"$migration_manifest")" -eq 1 ]] || \
    die "failed to render the maxUnavailable=1 migration phase"
  printf '%s\n' "$migration_manifest" | "${kctl[@]}" apply -f - >/dev/null
  "${kctl[@]}" rollout status "deployment/$DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT"
  require_three_available
  [[ -z "$(required_hostnames)" ]] || die "required pod anti-affinity remains after migration rollout"
  wait_for_new_pod_cohort
fi

printf '%s\n' "$canonical_manifest" | "${kctl[@]}" apply -f - >/dev/null
"${kctl[@]}" rollout status "deployment/$DEPLOYMENT" --timeout="$ROLLOUT_TIMEOUT"
require_three_available
final_state="$(deployment_state)"
IFS=$'\t' read -r _ _ _ final_max_unavailable final_max_surge <<<"$final_state"
[[ "$final_max_unavailable" == 0 && "$final_max_surge" == 1 ]] || \
  die "canonical strategy was not established; got: $final_state"
[[ -z "$(required_hostnames)" ]] || die "canonical Deployment still has required pod anti-affinity"
wait_for_new_pod_cohort

echo "Envoy Deployment migrated to maxUnavailable=0/maxSurge=1 with a three-node topology cohort"
