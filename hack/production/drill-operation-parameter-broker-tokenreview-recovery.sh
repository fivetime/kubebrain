#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$BASH_SOURCE")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
CURL="${CURL:-curl}"
BROKER_CHECKER="${BROKER_CHECKER:-$ROOT_DIR/hack/production/apply-operation-parameter-broker.sh}"
BROKER_CA_FILE="${BROKER_CA_FILE:-}"
BROKER_TOKEN_FILE="${BROKER_TOKEN_FILE:-}"
CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL="${CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL:-}"
DRILL_TIMEOUT_SECONDS="${DRILL_TIMEOUT_SECONDS:-180}"
NAMESPACE=kubebrain-operations
BINDING=kubebrain-operation-parameter-broker-token-review
IDENTITY=system:serviceaccount:kubebrain-operations:kubebrain-operation-parameter-broker
BROKER_HOST=kubebrain-operation-parameter-broker.kubebrain-operations.svc

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
kc() { if [[ "$KUBE_CONTEXT" == in-cluster ]]; then "$KUBECTL" "$@"; else "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; fi; }

[[ "$#" == 0 ]] || { echo "Usage: $0" >&2; exit 2; }
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for the parameter broker TokenReview recovery drill"
[[ "$CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL" == yes ]] || die "set CONFIRM_PARAMETER_BROKER_TOKENREVIEW_DRILL=yes to authorize temporary TokenReview revocation"
[[ "$DRILL_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,3}$ && "$DRILL_TIMEOUT_SECONDS" -le 3600 ]] || die "DRILL_TIMEOUT_SECONDS must be 1..3600"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
CURL="$(resolve "$CURL")" || die "CURL must be executable"
BROKER_CHECKER="$(resolve "$BROKER_CHECKER")" || die "broker checker must be executable"

KUBE_CONTEXT="$KUBE_CONTEXT" KUBECTL="$KUBECTL" JQ="$JQ" CURL="$CURL" BROKER_CA_FILE="$BROKER_CA_FILE" BROKER_TOKEN_FILE="$BROKER_TOKEN_FILE" "$BROKER_CHECKER" --check-enabled

binding_json="$(kc get clusterrolebinding "$BINDING" -o json)" || die "cannot read broker TokenReview binding"
"$JQ" -e '.metadata.uid | type == "string" and length > 0' <<<"$binding_json" >/dev/null || die "broker TokenReview binding UID is missing"
"$JQ" -e '.roleRef == {apiGroup:"rbac.authorization.k8s.io",kind:"ClusterRole",name:"kubebrain-operation-parameter-broker-token-review"} and .subjects == [{kind:"ServiceAccount",name:"kubebrain-operation-parameter-broker",namespace:"kubebrain-operations"}]' <<<"$binding_json" >/dev/null || die "broker TokenReview binding shape drifted"
binding_uid="$("$JQ" -r '.metadata.uid' <<<"$binding_json")"
binding_rv="$("$JQ" -r '.metadata.resourceVersion' <<<"$binding_json")"
[[ -n "$binding_rv" && "$binding_rv" != null ]] || die "broker TokenReview binding resourceVersion is missing"
original_subjects="$("$JQ" -c '.subjects' <<<"$binding_json")"

pods_json="$(kc get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kubebrain-operation-parameter-broker -o json)" || die "cannot list broker Pods"
"$JQ" -e '(.items | length) == 2 and all(.items[]; (.metadata.deletionTimestamp // "") == "" and any(.status.conditions[]?; .type == "Ready" and .status == "True")) and ([.items[].metadata.uid] | unique | length) == 2' <<<"$pods_json" >/dev/null || die "broker must start with exactly two distinct Ready Pods"
baseline_pods="$("$JQ" -c '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)' <<<"$pods_json")"

revoked=false
restore_binding() {
  local current current_rv patch
  [[ "$revoked" == true ]] || return 0
  current="$(kc get clusterrolebinding "$BINDING" -o json)" || return 1
  "$JQ" -e --arg uid "$binding_uid" '.metadata.uid == $uid and (.subjects // []) == [] and .roleRef == {apiGroup:"rbac.authorization.k8s.io",kind:"ClusterRole",name:"kubebrain-operation-parameter-broker-token-review"}' <<<"$current" >/dev/null || return 1
  current_rv="$("$JQ" -r '.metadata.resourceVersion' <<<"$current")"
  patch="$("$JQ" -cn --arg rv "$current_rv" --argjson subjects "$original_subjects" '[{op:"test",path:"/metadata/resourceVersion",value:$rv},{op:"replace",path:"/subjects",value:$subjects}]')"
  kc patch clusterrolebinding "$BINDING" --type=json -p "$patch" >/dev/null || return 1
  revoked=false
}
cleanup() {
  if ! restore_binding; then echo "CRITICAL: automatic restoration of broker TokenReview binding failed; restore $BINDING immediately" >&2; fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

revoke_patch="$("$JQ" -cn --arg rv "$binding_rv" '[{op:"test",path:"/metadata/resourceVersion",value:$rv},{op:"replace",path:"/subjects",value:[]}]')"
kc patch clusterrolebinding "$BINDING" --type=json -p "$revoke_patch" >/dev/null || die "failed to revoke broker TokenReview binding"
revoked=true

deadline=$((SECONDS + DRILL_TIMEOUT_SECONDS))
while :; do
  set +e
  answer="$(kc auth can-i create tokenreviews.authentication.k8s.io --as="$IDENTITY")"; rc=$?
  set -e
  [[ "$rc" == 1 && "$answer" == no ]] && break
  (( SECONDS < deadline )) || die "broker TokenReview revocation did not become effective"
  sleep 1
done

check_pod_identities() {
  local current
  current="$(kc get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kubebrain-operation-parameter-broker -o json)" || return 1
  [[ "$("$JQ" -c '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)' <<<"$current")" == "$baseline_pods" ]] || return 1
  printf '%s' "$current"
}

pod_readiness_code() (
  local pod="$1" expected_uid="$2" phase="$3" tmp log pid port line attempt code
  tmp="$(mktemp -d)"; log="$tmp/port-forward.log"; pid=""
  cleanup_tunnel() {
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
    rm -rf -- "$tmp"
  }
  trap cleanup_tunnel EXIT
  kc port-forward --address 127.0.0.1 -n "$NAMESPACE" "pod/$pod" :8443 >"$log" 2>&1 & pid=$!
  port=""
  for ((attempt = 1; attempt <= 100; attempt++)); do
    kill -0 "$pid" 2>/dev/null || die "broker Pod port-forward exited during TokenReview $phase: $pod"
    [[ "$(stat -Lc '%s' "$log")" -le 65536 ]] || die "broker Pod port-forward log exceeded 64 KiB: $pod"
    line="$(grep -m1 -E '^Forwarding from 127\.0\.0\.1:[0-9]+ -> 8443$' "$log" || true)"
    if [[ -n "$line" ]]; then port="${line#*:}"; port="${port%% *}"; break; fi
    sleep 0.1
  done
  [[ "$port" =~ ^[1-9][0-9]{0,4}$ && "$port" -le 65535 ]] || die "broker Pod port-forward did not become ready: $pod"
  code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --noproxy '*' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$BROKER_CA_FILE" --resolve "$BROKER_HOST:$port:127.0.0.1" "https://$BROKER_HOST:$port/readyz")" || die "broker Pod readiness request failed during TokenReview $phase: $pod"
  [[ "$(kc get pod "$pod" -n "$NAMESPACE" -o json | "$JQ" -r '.metadata.uid')" == "$expected_uid" ]] || die "broker Pod changed during TokenReview $phase: $pod"
  printf '%s' "$code"
)

wait_pod_state() {
  local expected_condition="$1" expected_code="$2" phase="$3" current pod uid code
  while :; do
    current="$(check_pod_identities)" || die "broker Pod identities changed during TokenReview $phase"
    if "$JQ" -e --arg state "$expected_condition" 'all(.items[]; any(.status.conditions[]?; .type == "Ready" and .status == $state))' <<<"$current" >/dev/null; then break; fi
    (( SECONDS < deadline )) || die "broker Pods did not reach Ready=$expected_condition after TokenReview $phase"
    sleep 1
  done
  while IFS=$'\t' read -r pod uid; do
    code="$(pod_readiness_code "$pod" "$uid" "$phase")"
    [[ "$code" == "$expected_code" ]] || die "broker Pod readiness returned $code, want $expected_code, during TokenReview $phase: $pod"
  done < <("$JQ" -r '.[]|[.name,.uid]|@tsv' <<<"$baseline_pods")
}

wait_pod_state False 503 revocation
restore_binding || die "failed to restore broker TokenReview binding"

deadline=$((SECONDS + DRILL_TIMEOUT_SECONDS))
while :; do
  answer="$(kc auth can-i create tokenreviews.authentication.k8s.io --as="$IDENTITY" || true)"
  [[ "$answer" == yes ]] && break
  (( SECONDS < deadline )) || die "broker TokenReview authorization did not recover"
  sleep 1
done
wait_pod_state True 204 recovery
trap - EXIT INT TERM
echo "verified parameter broker TokenReview 204 -> 503/NotReady -> 204/Ready recovery with unchanged Pod UIDs"
