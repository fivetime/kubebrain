#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NAMESPACE="${NAMESPACE:-kubebrain-system}"
TEST_TIMEOUT="${TEST_TIMEOUT:-8m}"
ROLLOUT_CYCLES="${ROLLOUT_CYCLES:-3}"
NODE_HOST="${NODE_HOST:-}"
NODE_PORT="${NODE_PORT:-}"
NODE_ENDPOINT_PORT="${NODE_ENDPOINT_PORT:-}"
TLS_CA_FILE="${TLS_CA_FILE:-}"
TLS_CERT_FILE="${TLS_CERT_FILE:-}"
TLS_KEY_FILE="${TLS_KEY_FILE:-}"
TLS_SERVER_NAME="${TLS_SERVER_NAME:-}"
TLS_ROTATION_COMMAND="${TLS_ROTATION_COMMAND:-}"
TLS_ROTATED_CA_FILE="${TLS_ROTATED_CA_FILE:-}"
TLS_ROTATED_CERT_FILE="${TLS_ROTATED_CERT_FILE:-}"
TLS_ROTATED_KEY_FILE="${TLS_ROTATED_KEY_FILE:-}"
TLS_ROTATED_SERVER_NAME="${TLS_ROTATED_SERVER_NAME:-}"
TLS_RETIRED_CA_FILE="${TLS_RETIRED_CA_FILE:-}"
SERVICE_NAME=kubebrain-envoy-rollout-gate
ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT="${ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT:-false}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"

if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBE_CONTEXT explicitly; the rollout gate will not use an implicit current context" >&2
  exit 2
fi
tls_values=("$TLS_CA_FILE" "$TLS_CERT_FILE" "$TLS_KEY_FILE" "$TLS_SERVER_NAME")
tls_nonempty=0
for value in "${tls_values[@]}"; do
  [[ -n "$value" ]] && ((tls_nonempty += 1))
done
if (( tls_nonempty != 0 && tls_nonempty != 4 )); then
  echo "TLS_CA_FILE, TLS_CERT_FILE, TLS_KEY_FILE, and TLS_SERVER_NAME must all be set for TLS passthrough" >&2
  exit 2
fi
if (( tls_nonempty == 4 )); then
  for tls_file in "$TLS_CA_FILE" "$TLS_CERT_FILE" "$TLS_KEY_FILE"; do
    if [[ ! -r "$tls_file" ]]; then
      echo "TLS_CA_FILE, TLS_CERT_FILE, and TLS_KEY_FILE must name readable files" >&2
      exit 2
    fi
  done
fi
if [[ -n "$TLS_ROTATION_COMMAND" ]]; then
  if (( tls_nonempty != 4 )); then
    echo "TLS_ROTATION_COMMAND requires all TLS passthrough inputs" >&2
    exit 2
  fi
  if [[ ! -f "$TLS_ROTATION_COMMAND" || ! -x "$TLS_ROTATION_COMMAND" ]]; then
    echo "TLS_ROTATION_COMMAND must name an executable regular file" >&2
    exit 2
  fi
  TLS_ROTATION_COMMAND="$(cd "$(dirname "$TLS_ROTATION_COMMAND")" && pwd -P)/$(basename "$TLS_ROTATION_COMMAND")"
fi
rotated_tls_values=("$TLS_ROTATED_CA_FILE" "$TLS_ROTATED_CERT_FILE" "$TLS_ROTATED_KEY_FILE" "$TLS_ROTATED_SERVER_NAME" "$TLS_RETIRED_CA_FILE")
rotated_tls_nonempty=0
for value in "${rotated_tls_values[@]}"; do
  [[ -n "$value" ]] && ((rotated_tls_nonempty += 1))
done
if (( rotated_tls_nonempty != 0 && rotated_tls_nonempty != 5 )); then
  echo "TLS_ROTATED_CA_FILE, TLS_ROTATED_CERT_FILE, TLS_ROTATED_KEY_FILE, TLS_ROTATED_SERVER_NAME, and TLS_RETIRED_CA_FILE must all be set" >&2
  exit 2
fi
if (( rotated_tls_nonempty == 5 )); then
  [[ -n "$TLS_ROTATION_COMMAND" ]] || { echo "rotated TLS inputs require TLS_ROTATION_COMMAND" >&2; exit 2; }
  for tls_file in "$TLS_ROTATED_CA_FILE" "$TLS_ROTATED_CERT_FILE" "$TLS_ROTATED_KEY_FILE" "$TLS_RETIRED_CA_FILE"; do
    [[ -r "$tls_file" ]] || { echo "rotated TLS files must be readable" >&2; exit 2; }
  done
fi
if [[ ! "$ROLLOUT_CYCLES" =~ ^[0-9]+$ ]] || (( ROLLOUT_CYCLES < 1 || ROLLOUT_CYCLES > 10 )); then
  echo "ROLLOUT_CYCLES must be an integer in [1,10]" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT" != true && "$ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT must be true or false, got $ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT" != true ]]; then
  echo "refusing destructive Envoy Kubernetes rollout without explicit approval" >&2
  echo "set ALLOW_DESTRUCTIVE_ENVOY_ROLLOUT=true only for a controlled three-node rollout target" >&2
  exit 1
fi
for command in kubectl go jq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
REFERENCE_ETCD_BIN="$ETCDCTL_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"
if ! kubectl config get-contexts "$KUBE_CONTEXT" --no-headers >/dev/null 2>&1; then
  echo "KUBE_CONTEXT does not exist: $KUBE_CONTEXT" >&2
  exit 2
fi

pod_rows="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get pods \
  -l app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain \
  -o jsonpath='{range .items[*]}{.metadata.uid}{"\t"}{.spec.nodeName}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}')"
if [[ "$(wc -l <<<"$pod_rows")" -ne 3 ]] || grep -qv $'\tTrue$' <<<"$pod_rows"; then
  echo "rollout gate requires exactly three Ready Envoy Pods" >&2
  echo "$pod_rows" >&2
  exit 1
fi
if [[ "$(cut -f2 <<<"$pod_rows" | sort -u | wc -l)" -ne 3 ]]; then
  echo "rollout gate requires the three Envoy Pods on three distinct Kubernetes nodes" >&2
  echo "$pod_rows" >&2
  exit 1
fi

endpoint_rows="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get endpointslice \
  -l kubernetes.io/service-name=kubebrain-envoy \
  -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.uid}{"\t"}{.conditions.ready}{"\n"}{end}')"
if [[ "$(grep -c $'\ttrue$' <<<"$endpoint_rows")" -ne 3 ]]; then
  echo "kubebrain-envoy Service must expose exactly three ready EndpointSlice targets" >&2
  echo "$endpoint_rows" >&2
  exit 1
fi

service_created=false
cleanup() {
  if [[ "$service_created" == true ]]; then
    kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" delete service "$SERVICE_NAME" \
      --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
if kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get service "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "rollout gate Service already exists; refusing to delete it: $NAMESPACE/$SERVICE_NAME" >&2
  exit 1
fi

service_port=2379
if (( tls_nonempty == 4 )); then
  service_port=2380
fi
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" create service nodeport "$SERVICE_NAME" \
  --tcp="${service_port}:${service_port}" --dry-run=client -o yaml | \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" apply -f - >/dev/null
service_created=true
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" patch service "$SERVICE_NAME" --type=json \
  -p '[{"op":"replace","path":"/spec/selector","value":{"app.kubernetes.io/name":"kubebrain-envoy","app.kubernetes.io/instance":"kubebrain"}}]' \
  >/dev/null

if [[ -n "$NODE_PORT" ]]; then
  if [[ ! "$NODE_PORT" =~ ^3[0-9]{4}$ ]]; then
    echo "NODE_PORT must be empty or a Kubernetes NodePort in [30000,39999]" >&2
    exit 2
  fi
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" patch service "$SERVICE_NAME" --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/ports/0/nodePort\",\"value\":${NODE_PORT}}]" >/dev/null
fi

node_port="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get service "$SERVICE_NAME" \
  -o jsonpath='{.spec.ports[0].nodePort}')"
node_ip="$NODE_HOST"
if [[ -z "$node_ip" ]]; then
  node_ip="$(kubectl --context "$KUBE_CONTEXT" get nodes \
    -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
fi
if [[ ! "$node_port" =~ ^[0-9]+$ || -z "$node_ip" ]]; then
  echo "failed to resolve rollout gate NodePort endpoint" >&2
  exit 1
fi
endpoint_port="$node_port"
if [[ -n "$NODE_ENDPOINT_PORT" ]]; then
  if [[ ! "$NODE_ENDPOINT_PORT" =~ ^[0-9]+$ ]] || (( NODE_ENDPOINT_PORT < 1 || NODE_ENDPOINT_PORT > 65535 )); then
    echo "NODE_ENDPOINT_PORT must be empty or a TCP port in [1,65535]" >&2
    exit 2
  fi
  endpoint_port="$NODE_ENDPOINT_PORT"
fi

declare -a etcdctl_tls_args=()
if (( tls_nonempty == 4 )); then
  etcdctl_tls_args=(--cacert="$TLS_CA_FILE" --cert="$TLS_CERT_FILE" --key="$TLS_KEY_FILE")
fi
rollout_endpoint="${node_ip}:${endpoint_port}"
for _ in $(seq 1 300); do
  if "$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" \
    --command-timeout=2s endpoint health >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
if ! "$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" \
  --command-timeout=2s endpoint health >/dev/null; then
  echo "Envoy rollout endpoint did not become healthy: $rollout_endpoint" >&2
  exit 1
fi

capture_target_state() {
  local phase="$1" auth_json range_json users_json roles_json leases_json alarms_json
  auth_json="$("$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" auth status -w json)"
  if ! jq -e '(.enabled // false) == false' >/dev/null <<<"$auth_json"; then
    echo "Envoy rollout target authentication must be disabled during ${phase}" >&2
    exit 1
  fi
  range_json="$("$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" get '' --from-key -w json)"
  users_json="$("$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" user list -w json)"
  roles_json="$("$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" role list -w json)"
  leases_json="$("$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" lease list -w json)"
  alarms_json="$("$ETCDCTL_BIN" "${etcdctl_tls_args[@]}" --endpoints="$rollout_endpoint" alarm list -w json)"
  jq -cn \
    --argjson range "$range_json" --argjson users "$users_json" --argjson roles "$roles_json" \
    --argjson leases "$leases_json" --argjson alarms "$alarms_json" \
    '{
      kvs: (($range.kvs // []) | map({key, value, create_revision, mod_revision, version, lease}) | sort_by(.key)),
      users: (($users.users // []) | sort), roles: (($roles.roles // []) | sort),
      leases: (($leases.leases // []) | map(.ID // .id) | sort),
      alarms: (($alarms.alarms // []) | map([(.memberID // .member_id // 0), (.alarm // 0)]) | sort)
    }'
}
baseline_target_state="$(capture_target_state preflight)"

test_status=0
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  tls_enabled=false
  if (( tls_nonempty == 4 )); then
    tls_enabled=true
  fi
  KUBEBRAIN_ENVOY_ROLLOUT_ENDPOINT="$rollout_endpoint" \
    KUBEBRAIN_ENVOY_ROLLOUT_CONTEXT="$KUBE_CONTEXT" \
	  KUBEBRAIN_ENVOY_ROLLOUT_NAMESPACE="$NAMESPACE" \
	  KUBEBRAIN_ENVOY_ROLLOUT_CYCLES="$ROLLOUT_CYCLES" \
	  KUBEBRAIN_ENVOY_ROLLOUT_TLS="$tls_enabled" \
	  KUBEBRAIN_ENVOY_ROLLOUT_TLS_ROTATION_COMMAND="$TLS_ROTATION_COMMAND" \
	  KUBERNETES_ENVOY_ROLLOUT_TLS_CA_FILE="$TLS_CA_FILE" \
	  KUBERNETES_ENVOY_ROLLOUT_TLS_CERT_FILE="$TLS_CERT_FILE" \
	  KUBERNETES_ENVOY_ROLLOUT_TLS_KEY_FILE="$TLS_KEY_FILE" \
	  KUBERNETES_ENVOY_ROLLOUT_TLS_SERVER_NAME="$TLS_SERVER_NAME" \
	  KUBERNETES_ENVOY_ROTATED_TLS_CA_FILE="$TLS_ROTATED_CA_FILE" \
	  KUBERNETES_ENVOY_ROTATED_TLS_CERT_FILE="$TLS_ROTATED_CERT_FILE" \
	  KUBERNETES_ENVOY_ROTATED_TLS_KEY_FILE="$TLS_ROTATED_KEY_FILE" \
	  KUBERNETES_ENVOY_ROTATED_TLS_SERVER_NAME="$TLS_ROTATED_SERVER_NAME" \
	  KUBERNETES_ENVOY_RETIRED_TLS_CA_FILE="$TLS_RETIRED_CA_FILE" \
	  go test . -run '^TestEnvoyKubernetesRollout$' -count=1 -timeout="$TEST_TIMEOUT" -v
) || test_status=$?
if (( rotated_tls_nonempty == 5 )); then
  etcdctl_tls_args=(
    --cacert="$TLS_ROTATED_CA_FILE"
    --cert="$TLS_ROTATED_CERT_FILE"
    --key="$TLS_ROTATED_KEY_FILE"
  )
fi
final_target_state="$(capture_target_state postflight)"
if [[ "$final_target_state" != "$baseline_target_state" ]]; then
  echo "Envoy Kubernetes rollout suite changed visible target state" >&2
  echo "before: $baseline_target_state" >&2
  echo "after:  $final_target_state" >&2
  exit 1
fi
if [[ "$test_status" -ne 0 ]]; then
  echo "Envoy Kubernetes rollout test package failed with status $test_status" >&2
  exit "$test_status"
fi
