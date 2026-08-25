#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="${ROOT_DIR}/deploy/production/kubebrain-operation-parameter-broker.yaml"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
CURL="${CURL:-curl}"
SHA256SUM="${SHA256SUM:-sha256sum}"
BROKER_CA_FILE="${BROKER_CA_FILE:-}"
BROKER_TOKEN_FILE="${BROKER_TOKEN_FILE:-}"
NAMESPACE=kubebrain-operations
REPAIR_NAMESPACE=kubebrain-repair-operations
IDENTITY=system:serviceaccount:kubebrain-operations:kubebrain-operation-parameter-broker
BROKER_HOST=kubebrain-operation-parameter-broker.kubebrain-operations.svc

requesters=(backup backup-deletion certificate-rotation cold-physical-restore cold-physical-snapshot destroy info-certificate-rotation jwt-key-rotation legacy-snapshot-remediation native-pitr-full-backup native-pitr-full-restore native-pitr-target-provisioning native-pitr-target-retirement post-restore-audit restore-cutover tikv-quiesced-repair tikv-transaction-recovery)

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
requester_namespace() { case "$1" in tikv-*) printf '%s' "$REPAIR_NAMESPACE" ;; *) printf '%s' "$NAMESPACE" ;; esac; }
executor_name() { case "$1" in tikv-quiesced-repair) printf '%s' tikv-transaction-repair ;; *) printf '%s' "$1" ;; esac; }

verify_inventory() {
  [[ -f "$MANIFEST" && ! -L "$MANIFEST" ]] || die "parameter broker manifest is missing or a symlink"
  grep -Fq 'replicas: 0' "$MANIFEST" || die "parameter broker must remain disabled until credentials are provisioned"
  grep -Fq -- '--token-audience=kubebrain-operation-parameters' "$MANIFEST" || die "parameter broker token audience drifted"
  grep -Fq -- '--additional-readiness-namespace=kubebrain-repair-operations' "$MANIFEST" || die "parameter broker repair queue readiness drifted"
  grep -Fq 'secretName: kubebrain-operation-parameter-broker-tls' "$MANIFEST" || die "parameter broker TLS Secret drifted"
  echo "verified fail-closed Operation parameter broker inventory"
}

check_can_i() {
  local expected="$1" identity="$2" namespace="$3" verb="$4" resource="$5" answer rc args
  args=(auth can-i "$verb" "$resource" --as="$identity")
  [[ -z "$namespace" ]] || args+=( -n "$namespace" )
  set +e
  answer="$("$KUBECTL" "${context[@]}" "${args[@]}" 2>&1)"; rc=$?
  set -e
  if [[ "$expected" == yes ]]; then
    [[ "$rc" == 0 && "$answer" == yes ]] || die "parameter broker authorization must allow ${verb} ${resource}: ${identity}"
  else
    [[ "$rc" == 1 && "$answer" == no ]] || die "parameter broker authorization was not a definitive denial for ${verb} ${resource}: ${identity}"
  fi
}

check_rbac() {
  local verb resource requester namespace executor
  check_can_i yes "$IDENTITY" "" create tokenreviews.authentication.k8s.io
  for verb in get list watch update patch delete deletecollection; do check_can_i no "$IDENTITY" "" "$verb" tokenreviews.authentication.k8s.io; done
  for namespace in "$NAMESPACE" "$REPAIR_NAMESPACE"; do
    for resource in kubebrainoperations.dbaas.kubebrain.io secrets; do
      check_can_i yes "$IDENTITY" "$namespace" get "$resource"
      for verb in create list watch update patch delete deletecollection; do check_can_i no "$IDENTITY" "$namespace" "$verb" "$resource"; done
    done
  done
  for requester in "${requesters[@]}"; do
    namespace="$(requester_namespace "$requester")"
    executor="system:serviceaccount:kubebrain-operations:kubebrain-$(executor_name "$requester")-executor"
    check_can_i no "$executor" "$namespace" get secrets
    check_can_i no "$executor" "$namespace" list secrets
  done
}

check_deployment() {
  local expected="$1" deployment_json
  deployment_json="$("$KUBECTL" "${context[@]}" get deployment kubebrain-operation-parameter-broker -n "$NAMESPACE" -o json)" || die "parameter broker Deployment is missing"
  if [[ "$expected" == 0 ]]; then
    "$JQ" -e '.spec.replicas == 0' <<<"$deployment_json" >/dev/null || die "parameter broker is enabled before credential and rollout checks"
  else
    "$JQ" -e '.spec.replicas == 2 and .status.observedGeneration >= .metadata.generation and .status.updatedReplicas == 2 and .status.readyReplicas == 2 and .status.availableReplicas == 2 and ((.status.unavailableReplicas // 0) == 0)' <<<"$deployment_json" >/dev/null || die "parameter broker did not converge to two ready replicas"
  fi
}

runtime_smoke() (
  local should_scale="$1" tls_json ca_json pods_json pod_json token token_bytes ca_bytes ca_mode tmp ca_from_config curl_config port_log port_forward_pid port attempt line ready_code auth_code pod uid_listed uid_before uid_after
  [[ -f "$BROKER_CA_FILE" && ! -L "$BROKER_CA_FILE" ]] || die "BROKER_CA_FILE must be a regular non-symlink file"
  ca_bytes="$(stat -Lc '%s' "$BROKER_CA_FILE")"; ca_mode="$(stat -Lc '%a' "$BROKER_CA_FILE")"
  [[ "$ca_bytes" =~ ^[0-9]+$ && "$ca_bytes" -ge 1 && "$ca_bytes" -le 1048576 ]] || die "broker CA bundle must be 1..1048576 bytes"
  (( (8#$ca_mode & 8#022) == 0 )) || die "broker CA bundle must not be group/world writable"
  [[ -f "$BROKER_TOKEN_FILE" && ! -L "$BROKER_TOKEN_FILE" ]] || die "BROKER_TOKEN_FILE must be a regular non-symlink file"
  [[ "$(stat -Lc '%a' "$BROKER_TOKEN_FILE")" == 600 && "$(stat -Lc '%u' "$BROKER_TOKEN_FILE")" == "$(id -u)" ]] || die "broker token file must be current-user-owned mode 0600"
  token_bytes="$(stat -Lc '%s' "$BROKER_TOKEN_FILE")"
  [[ "$token_bytes" =~ ^[0-9]+$ && "$token_bytes" -ge 1 && "$token_bytes" -le 16384 ]] || die "broker token must be 1..16384 bytes"
  ! LC_ALL=C grep -q '[[:space:]]' "$BROKER_TOKEN_FILE" || die "broker token must not contain whitespace"
  token="$(<"$BROKER_TOKEN_FILE")"; [[ "$token" =~ ^[A-Za-z0-9._~+/=-]+$ ]] || die "broker token contains characters unsafe for a Bearer header"
  tls_json="$("$KUBECTL" "${context[@]}" get secret kubebrain-operation-parameter-broker-tls -n "$NAMESPACE" -o json)" || die "parameter broker TLS Secret is missing"
  "$JQ" -e '.type == "kubernetes.io/tls" and (.data|keys|sort) == ["tls.crt","tls.key"] and ((.data["tls.crt"]|@base64d)|length > 0) and ((.data["tls.key"]|@base64d)|length > 0)' <<<"$tls_json" >/dev/null || die "parameter broker TLS Secret must contain only non-empty tls.crt and tls.key"
  ca_json="$("$KUBECTL" "${context[@]}" get configmap kubebrain-operation-parameter-broker-ca -n "$NAMESPACE" -o json)" || die "parameter broker CA ConfigMap is missing"
  "$JQ" -e '(.data|keys) == ["ca.crt"] and (.data["ca.crt"]|length > 0)' <<<"$ca_json" >/dev/null || die "parameter broker CA ConfigMap must contain only non-empty ca.crt"
  umask 077; tmp="$(mktemp -d)"; port_forward_pid=""; did_scale=false
  cleanup() {
    if [[ -n "$port_forward_pid" ]] && kill -0 "$port_forward_pid" 2>/dev/null; then kill "$port_forward_pid" 2>/dev/null || true; wait "$port_forward_pid" 2>/dev/null || true; fi
    rm -rf -- "$tmp"
    if [[ "$did_scale" == true && "${smoke_succeeded:-false}" != true ]]; then
      "$KUBECTL" "${context[@]}" scale deployment/kubebrain-operation-parameter-broker --replicas=0 -n "$NAMESPACE" >/dev/null 2>&1 || true
      "$KUBECTL" "${context[@]}" rollout status deployment/kubebrain-operation-parameter-broker --timeout=2m -n "$NAMESPACE" >/dev/null 2>&1 || true
      echo "parameter broker enable failed; requested rollback to zero replicas" >&2
    fi
  }
  trap cleanup EXIT
  ca_from_config="${tmp}/ca.crt"; "$JQ" -j '.data["ca.crt"]' <<<"$ca_json" >"$ca_from_config"
  [[ "$("$SHA256SUM" "$ca_from_config" | awk '{print $1}')" == "$("$SHA256SUM" "$BROKER_CA_FILE" | awk '{print $1}')" ]] || die "local broker CA does not match the executor CA ConfigMap"
  curl_config="${tmp}/token.curl"; printf 'header = "Authorization: Bearer %s"\n' "$token" >"$curl_config"; chmod 600 "$curl_config"
  if [[ "$should_scale" == true ]]; then
    "$KUBECTL" "${context[@]}" scale deployment/kubebrain-operation-parameter-broker --replicas=2 -n "$NAMESPACE"
    did_scale=true
    "$KUBECTL" "${context[@]}" rollout status deployment/kubebrain-operation-parameter-broker --timeout=5m -n "$NAMESPACE"
  fi
  check_deployment 2
  port_log="${tmp}/port-forward.log"
  "$KUBECTL" "${context[@]}" port-forward --address 127.0.0.1 -n "$NAMESPACE" service/kubebrain-operation-parameter-broker :443 >"$port_log" 2>&1 & port_forward_pid=$!
  port=""
  for ((attempt = 1; attempt <= 100; attempt++)); do
    kill -0 "$port_forward_pid" 2>/dev/null || die "broker port-forward exited before becoming ready"
    [[ "$(stat -Lc '%s' "$port_log")" -le 65536 ]] || die "broker port-forward log exceeded 64 KiB"
    line="$(grep -m1 -E '^Forwarding from 127\.0\.0\.1:[0-9]+ -> 443$' "$port_log" || true)"
    if [[ -n "$line" ]]; then port="${line#*:}"; port="${port%% *}"; break; fi
    sleep 0.1
  done
  [[ "$port" =~ ^[1-9][0-9]{0,4}$ && "$port" -le 65535 ]] || die "broker port-forward did not report a valid local IPv4 port"
  ready_code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --noproxy '*' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$BROKER_CA_FILE" --resolve "${BROKER_HOST}:${port}:127.0.0.1" "https://${BROKER_HOST}:${port}/readyz")" || die "broker HTTPS readiness request failed"
  [[ "$ready_code" == 204 ]] || die "broker HTTPS readiness did not return 204"
  auth_code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --noproxy '*' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$BROKER_CA_FILE" --resolve "${BROKER_HOST}:${port}:127.0.0.1" --config "$curl_config" "https://${BROKER_HOST}:${port}/v1/parameters?namespace=kubebrain-operations&name=broker-smoke-missing&owner=worker-a&attempt=1")" || die "broker authenticated parameter request failed"
  [[ "$auth_code" == 403 ]] || die "broker authenticated missing-operation request did not return 403"
  kill "$port_forward_pid" 2>/dev/null || true; wait "$port_forward_pid" 2>/dev/null || true; port_forward_pid=""

  pods_json="$("$KUBECTL" "${context[@]}" get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kubebrain-operation-parameter-broker -o json)" || die "cannot list parameter broker Pods"
  "$JQ" -e '(.items|length) == 2 and all(.items[]; (.metadata.name|type == "string" and length > 0) and (.metadata.uid|type == "string" and length > 0) and ((.metadata.deletionTimestamp // "") == "") and any(.status.conditions[]?; .type == "Ready" and .status == "True")) and ([.items[].metadata.uid]|unique|length) == 2' <<<"$pods_json" >/dev/null || die "parameter broker must have exactly two distinct non-terminating Ready Pods"
  mapfile -t pod_names < <("$JQ" -er '.items|sort_by(.metadata.name)|.[].metadata.name' <<<"$pods_json")
  for pod in "${pod_names[@]}"; do
    uid_listed="$("$JQ" -er --arg pod "$pod" '.items[] | select(.metadata.name == $pod) | .metadata.uid' <<<"$pods_json")"
    pod_json="$("$KUBECTL" "${context[@]}" get pod "$pod" -n "$NAMESPACE" -o json)" || die "cannot read parameter broker Pod identity: ${pod}"
    uid_before="$("$JQ" -er '.metadata.uid | select(type == "string" and length > 0)' <<<"$pod_json")" || die "parameter broker Pod UID is missing: ${pod}"
    [[ "$uid_before" == "$uid_listed" ]] || die "parameter broker Pod changed before direct smoke: ${pod}"
    port_log="${tmp}/${pod}.port-forward.log"
    "$KUBECTL" "${context[@]}" port-forward --address 127.0.0.1 -n "$NAMESPACE" "pod/${pod}" :8443 >"$port_log" 2>&1 & port_forward_pid=$!
    port=""
    for ((attempt = 1; attempt <= 100; attempt++)); do
      kill -0 "$port_forward_pid" 2>/dev/null || die "broker Pod port-forward exited before becoming ready: ${pod}"
      [[ "$(stat -Lc '%s' "$port_log")" -le 65536 ]] || die "broker Pod port-forward log exceeded 64 KiB: ${pod}"
      line="$(grep -m1 -E '^Forwarding from 127\.0\.0\.1:[0-9]+ -> 8443$' "$port_log" || true)"
      if [[ -n "$line" ]]; then port="${line#*:}"; port="${port%% *}"; break; fi
      sleep 0.1
    done
    [[ "$port" =~ ^[1-9][0-9]{0,4}$ && "$port" -le 65535 ]] || die "broker Pod port-forward did not report a valid local IPv4 port: ${pod}"
    ready_code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --noproxy '*' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$BROKER_CA_FILE" --resolve "${BROKER_HOST}:${port}:127.0.0.1" "https://${BROKER_HOST}:${port}/readyz")" || die "broker Pod HTTPS readiness request failed: ${pod}"
    [[ "$ready_code" == 204 ]] || die "broker Pod HTTPS readiness did not return 204: ${pod}"
    auth_code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --noproxy '*' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$BROKER_CA_FILE" --resolve "${BROKER_HOST}:${port}:127.0.0.1" --config "$curl_config" "https://${BROKER_HOST}:${port}/v1/parameters?namespace=kubebrain-operations&name=broker-smoke-missing&owner=worker-a&attempt=1")" || die "broker Pod authenticated parameter request failed: ${pod}"
    [[ "$auth_code" == 403 ]] || die "broker Pod authenticated missing-operation request did not return 403: ${pod}"
    pod_json="$("$KUBECTL" "${context[@]}" get pod "$pod" -n "$NAMESPACE" -o json)" || die "cannot reread parameter broker Pod identity: ${pod}"
    uid_after="$("$JQ" -er '.metadata.uid' <<<"$pod_json")"
    [[ "$uid_after" == "$uid_before" ]] || die "parameter broker Pod changed during direct smoke: ${pod}"
    kill "$port_forward_pid" 2>/dev/null || true; wait "$port_forward_pid" 2>/dev/null || true; port_forward_pid=""
  done
  smoke_succeeded=true
  if [[ "$should_scale" == true ]]; then echo "enabled two ready parameter broker replicas and verified HTTPS/TokenReview smoke"; else echo "checked two ready parameter broker replicas and HTTPS/TokenReview smoke"; fi
)

usage() { echo "Usage: $0 --verify | --check | --enable | --check-enabled" >&2; exit 2; }
[[ "$#" == 1 ]] || usage
case "$1" in --verify) verify_inventory; exit 0 ;; --check|--enable|--check-enabled) mode="$1" ;; *) usage ;; esac
verify_inventory >/dev/null
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for production ${mode#--}"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"
context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
check_rbac
if [[ "$mode" == --check ]]; then check_deployment 0; echo "checked disabled parameter broker RBAC foundation"; exit 0; fi
CURL="$(resolve "$CURL")" || die "CURL must be executable"; SHA256SUM="$(resolve "$SHA256SUM")" || die "SHA256SUM must be executable"
if [[ "$mode" == --enable ]]; then check_deployment 0; runtime_smoke true; else check_deployment 2; runtime_smoke false; fi
