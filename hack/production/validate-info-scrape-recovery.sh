#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

TLS_RECEIPT_INPUT="${TLS_RECEIPT_INPUT:-}"
ACTION="${ACTION:-complete}"
SCRAPE_RECEIPT_OUTPUT="${SCRAPE_RECEIPT_OUTPUT:-}"
PROMETHEUS_URL="${PROMETHEUS_URL:-}"
PROMETHEUS_CA_FILE="${PROMETHEUS_CA_FILE:-}"
PROMETHEUS_BEARER_TOKEN_FILE="${PROMETHEUS_BEARER_TOKEN_FILE:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_SERVICE="${KUBEBRAIN_SERVICE:-kubebrain-peer}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
RECOVERY_TIMEOUT_SECONDS="${RECOVERY_TIMEOUT_SECONDS:-120}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
QUERY_TIMEOUT_SECONDS="${QUERY_TIMEOUT_SECONDS:-10}"
MAX_CLOCK_SKEW_SECONDS="${MAX_CLOCK_SKEW_SECONDS:-5}"
MAX_SAMPLE_AGE_SECONDS="${MAX_SAMPLE_AGE_SECONDS:-60}"
MAX_RESPONSE_BYTES=1048576
MAX_TLS_RECEIPT_BYTES=1048576
MAX_SCRAPE_RECEIPT_BYTES=2097152
MAX_PROMETHEUS_CA_BYTES=1048576
MAX_PROMETHEUS_TOKEN_BYTES=16384
CURL="${CURL:-curl}"
JQ="${JQ:-jq}"
LN="${LN:-ln}"

die() { echo "$*" >&2; exit 1; }
[[ -f "$TLS_RECEIPT_INPUT" && -r "$TLS_RECEIPT_INPUT" ]] || die "TLS_RECEIPT_INPUT must be a readable regular file"
[[ "$ACTION" == complete || "$ACTION" == verify ]] || die "ACTION must be complete or verify"
[[ -n "$SCRAPE_RECEIPT_OUTPUT" ]] || die "SCRAPE_RECEIPT_OUTPUT is required"
if [[ "$ACTION" == complete ]]; then
  [[ ! -e "$SCRAPE_RECEIPT_OUTPUT" ]] || die "SCRAPE_RECEIPT_OUTPUT must not already exist"
else
  [[ -f "$SCRAPE_RECEIPT_OUTPUT" && -r "$SCRAPE_RECEIPT_OUTPUT" ]] || die "SCRAPE_RECEIPT_OUTPUT must be a readable existing receipt for verify"
fi
[[ "$PROMETHEUS_URL" == https://* && "$PROMETHEUS_URL" != *[[:space:]]* && "$PROMETHEUS_URL" != *'?'* && "$PROMETHEUS_URL" != *'#'* ]] || die "PROMETHEUS_URL must be an absolute HTTPS base URL without query, fragment, or whitespace"
[[ -f "$PROMETHEUS_CA_FILE" && -r "$PROMETHEUS_CA_FILE" ]] || die "PROMETHEUS_CA_FILE must be a readable regular file"
tls_receipt_size="$(stat -Lc '%s' -- "$TLS_RECEIPT_INPUT")" || die "cannot stat TLS_RECEIPT_INPUT"
[[ "$tls_receipt_size" =~ ^[0-9]+$ ]] && (( tls_receipt_size > 0 && tls_receipt_size <= MAX_TLS_RECEIPT_BYTES )) || die "TLS_RECEIPT_INPUT must contain 1..${MAX_TLS_RECEIPT_BYTES} bytes"
prometheus_ca_size="$(stat -Lc '%s' -- "$PROMETHEUS_CA_FILE")" || die "cannot stat PROMETHEUS_CA_FILE"
[[ "$prometheus_ca_size" =~ ^[0-9]+$ ]] && (( prometheus_ca_size > 0 && prometheus_ca_size <= MAX_PROMETHEUS_CA_BYTES )) || die "PROMETHEUS_CA_FILE must contain 1..${MAX_PROMETHEUS_CA_BYTES} bytes"
if [[ "$ACTION" == verify ]]; then
  scrape_receipt_size="$(stat -Lc '%s' -- "$SCRAPE_RECEIPT_OUTPUT")" || die "cannot stat SCRAPE_RECEIPT_OUTPUT"
  [[ "$scrape_receipt_size" =~ ^[0-9]+$ ]] && (( scrape_receipt_size > 0 && scrape_receipt_size <= MAX_SCRAPE_RECEIPT_BYTES )) || die "SCRAPE_RECEIPT_OUTPUT must contain 1..${MAX_SCRAPE_RECEIPT_BYTES} bytes"
fi
[[ "$KUBEBRAIN_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "KUBEBRAIN_NAMESPACE must be a DNS label"
[[ "$KUBEBRAIN_SERVICE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "KUBEBRAIN_SERVICE must be a DNS label"
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
operation_is_nonnegative_int64 "$RECOVERY_TIMEOUT_SECONDS" && (( RECOVERY_TIMEOUT_SECONDS <= 86400 )) || die "RECOVERY_TIMEOUT_SECONDS must be a canonical non-negative int64 no greater than 86400"
operation_is_nonnegative_int64 "$POLL_INTERVAL_SECONDS" && (( POLL_INTERVAL_SECONDS <= RECOVERY_TIMEOUT_SECONDS )) || die "POLL_INTERVAL_SECONDS must be canonical, non-negative, and no greater than RECOVERY_TIMEOUT_SECONDS"
operation_is_positive_int64 "$QUERY_TIMEOUT_SECONDS" && (( QUERY_TIMEOUT_SECONDS <= 300 )) || die "QUERY_TIMEOUT_SECONDS must be a canonical positive int64 no greater than 300"
operation_is_nonnegative_int64 "$MAX_CLOCK_SKEW_SECONDS" && (( MAX_CLOCK_SKEW_SECONDS <= 300 )) || die "MAX_CLOCK_SKEW_SECONDS must be a canonical non-negative int64 no greater than 300"
operation_is_positive_int64 "$MAX_SAMPLE_AGE_SECONDS" && (( MAX_SAMPLE_AGE_SECONDS <= 3600 )) || die "MAX_SAMPLE_AGE_SECONDS must be a canonical positive int64 no greater than 3600"
command -v "$CURL" >/dev/null || die "curl is required"
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
command -v "$LN" >/dev/null || die "ln is required"

umask 077
capture="$(mktemp -d)"
trap 'rm -rf "$capture"' EXIT INT TERM
freeze_input() {
  local source="$1" name="$2" destination before after
  before="$(sha256sum "$source" | cut -d ' ' -f1)" || die "cannot hash ${name} before capture"
  destination="$capture/$name"
  cp -- "$source" "$destination" || die "cannot capture ${name}"
  chmod 600 "$destination"
  after="$(sha256sum "$source" | cut -d ' ' -f1)" || die "cannot hash ${name} after capture"
  [[ "$before" =~ ^[a-f0-9]{64}$ && "$after" == "$before" && "$(sha256sum "$destination" | cut -d ' ' -f1)" == "$before" ]] ||
    die "${name} changed during capture"
  printf '%s\n' "$destination"
}
publish_no_replace() {
  local source="$1" destination="$2"
  if ! "$LN" -- "$source" "$destination"; then
    rm -f -- "$source"
    die "scrape recovery receipt already exists; refusing to overwrite"
  fi
  rm -f -- "$source"
}
TLS_RECEIPT_INPUT="$(freeze_input "$TLS_RECEIPT_INPUT" tls-receipt.json)"
PROMETHEUS_CA_FILE="$(freeze_input "$PROMETHEUS_CA_FILE" prometheus-ca)"
scrape_receipt_input="$SCRAPE_RECEIPT_OUTPUT"
if [[ "$ACTION" == verify ]]; then
  scrape_receipt_input="$(freeze_input "$SCRAPE_RECEIPT_OUTPUT" scrape-receipt.json)"
fi
authorization_header_file=""
if [[ -n "$PROMETHEUS_BEARER_TOKEN_FILE" ]]; then
  [[ -f "$PROMETHEUS_BEARER_TOKEN_FILE" && -r "$PROMETHEUS_BEARER_TOKEN_FILE" ]] || die "PROMETHEUS_BEARER_TOKEN_FILE must be a readable regular file"
  token_size="$(stat -Lc '%s' -- "$PROMETHEUS_BEARER_TOKEN_FILE")" || die "cannot stat PROMETHEUS_BEARER_TOKEN_FILE"
  [[ "$token_size" =~ ^[0-9]+$ ]] && (( token_size > 0 && token_size <= MAX_PROMETHEUS_TOKEN_BYTES )) || die "PROMETHEUS_BEARER_TOKEN_FILE must contain 1..${MAX_PROMETHEUS_TOKEN_BYTES} bytes"
  PROMETHEUS_BEARER_TOKEN_FILE="$(freeze_input "$PROMETHEUS_BEARER_TOKEN_FILE" prometheus-token)"
  bearer_token="$(<"$PROMETHEUS_BEARER_TOKEN_FILE")"
  [[ "${#bearer_token}" == "$token_size" ]] || die "PROMETHEUS_BEARER_TOKEN_FILE must contain exactly one token without trailing newlines or NUL bytes"
  [[ "$bearer_token" =~ ^[A-Za-z0-9._~+/-]+=*$ ]] || die "PROMETHEUS_BEARER_TOKEN_FILE must contain one RFC 6750 b64token"
  authorization_header_file="$capture/authorization-header"
  printf 'Authorization: Bearer %s\n' "$bearer_token" >"$authorization_header_file"
  chmod 600 "$authorization_header_file"
  unset bearer_token
fi

tls_receipt_sha="$(sha256sum "$TLS_RECEIPT_INPUT" | cut -d ' ' -f1)"
[[ "$tls_receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "failed to hash TLS receipt"
tls_fields="$("$JQ" -er '
  select(keys == ["completed_at_unix","format","info_endpoint","instance","new_certificate_sha256","old_ca_rejected","old_ca_rejection_required","old_certificate_sha256","pods_unchanged","replicas","rotation_id"]) |
  select(.format == "kubebrain.info-certificate-rotation.receipt.v1" and .pods_unchanged == true) |
  select((.completed_at_unix | type == "number" and . > 0 and . == floor) and (.replicas | type == "number" and . > 0 and . == floor)) |
  select((.instance | type == "string") and (.rotation_id | type == "string") and (.info_endpoint | type == "string")) |
  [.instance,.rotation_id,.info_endpoint,(.completed_at_unix|tostring),(.replicas|tostring),.new_certificate_sha256] | @tsv' "$TLS_RECEIPT_INPUT")" || die "TLS receipt is invalid"
IFS=$'\t' read -r instance rotation_id info_endpoint completed_at tls_replicas new_certificate_sha <<<"$tls_fields"
[[ "$instance" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ && "$rotation_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ && "$new_certificate_sha" =~ ^[a-f0-9]{64}$ ]] || die "TLS receipt identity or certificate digest is invalid"
[[ "$tls_replicas" == "$EXPECTED_REPLICAS" ]] || die "EXPECTED_REPLICAS does not match TLS receipt"

query_url="${PROMETHEUS_URL%/}/api/v1/query"
selector="up{namespace=\"${KUBEBRAIN_NAMESPACE}\",service=\"${KUBEBRAIN_SERVICE}\"}"
curl_args=(--fail-with-body --silent --show-error --max-time "$QUERY_TIMEOUT_SECONDS" --max-filesize "$MAX_RESPONSE_BYTES" --cacert "$PROMETHEUS_CA_FILE" --get -H 'Accept: application/json')
[[ -z "$authorization_header_file" ]] || curl_args+=(-H "@${authorization_header_file}")
deadline=$((SECONDS + RECOVERY_TIMEOUT_SECONDS))
attempt=0
last_reason="no query attempted"

while true; do
  attempt=$((attempt + 1))
  response="$capture/response-${attempt}.json"
  if "$CURL" "${curl_args[@]}" --data-urlencode "query=${selector}" "$query_url" >"$response"; then
    response_size="$(stat -Lc '%s' -- "$response")" || response_size=""
    if [[ ! "$response_size" =~ ^[0-9]+$ ]] || (( response_size > MAX_RESPONSE_BYTES )); then
      last_reason="Prometheus response exceeds ${MAX_RESPONSE_BYTES} bytes"
    else
      observed_at="$(date +%s)"
      operation_is_nonnegative_int64 "$observed_at" || die "invalid observation timestamp"
      oldest_allowed=$((observed_at > MAX_SAMPLE_AGE_SECONDS ? observed_at - MAX_SAMPLE_AGE_SECONDS : 0))
      if "$JQ" -e --arg namespace "$KUBEBRAIN_NAMESPACE" --arg service "$KUBEBRAIN_SERVICE" --argjson expected "$EXPECTED_REPLICAS" --argjson completed "$completed_at" --argjson oldestAllowed "$oldest_allowed" --argjson future "$((observed_at + MAX_CLOCK_SKEW_SECONDS))" '
        type == "object" and .status == "success" and .data.resultType == "vector" and
        (.data.result | type == "array" and length == $expected) and
        all(.data.result[];
          (.metric | type == "object") and .metric.namespace == $namespace and .metric.service == $service and
          (.metric.pod | type == "string" and test("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")) and
          (.metric.instance | type == "string" and length > 0 and (all(explode[]; . >= 32 and . != 127))) and
          (.value | type == "array" and length == 2) and
          (.value[0] | type == "number" and . >= $completed and . >= $oldestAllowed and . <= $future) and .value[1] == "1") and
        ([.data.result[].metric.pod] | unique | length) == $expected and
        ([.data.result[].metric.instance] | unique | length) == $expected
      ' "$response" >/dev/null 2>&1; then
        oldest_sample="$("$JQ" -r '[.data.result[].value[0]] | min' "$response")"
        if [[ "$ACTION" == complete ]]; then
          tmp="${SCRAPE_RECEIPT_OUTPUT}.tmp.$$"
          "$JQ" -cnS --arg instance "$instance" --arg rotation "$rotation_id" --arg endpoint "$info_endpoint" \
            --arg namespace "$KUBEBRAIN_NAMESPACE" --arg service "$KUBEBRAIN_SERVICE" --arg tlsDigest "$tls_receipt_sha" \
            --arg certificate "$new_certificate_sha" --arg query "$selector" --argjson replicas "$EXPECTED_REPLICAS" \
            --argjson completed "$completed_at" --argjson observed "$observed_at" --argjson oldest "$oldest_sample" \
            --slurpfile response "$response" '
            {format:"kubebrain.info-scrape-recovery.receipt.v1",instance:$instance,rotation_id:$rotation,
             info_endpoint:$endpoint,namespace:$namespace,service:$service,replicas:$replicas,
             tls_rotation_completed_at_unix:$completed,tls_rotation_receipt_sha256:$tlsDigest,
             new_certificate_sha256:$certificate,prometheus_query:$query,observed_at_unix:$observed,
             oldest_sample_unix:$oldest,all_targets_up:true,
             targets:($response[0].data.result | sort_by(.metric.pod) | map({pod:.metric.pod,instance:.metric.instance,sample_unix:.value[0]}))}' >"$tmp"
          chmod 600 "$tmp"
          publish_no_replace "$tmp" "$SCRAPE_RECEIPT_OUTPUT"
          echo "info scrape recovery gate passed: instance=${instance} rotation=${rotation_id} replicas=${EXPECTED_REPLICAS} receipt=${SCRAPE_RECEIPT_OUTPUT}"
        else
          "$JQ" -e --arg instance "$instance" --arg rotation "$rotation_id" --arg endpoint "$info_endpoint" \
            --arg namespace "$KUBEBRAIN_NAMESPACE" --arg service "$KUBEBRAIN_SERVICE" --arg tlsDigest "$tls_receipt_sha" \
            --arg certificate "$new_certificate_sha" --arg query "$selector" --argjson replicas "$EXPECTED_REPLICAS" \
            --argjson completed "$completed_at" '
            keys == ["all_targets_up","format","info_endpoint","instance","namespace","new_certificate_sha256","observed_at_unix","oldest_sample_unix","prometheus_query","replicas","rotation_id","service","targets","tls_rotation_completed_at_unix","tls_rotation_receipt_sha256"] and
            .format == "kubebrain.info-scrape-recovery.receipt.v1" and .instance == $instance and
            .rotation_id == $rotation and .info_endpoint == $endpoint and .namespace == $namespace and
            .service == $service and .replicas == $replicas and .tls_rotation_completed_at_unix == $completed and
            .tls_rotation_receipt_sha256 == $tlsDigest and .new_certificate_sha256 == $certificate and
            .prometheus_query == $query and .all_targets_up == true and
            (.observed_at_unix | type == "number" and . >= $completed and . == floor) and
            (.oldest_sample_unix | type == "number" and . >= $completed) and
            (.targets | type == "array" and length == $replicas) and
            ([.targets[].pod] | unique | length) == $replicas and
            ([.targets[].instance] | unique | length) == $replicas and
            all(.targets[]; keys == ["instance","pod","sample_unix"] and
              (.pod | type == "string" and test("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")) and
              (.instance | type == "string" and length > 0) and
              (.sample_unix | type == "number" and . >= $completed))' "$scrape_receipt_input" >/dev/null ||
            die "existing scrape recovery receipt does not match verified evidence"
          echo "info scrape recovery receipt verification passed: instance=${instance} rotation=${rotation_id} replicas=${EXPECTED_REPLICAS} receipt=${SCRAPE_RECEIPT_OUTPUT}"
        fi
        exit 0
      fi
      last_reason="Prometheus up vector is incomplete, stale, pre-rotation, malformed, duplicated, or not all up"
    fi
  else
    last_reason="Prometheus query transport failed"
  fi
  (( SECONDS < deadline )) || die "info scrape recovery timed out: ${last_reason}"
  (( POLL_INTERVAL_SECONDS == 0 )) || sleep "$POLL_INTERVAL_SECONDS"
done
