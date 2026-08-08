#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_LABEL_SELECTOR="${KUBEBRAIN_LABEL_SELECTOR:-app.kubernetes.io/name=kubebrain}"
EXPECTED_READY_PODS="${EXPECTED_READY_PODS:-3}"
ENDPOINT="${ENDPOINT:-}"
READYZ_URL="${READYZ_URL:-}"
PREFIX="${PREFIX:-/}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10s}"
KUBECTL="${KUBECTL:-kubectl}"
CURL="${CURL:-curl}"
GO="${GO:-go}"
ETCDCTL="${ETCDCTL:-etcdctl}"
TIMEOUT_CMD="${TIMEOUT_CMD:-timeout}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
EXPECTED_PREFIX_COUNT="${EXPECTED_PREFIX_COUNT:-}"
EXPECTED_STATUS_CLUSTER_ID="${EXPECTED_STATUS_CLUSTER_ID:-}"
EXPECTED_STATUS_VERSION="${EXPECTED_STATUS_VERSION:-}"
EXPECTED_HASHKV_HASH="${EXPECTED_HASHKV_HASH:-}"
STATUS_ENDPOINTS="${STATUS_ENDPOINTS:-$ENDPOINT}"

if ! [[ "$EXPECTED_READY_PODS" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_READY_PODS must be a positive integer" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PREFIX_COUNT" && ! "$EXPECTED_PREFIX_COUNT" =~ ^[0-9]+$ ]]; then
  echo "EXPECTED_PREFIX_COUNT must be empty or a non-negative integer" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" && ! "$EXPECTED_STATUS_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_STATUS_CLUSTER_ID must be empty or a positive integer" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_VERSION" && ! "$EXPECTED_STATUS_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
  echo "EXPECTED_STATUS_VERSION must be empty or a semver string" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HASHKV_HASH" && ! "$EXPECTED_HASHKV_HASH" =~ ^[0-9]+$ ]]; then
  echo "EXPECTED_HASHKV_HASH must be empty or a non-negative integer" >&2
  exit 2
fi
if ! [[ "$PROBE_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m|h)$ ]]; then
  echo "PROBE_TIMEOUT must be a positive duration ending in ms, s, m, or h" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HASHKV_HASH" && -z "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  echo "EXPECTED_HASHKV_HASH requires EXPECTED_STATUS_CLUSTER_ID" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_VERSION" && -z "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  echo "EXPECTED_STATUS_VERSION requires EXPECTED_STATUS_CLUSTER_ID" >&2
  exit 2
fi
if [[ -z "$ENDPOINT" ]]; then
  echo "ENDPOINT is required" >&2
  exit 2
fi
if [[ -z "$READYZ_URL" ]]; then
  echo "READYZ_URL is required" >&2
  exit 2
fi
contains_unsafe_probe_value() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
for variable in ENDPOINT READYZ_URL PREFIX STATUS_ENDPOINTS; do
  value="${!variable}"
  if contains_unsafe_probe_value "$value"; then
    echo "${variable} contains unsupported characters" >&2
    exit 2
  fi
done
if [[ "$STATUS_ENDPOINTS" == ,* || "$STATUS_ENDPOINTS" == *, || "$STATUS_ENDPOINTS" == *,,* ]]; then
  echo "STATUS_ENDPOINTS contains an empty endpoint" >&2
  exit 2
fi
IFS=',' read -r -a status_endpoint_array <<<"$STATUS_ENDPOINTS"
declare -A seen_status_endpoints=()
for status_endpoint in "${status_endpoint_array[@]}"; do
  if [[ -z "$status_endpoint" ]]; then
    echo "STATUS_ENDPOINTS contains an empty endpoint" >&2
    exit 2
  fi
  if [[ -n "${seen_status_endpoints[$status_endpoint]:-}" ]]; then
    echo "STATUS_ENDPOINTS must not contain duplicate endpoints: ${status_endpoint}" >&2
    exit 2
  fi
  seen_status_endpoints[$status_endpoint]=1
done
prefix_endpoint_array=("$ENDPOINT")
declare -A seen_prefix_endpoints=()
seen_prefix_endpoints[$ENDPOINT]=1
for status_endpoint in "${status_endpoint_array[@]}"; do
  if [[ -z "${seen_prefix_endpoints[$status_endpoint]:-}" ]]; then
    prefix_endpoint_array+=("$status_endpoint")
    seen_prefix_endpoints[$status_endpoint]=1
  fi
done
run_with_probe_timeout() {
  "$TIMEOUT_CMD" "$PROBE_TIMEOUT" "$@"
}

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

pods_json="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get pods -l "$KUBEBRAIN_LABEL_SELECTOR" -o json)"
ready_pods="$(printf '%s' "$pods_json" | "$JQ" -r '
  [
    .items[]
    | select(.metadata.deletionTimestamp == null)
    | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
  ] | length
')"
total_pods="$(printf '%s' "$pods_json" | "$JQ" -r '[.items[] | select(.metadata.deletionTimestamp == null)] | length')"
if [[ "$ready_pods" != "$EXPECTED_READY_PODS" || "$total_pods" != "$EXPECTED_READY_PODS" ]]; then
  echo "KubeBrain Ready pod count mismatch: expected ${EXPECTED_READY_PODS}/${EXPECTED_READY_PODS}, got ready/total ${ready_pods}/${total_pods}" >&2
  exit 1
fi

readyz="$(run_with_probe_timeout "$CURL" -fsS "$READYZ_URL")"
if [[ "$readyz" != "ok" ]]; then
  echo "readyz mismatch: expected ok, got ${readyz}" >&2
  exit 1
fi

prefix_count=""
first_prefix_endpoint=""
for prefix_endpoint in "${prefix_endpoint_array[@]}"; do
  current_prefix_count="$(ENDPOINT="$prefix_endpoint" ACTION=count PREFIX="$PREFIX" TIMEOUT="$PROBE_TIMEOUT" \
    "$TIMEOUT_CMD" "$PROBE_TIMEOUT" "$GO" run "$ROOT_DIR/hack/backup/cmd/prefix-tool")"
  current_prefix_count="$(printf '%s' "$current_prefix_count" | tr -d '[:space:]')"
  if ! [[ "$current_prefix_count" =~ ^[0-9]+$ ]]; then
    echo "prefix count probe for ${prefix_endpoint} returned non-numeric output: ${current_prefix_count}" >&2
    exit 1
  fi
  if [[ -n "$EXPECTED_PREFIX_COUNT" && "$current_prefix_count" != "$EXPECTED_PREFIX_COUNT" ]]; then
    echo "prefix count mismatch for ${prefix_endpoint}: expected ${EXPECTED_PREFIX_COUNT}, got ${current_prefix_count}" >&2
    exit 1
  fi
  if [[ -z "$prefix_count" ]]; then
    prefix_count="$current_prefix_count"
    first_prefix_endpoint="$prefix_endpoint"
  elif [[ "$current_prefix_count" != "$prefix_count" ]]; then
    echo "prefix count mismatch across endpoints: expected ${prefix_count} from ${first_prefix_endpoint}, got ${current_prefix_count} from ${prefix_endpoint}" >&2
    exit 1
  fi
done

status_summary=""
if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  status_json="$(ETCDCTL_API=3 run_with_probe_timeout "$ETCDCTL" --endpoints="$STATUS_ENDPOINTS" endpoint status -w json)"
  expected_status_endpoints="${#status_endpoint_array[@]}"
  status_count="$(printf '%s' "$status_json" | "$JQ" -r 'if type == "array" then length else 0 end')"
  if [[ "$status_count" != "$expected_status_endpoints" ]]; then
    echo "status endpoint count mismatch: expected ${expected_status_endpoints}, got ${status_count}" >&2
    exit 1
  fi
  expected_status_endpoint_set="$(printf '%s\n' "${status_endpoint_array[@]}" | LC_ALL=C sort | paste -sd, -)"
  actual_status_endpoint_set="$(printf '%s' "$status_json" | "$JQ" -r '
    if (type != "array" or any(.[]; (.Endpoint // "") == "")) then
      "invalid"
    else
      ([.[].Endpoint] | sort | join(","))
    end
  ')"
  if [[ "$actual_status_endpoint_set" != "$expected_status_endpoint_set" ]]; then
    echo "status endpoint set mismatch: expected ${expected_status_endpoint_set}, got ${actual_status_endpoint_set}" >&2
    exit 1
  fi
  status_payload_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.Status == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_payload_missing" ]]; then
    echo "status payload is required for endpoints: ${status_payload_missing}" >&2
    exit 1
  fi
  status_db_size_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.Status | has("dbSize")) then .Status.dbSize elif (.Status | has("db_size")) then .Status.db_size else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_db_size_missing" ]]; then
    echo "status dbSize is required for endpoints: ${status_db_size_missing}" >&2
    exit 1
  fi
  status_header_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.Status.header == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_header_missing" ]]; then
    echo "status header is required for endpoints: ${status_header_missing}" >&2
    exit 1
  fi
  status_revision_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.Status.header.revision == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_revision_missing" ]]; then
    echo "status revision is required for endpoints: ${status_revision_missing}" >&2
    exit 1
  fi
  status_member_id_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.Status.header | has("member_id")) then .Status.header.member_id elif (.Status.header | has("memberId")) then .Status.header.memberId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_member_id_missing" ]]; then
    echo "status member ID is required for endpoints: ${status_member_id_missing}" >&2
    exit 1
  fi
  status_cluster_id_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.Status.header | has("cluster_id")) then .Status.header.cluster_id elif (.Status.header | has("clusterId")) then .Status.header.clusterId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_cluster_id_missing" ]]; then
    echo "status cluster ID is required for endpoints: ${status_cluster_id_missing}" >&2
    exit 1
  fi
  status_numeric_type_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if (((if ($item.Status.header | has("cluster_id")) then $item.Status.header.cluster_id elif ($item.Status.header | has("clusterId")) then $item.Status.header.clusterId else null end) | type) != "number") then "cluster_id" else empty end),
            (if (((if ($item.Status.header | has("member_id")) then $item.Status.header.member_id elif ($item.Status.header | has("memberId")) then $item.Status.header.memberId else null end) | type) != "number") then "member_id" else empty end),
            (if (($item.Status.header.revision | type) != "number") then "revision" else empty end),
            (if (((if ($item.Status | has("dbSize")) then $item.Status.dbSize elif ($item.Status | has("db_size")) then $item.Status.db_size else null end) | type) != "number") then "dbSize" else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_numeric_type_violations" ]]; then
    echo "status numeric fields must be JSON numbers: ${status_numeric_type_violations}" >&2
    exit 1
  fi
  status_integer_type_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      def noninteger: . != floor;
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if ((if ($item.Status.header | has("cluster_id")) then $item.Status.header.cluster_id elif ($item.Status.header | has("clusterId")) then $item.Status.header.clusterId else null end) | noninteger) then "cluster_id" else empty end),
            (if ((if ($item.Status.header | has("member_id")) then $item.Status.header.member_id elif ($item.Status.header | has("memberId")) then $item.Status.header.memberId else null end) | noninteger) then "member_id" else empty end),
            (if ($item.Status.header.revision | noninteger) then "revision" else empty end),
            (if ((if ($item.Status | has("dbSize")) then $item.Status.dbSize elif ($item.Status | has("db_size")) then $item.Status.db_size else null end) | noninteger) then "dbSize" else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_integer_type_violations" ]]; then
    echo "status numeric fields must be JSON integers: ${status_integer_type_violations}" >&2
    exit 1
  fi
  status_raft_term_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status.header | has("raft_term")) then $item.Status.header.raft_term elif ($item.Status.header | has("raftTerm")) then $item.Status.header.raftTerm else null end) as $header_term
        | (if ($item.Status | has("raftTerm")) then $item.Status.raftTerm elif ($item.Status | has("raft_term")) then $item.Status.raft_term else null end) as $status_term
        | (
            if ($header_term == null and $status_term == null) then
              empty
            elif ($header_term == null or $status_term == null) then
              "missing_pair"
            elif (($header_term | type) != "number" or ($status_term | type) != "number") then
              "not_number"
            elif ($header_term != ($header_term | floor) or $status_term != ($status_term | floor)) then
              "not_integer"
            elif ($header_term != $status_term) then
              "mismatch"
            elif ($header_term <= 0 or $status_term <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_raft_term_violations" ]]; then
    echo "status raft term envelope invalid: ${status_raft_term_violations}" >&2
    exit 1
  fi
  status_raft_index_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | $item.Status.header.revision as $revision
        | (if ($item.Status | has("raftIndex")) then $item.Status.raftIndex elif ($item.Status | has("raft_index")) then $item.Status.raft_index else null end) as $raft_index
        | (if ($item.Status | has("raftAppliedIndex")) then $item.Status.raftAppliedIndex elif ($item.Status | has("raft_applied_index")) then $item.Status.raft_applied_index else null end) as $applied_index
        | (
            if ($raft_index == null and $applied_index == null) then
              empty
            elif ($raft_index == null or $applied_index == null) then
              "missing_pair"
            elif (($raft_index | type) != "number" or ($applied_index | type) != "number") then
              "not_number"
            elif ($raft_index != ($raft_index | floor) or $applied_index != ($applied_index | floor)) then
              "not_integer"
            elif ($raft_index < 0 or $applied_index < 0) then
              "negative"
            elif ($applied_index > $raft_index) then
              "applied_beyond_raft_index"
            elif ($raft_index != $revision or $applied_index != $revision) then
              "not_revision"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_raft_index_violations" ]]; then
    echo "status raft index envelope invalid: ${status_raft_index_violations}" >&2
    exit 1
  fi
  status_db_size_in_use_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("dbSize")) then $item.Status.dbSize elif ($item.Status | has("db_size")) then $item.Status.db_size else null end) as $db_size
        | (if ($item.Status | has("dbSizeInUse")) then $item.Status.dbSizeInUse elif ($item.Status | has("db_size_in_use")) then $item.Status.db_size_in_use else null end) as $db_size_in_use
        | (
            if ($db_size == null or $db_size_in_use == null) then
              empty
            elif (($db_size | type) != "number" or ($db_size_in_use | type) != "number") then
              "not_number"
            elif ($db_size != ($db_size | floor) or $db_size_in_use != ($db_size_in_use | floor)) then
              "not_integer"
            elif ($db_size < 0 or $db_size_in_use < 0) then
              "negative"
            elif ($db_size_in_use > $db_size) then
              "in_use_beyond_db_size"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_db_size_in_use_violations" ]]; then
    echo "status dbSizeInUse envelope invalid: ${status_db_size_in_use_violations}" >&2
    exit 1
  fi
  status_version_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | $item.Status.version as $version
        | (
            if $version == null then
              empty
            elif (($version | type) != "string") then
              "not_string"
            elif ($version | test("^[0-9]+\\.[0-9]+\\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$") | not) then
              "not_semver"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_version_violations" ]]; then
    echo "status version envelope invalid: ${status_version_violations}" >&2
    exit 1
  fi
  if [[ -n "$EXPECTED_STATUS_VERSION" ]]; then
    status_version_values="$(printf '%s' "$status_json" | "$JQ" -r '
      if type != "array" then
        "invalid"
      else
        [.[].Status.version] | unique | join(",")
      end
    ')"
    if [[ "$status_version_values" != "$EXPECTED_STATUS_VERSION" ]]; then
      echo "status version mismatch: expected ${EXPECTED_STATUS_VERSION}, got ${status_version_values}" >&2
      exit 1
    fi
  fi
  status_storage_version_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("storageVersion")) then $item.Status.storageVersion elif ($item.Status | has("storage_version")) then $item.Status.storage_version else null end) as $storage_version
        | (
            if $storage_version == null then
              empty
            elif (($storage_version | type) != "string") then
              "not_string"
            elif ($storage_version | test("^[0-9]+\\.[0-9]+(\\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?)?$") | not) then
              "not_version"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_storage_version_violations" ]]; then
    echo "status storageVersion envelope invalid: ${status_storage_version_violations}" >&2
    exit 1
  fi
  status_db_size_quota_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("dbSizeQuota")) then $item.Status.dbSizeQuota elif ($item.Status | has("db_size_quota")) then $item.Status.db_size_quota else null end) as $quota
        | (
            if $quota == null then
              empty
            elif (($quota | type) != "number") then
              "not_number"
            elif ($quota != ($quota | floor)) then
              "not_integer"
            elif ($quota <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_db_size_quota_violations" ]]; then
    echo "status dbSizeQuota envelope invalid: ${status_db_size_quota_violations}" >&2
    exit 1
  fi
  status_is_learner_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("isLearner")) then $item.Status.isLearner elif ($item.Status | has("is_learner")) then $item.Status.is_learner else null end) as $is_learner
        | (
            if $is_learner == null then
              empty
            elif (($is_learner | type) != "boolean") then
              "not_boolean"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_is_learner_violations" ]]; then
    echo "status isLearner envelope invalid: ${status_is_learner_violations}" >&2
    exit 1
  fi
  status_downgrade_info_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("downgradeInfo")) then $item.Status.downgradeInfo elif ($item.Status | has("downgrade_info")) then $item.Status.downgrade_info else null end) as $downgrade_info
        | (
            if $downgrade_info == null then
              empty
            elif (($downgrade_info | type) != "object") then
              "not_object"
            else
              (if ($downgrade_info | has("enabled")) then $downgrade_info.enabled else null end) as $enabled
              | (if ($downgrade_info | has("targetVersion")) then $downgrade_info.targetVersion elif ($downgrade_info | has("target_version")) then $downgrade_info.target_version else null end) as $target_version
              | if ($enabled != null and (($enabled | type) != "boolean")) then
                  "enabled_not_boolean"
                elif ($target_version != null and (($target_version | type) != "string")) then
                  "target_not_string"
                elif ($target_version != null and $target_version != "" and ($target_version | test("^[0-9]+\\.[0-9]+\\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$") | not)) then
                  "target_not_semver"
                elif ($enabled == true and ($target_version == null or $target_version == "")) then
                  "enabled_without_target"
                else
                  empty
                end
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_downgrade_info_violations" ]]; then
    echo "status downgradeInfo envelope invalid: ${status_downgrade_info_violations}" >&2
    exit 1
  fi
  status_leader_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("leader")) then $item.Status.leader elif ($item.Status | has("leader_id")) then $item.Status.leader_id elif ($item.Status | has("leaderId")) then $item.Status.leaderId else null end) as $leader
        | (
            if $leader == null then
              empty
            elif (($leader | type) != "number") then
              "not_number"
            elif ($leader != ($leader | floor)) then
              "not_integer"
            elif ($leader <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_leader_violations" ]]; then
    echo "status leader envelope invalid: ${status_leader_violations}" >&2
    exit 1
  fi
  status_error_values="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | ((if ($item.Status | has("errors")) then $item.Status.errors elif ($item.Status | has("Errors")) then $item.Status.Errors else [] end) as $errors
          | if (($errors | type) != "array") then
              "\($endpoint): non_array"
            elif (any($errors[]; type != "string")) then
              "\($endpoint): non_string_array"
            elif (($errors | length) > 0) then
              "\($endpoint): \($errors | join("|"))"
            else
              empty
            end)
      ] | join(";")
    end
  ')"
  if [[ -n "$status_error_values" ]]; then
    echo "status errors must be empty: ${status_error_values}" >&2
    exit 1
  fi
  status_values="$(printf '%s' "$status_json" | "$JQ" -r '
    if (type != "array" or length == 0) then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        ([.[].Status.header | if has("cluster_id") then .cluster_id elif has("clusterId") then .clusterId else empty end] | unique | join(",")),
        ([.[].Status.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | join(",")),
        ([.[].Status.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | unique | join(",")),
        ([.[].Status.header | .revision] | min),
        ([.[].Status | if has("dbSize") then .dbSize elif has("db_size") then .db_size else empty end] | min),
        ([.[].Status | if has("dbSizeInUse") then .dbSizeInUse elif has("db_size_in_use") then .db_size_in_use else empty end] | if length == 0 then "-" else min end),
        ([.[].Status | if has("version") then .version else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("storageVersion") then .storageVersion elif has("storage_version") then .storage_version else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("dbSizeQuota") then .dbSizeQuota elif has("db_size_quota") then .db_size_quota else empty end] | if length == 0 then "-" else min end),
        ([.[].Status | if has("isLearner") then .isLearner elif has("is_learner") then .is_learner else empty end] | unique | map(tostring) | join(",") | if . == "" then "-" else . end),
        ([.[].Status | (if has("downgradeInfo") then .downgradeInfo elif has("downgrade_info") then .downgrade_info else empty end) | if has("enabled") then .enabled else empty end] | unique | map(tostring) | join(",") | if . == "" then "-" else . end),
        ([.[].Status | (if has("downgradeInfo") then .downgradeInfo elif has("downgrade_info") then .downgrade_info else empty end) | (if has("targetVersion") then .targetVersion elif has("target_version") then .target_version else empty end) | select(. != "")] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("leader") then .leader elif has("leader_id") then .leader_id elif has("leaderId") then .leaderId else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("raftTerm") then .raftTerm elif has("raft_term") then .raft_term else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("raftIndex") then .raftIndex elif has("raft_index") then .raft_index else empty end] | if length == 0 then "-" else min end),
        ([.[].Status | if has("raftAppliedIndex") then .raftAppliedIndex elif has("raft_applied_index") then .raft_applied_index else empty end] | if length == 0 then "-" else min end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r status_cluster_ids status_member_ids unique_status_member_ids min_status_revision min_status_db_size min_status_db_size_in_use status_versions status_storage_versions min_status_db_size_quota status_is_learners status_downgrade_enableds status_downgrade_target_versions status_leader_ids status_raft_terms min_status_raft_index min_status_raft_applied_index <<<"$status_values"
  if [[ "$status_cluster_ids" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${status_cluster_ids}" >&2
    exit 1
  fi
  IFS=',' read -r -a status_member_id_array <<<"$status_member_ids"
  IFS=',' read -r -a unique_status_member_id_array <<<"$unique_status_member_ids"
  if [[ "${#status_member_id_array[@]}" != "$expected_status_endpoints" ]]; then
    echo "status member ID count mismatch: expected ${expected_status_endpoints}, got ${#status_member_id_array[@]}" >&2
    exit 1
  fi
  if [[ "${#unique_status_member_id_array[@]}" != "$expected_status_endpoints" ]]; then
    echo "status member IDs must be unique, got ${status_member_ids}" >&2
    exit 1
  fi
  for status_member_id in "${status_member_id_array[@]}"; do
    if ! [[ "$status_member_id" =~ ^[1-9][0-9]*$ ]]; then
      echo "status member ID must be positive, got ${status_member_id}" >&2
      exit 1
    fi
  done
  if ! [[ "$min_status_revision" =~ ^[0-9]+$ ]]; then
    echo "status revision must be non-negative, got ${min_status_revision}" >&2
    exit 1
  fi
  if ! [[ "$min_status_db_size" =~ ^[1-9][0-9]*$ ]]; then
    echo "status dbSize must be positive, got ${min_status_db_size}" >&2
    exit 1
  fi
  status_summary=", status_cluster_id=${status_cluster_ids}, status_member_ids=${status_member_ids}, min_status_revision=${min_status_revision}, min_status_db_size=${min_status_db_size}"
  if [[ "$min_status_db_size_in_use" != "-" ]]; then
    status_summary+=", min_status_db_size_in_use=${min_status_db_size_in_use}"
  fi
  status_summary+=", status_errors=empty"
  if [[ "$status_versions" != "-" ]]; then
    status_summary+=", status_version=${status_versions}"
  fi
  if [[ "$status_storage_versions" != "-" ]]; then
    status_summary+=", status_storage_versions=${status_storage_versions}"
  fi
  if [[ "$min_status_db_size_quota" != "-" ]]; then
    status_summary+=", min_status_db_size_quota=${min_status_db_size_quota}"
  fi
  if [[ "$status_is_learners" != "-" ]]; then
    status_summary+=", status_is_learners=${status_is_learners}"
  fi
  if [[ "$status_downgrade_enableds" != "-" ]]; then
    status_summary+=", status_downgrade_enableds=${status_downgrade_enableds}"
  fi
  if [[ "$status_downgrade_target_versions" != "-" ]]; then
    status_summary+=", status_downgrade_target_versions=${status_downgrade_target_versions}"
  fi
  if [[ "$status_leader_ids" != "-" ]]; then
    status_summary+=", status_leader_ids=${status_leader_ids}"
  fi
  if [[ "$status_raft_terms" != "-" ]]; then
    status_summary+=", status_raft_terms=${status_raft_terms}"
  fi
  if [[ "$min_status_raft_index" != "-" ]]; then
    status_summary+=", min_status_raft_index=${min_status_raft_index}"
  fi
  if [[ "$min_status_raft_applied_index" != "-" ]]; then
    status_summary+=", min_status_raft_applied_index=${min_status_raft_applied_index}"
    status_summary+=", raft_indexes_match_revision=true"
  fi
fi

hashkv_summary=""
if [[ -n "$EXPECTED_HASHKV_HASH" ]]; then
  hashkv_json="$(ETCDCTL_API=3 run_with_probe_timeout "$ETCDCTL" --endpoints="$STATUS_ENDPOINTS" endpoint hashkv -w json)"
  expected_hashkv_endpoints="${#status_endpoint_array[@]}"
  hashkv_count="$(printf '%s' "$hashkv_json" | "$JQ" -r 'if type == "array" then length else 0 end')"
  if [[ "$hashkv_count" != "$expected_hashkv_endpoints" ]]; then
    echo "hashkv endpoint count mismatch: expected ${expected_hashkv_endpoints}, got ${hashkv_count}" >&2
    exit 1
  fi
  actual_hashkv_endpoint_set="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if (type != "array" or any(.[]; (.Endpoint // "") == "")) then
      "invalid"
    else
      ([.[].Endpoint] | sort | join(","))
    end
  ')"
  if [[ "$actual_hashkv_endpoint_set" != "$expected_status_endpoint_set" ]]; then
    echo "hashkv endpoint set mismatch: expected ${expected_status_endpoint_set}, got ${actual_hashkv_endpoint_set}" >&2
    exit 1
  fi
  hashkv_payload_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_payload_missing" ]]; then
    echo "hashkv payload is required for endpoints: ${hashkv_payload_missing}" >&2
    exit 1
  fi
  hashkv_hash_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV.hash == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_hash_missing" ]]; then
    echo "hashkv hash is required for endpoints: ${hashkv_hash_missing}" >&2
    exit 1
  fi
  hashkv_compact_revision_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.HashKV | has("compact_revision")) then .HashKV.compact_revision elif (.HashKV | has("compactRevision")) then .HashKV.compactRevision else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_compact_revision_missing" ]]; then
    echo "hashkv compact revision is required for endpoints: ${hashkv_compact_revision_missing}" >&2
    exit 1
  fi
  hashkv_header_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV.header == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_header_missing" ]]; then
    echo "hashkv header is required for endpoints: ${hashkv_header_missing}" >&2
    exit 1
  fi
  hashkv_member_id_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.HashKV.header | has("member_id")) then .HashKV.header.member_id elif (.HashKV.header | has("memberId")) then .HashKV.header.memberId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_member_id_missing" ]]; then
    echo "hashkv member ID is required for endpoints: ${hashkv_member_id_missing}" >&2
    exit 1
  fi
  hashkv_cluster_id_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.HashKV.header | has("cluster_id")) then .HashKV.header.cluster_id elif (.HashKV.header | has("clusterId")) then .HashKV.header.clusterId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_cluster_id_missing" ]]; then
    echo "hashkv cluster ID is required for endpoints: ${hashkv_cluster_id_missing}" >&2
    exit 1
  fi
  hashkv_revision_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV.header.revision == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_revision_missing" ]]; then
    echo "hashkv revision is required for endpoints: ${hashkv_revision_missing}" >&2
    exit 1
  fi
  hashkv_numeric_type_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if (((if ($item.HashKV.header | has("cluster_id")) then $item.HashKV.header.cluster_id elif ($item.HashKV.header | has("clusterId")) then $item.HashKV.header.clusterId else null end) | type) != "number") then "cluster_id" else empty end),
            (if (((if ($item.HashKV.header | has("member_id")) then $item.HashKV.header.member_id elif ($item.HashKV.header | has("memberId")) then $item.HashKV.header.memberId else null end) | type) != "number") then "member_id" else empty end),
            (if (($item.HashKV.header.revision | type) != "number") then "revision" else empty end),
            (if (($item.HashKV.hash | type) != "number") then "hash" else empty end),
            (if (((if ($item.HashKV | has("compact_revision")) then $item.HashKV.compact_revision elif ($item.HashKV | has("compactRevision")) then $item.HashKV.compactRevision else null end) | type) != "number") then "compact_revision" else empty end),
            (if (($item.HashKV | has("hash_revision")) or ($item.HashKV | has("hashRevision"))) then
              (if (((if ($item.HashKV | has("hash_revision")) then $item.HashKV.hash_revision else $item.HashKV.hashRevision end) | type) != "number") then "hash_revision" else empty end)
            else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_numeric_type_violations" ]]; then
    echo "hashkv numeric fields must be JSON numbers: ${hashkv_numeric_type_violations}" >&2
    exit 1
  fi
  hashkv_integer_type_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      def noninteger: . != floor;
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if ((if ($item.HashKV.header | has("cluster_id")) then $item.HashKV.header.cluster_id elif ($item.HashKV.header | has("clusterId")) then $item.HashKV.header.clusterId else null end) | noninteger) then "cluster_id" else empty end),
            (if ((if ($item.HashKV.header | has("member_id")) then $item.HashKV.header.member_id elif ($item.HashKV.header | has("memberId")) then $item.HashKV.header.memberId else null end) | noninteger) then "member_id" else empty end),
            (if ($item.HashKV.header.revision | noninteger) then "revision" else empty end),
            (if ($item.HashKV.hash | noninteger) then "hash" else empty end),
            (if ((if ($item.HashKV | has("compact_revision")) then $item.HashKV.compact_revision elif ($item.HashKV | has("compactRevision")) then $item.HashKV.compactRevision else null end) | noninteger) then "compact_revision" else empty end),
            (if (($item.HashKV | has("hash_revision")) or ($item.HashKV | has("hashRevision"))) then
              (if ((if ($item.HashKV | has("hash_revision")) then $item.HashKV.hash_revision else $item.HashKV.hashRevision end) | noninteger) then "hash_revision" else empty end)
            else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_integer_type_violations" ]]; then
    echo "hashkv numeric fields must be JSON integers: ${hashkv_integer_type_violations}" >&2
    exit 1
  fi
  hashkv_hash_revision_presence="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      ([.[] | select((.HashKV | has("hash_revision")) or (.HashKV | has("hashRevision")))] | length)
    end
  ')"
  if [[ "$hashkv_hash_revision_presence" != "0" && "$hashkv_hash_revision_presence" != "$hashkv_count" ]]; then
    echo "hashkv hash revision must be present on all endpoints when present: present=${hashkv_hash_revision_presence}, total=${hashkv_count}" >&2
    exit 1
  fi
  hashkv_hash_revision_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((.HashKV | has("hash_revision")) or (.HashKV | has("hashRevision")))
        | {
            endpoint: (.Endpoint // "unknown"),
            header_revision: .HashKV.header.revision,
            hash_revision: (if (.HashKV | has("hash_revision")) then .HashKV.hash_revision else .HashKV.hashRevision end)
          }
        | select(.hash_revision != .header_revision)
        | "\(.endpoint): hash_revision=\(.hash_revision), header_revision=\(.header_revision)"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_hash_revision_violations" ]]; then
    echo "hashkv hash revision must match header revision: ${hashkv_hash_revision_violations}" >&2
    exit 1
  fi
  hashkv_raft_term_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.HashKV.header | has("raft_term")) then $item.HashKV.header.raft_term elif ($item.HashKV.header | has("raftTerm")) then $item.HashKV.header.raftTerm else null end) as $term
        | (
            if $term == null then
              empty
            elif (($term | type) != "number") then
              "not_number"
            elif ($term != ($term | floor)) then
              "not_integer"
            elif ($term <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_raft_term_violations" ]]; then
    echo "hashkv raft term envelope invalid: ${hashkv_raft_term_violations}" >&2
    exit 1
  fi
  hashkv_revision_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | {
            endpoint: (.Endpoint // "unknown"),
            revision: (if (.HashKV | has("hash_revision")) then .HashKV.hash_revision elif (.HashKV | has("hashRevision")) then .HashKV.hashRevision else .HashKV.header.revision end),
            compact_revision: (if (.HashKV | has("compact_revision")) then .HashKV.compact_revision elif (.HashKV | has("compactRevision")) then .HashKV.compactRevision else null end)
          }
        | select(.compact_revision > .revision)
        | "\(.endpoint): compact=\(.compact_revision), hash=\(.revision)"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_revision_violations" ]]; then
    echo "hashkv compact revision must not exceed hash revision: ${hashkv_revision_violations}" >&2
    exit 1
  fi
  hashkv_values="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if (type != "array" or length == 0) then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        ([.[].HashKV.header | if has("cluster_id") then .cluster_id elif has("clusterId") then .clusterId else empty end] | unique | join(",")),
        ([.[].HashKV.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | join(",")),
        ([.[].HashKV.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | unique | join(",")),
        ([.[].HashKV | .hash] | unique | join(",")),
        ([.[].HashKV.header | .revision] | min),
        ([.[].HashKV | if has("compact_revision") then .compact_revision elif has("compactRevision") then .compactRevision else empty end] | min),
        ([.[].HashKV.header | if has("raft_term") then .raft_term elif has("raftTerm") then .raftTerm else empty end] | unique | join(",") | if . == "" then "-" else . end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r hashkv_cluster_ids hashkv_member_ids unique_hashkv_member_ids hashkv_hashes min_hashkv_revision min_hashkv_compact_revision hashkv_raft_terms <<<"$hashkv_values"
  if [[ "$hashkv_cluster_ids" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "hashkv cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${hashkv_cluster_ids}" >&2
    exit 1
  fi
  IFS=',' read -r -a hashkv_member_id_array <<<"$hashkv_member_ids"
  IFS=',' read -r -a unique_hashkv_member_id_array <<<"$unique_hashkv_member_ids"
  if [[ "${#hashkv_member_id_array[@]}" != "$expected_hashkv_endpoints" ]]; then
    echo "hashkv member ID count mismatch: expected ${expected_hashkv_endpoints}, got ${#hashkv_member_id_array[@]}" >&2
    exit 1
  fi
  if [[ "${#unique_hashkv_member_id_array[@]}" != "$expected_hashkv_endpoints" ]]; then
    echo "hashkv member IDs must be unique, got ${hashkv_member_ids}" >&2
    exit 1
  fi
  for hashkv_member_id in "${hashkv_member_id_array[@]}"; do
    if ! [[ "$hashkv_member_id" =~ ^[1-9][0-9]*$ ]]; then
      echo "hashkv member ID must be positive, got ${hashkv_member_id}" >&2
      exit 1
    fi
  done
  if [[ "$hashkv_hashes" != "$EXPECTED_HASHKV_HASH" ]]; then
    echo "hashkv hash mismatch: expected ${EXPECTED_HASHKV_HASH}, got ${hashkv_hashes}" >&2
    exit 1
  fi
  if ! [[ "$min_hashkv_revision" =~ ^[0-9]+$ ]]; then
    echo "hashkv revision must be non-negative, got ${min_hashkv_revision}" >&2
    exit 1
  fi
  if ! [[ "$min_hashkv_compact_revision" =~ ^[0-9]+$ ]]; then
    echo "hashkv compact revision must be non-negative, got ${min_hashkv_compact_revision}" >&2
    exit 1
  fi
  if (( min_hashkv_compact_revision > min_hashkv_revision )); then
    echo "hashkv compact revision must not exceed hash revision: compact=${min_hashkv_compact_revision}, hash=${min_hashkv_revision}" >&2
    exit 1
  fi
  if [[ "${min_status_revision:-}" != "$min_hashkv_revision" ]]; then
    echo "status/hashkv revision mismatch: status=${min_status_revision:-missing}, hashkv=${min_hashkv_revision}" >&2
    exit 1
  fi
  hashkv_summary=", hashkv_member_ids=${hashkv_member_ids}, hashkv_hash=${hashkv_hashes}, min_hashkv_revision=${min_hashkv_revision}, min_hashkv_compact_revision=${min_hashkv_compact_revision}"
  hashkv_summary+=", revisions_match=true"
  if [[ "$hashkv_raft_terms" != "-" ]]; then
    hashkv_summary+=", hashkv_raft_terms=${hashkv_raft_terms}"
    if [[ "${status_raft_terms:-"-"}" != "-" ]]; then
      if [[ "$hashkv_raft_terms" != "$status_raft_terms" ]]; then
        echo "status/hashkv raft term mismatch: status=${status_raft_terms}, hashkv=${hashkv_raft_terms}" >&2
        exit 1
      fi
      hashkv_summary+=", raft_terms_match=true"
    fi
  fi
fi

echo "dataplane readonly gate passed: ready_pods=${ready_pods}, readyz=ok, prefix_count=${prefix_count}${status_summary}${hashkv_summary}"
