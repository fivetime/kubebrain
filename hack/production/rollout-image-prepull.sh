#!/usr/bin/env bash
# Sourced only by the rollout runner. The Go helper owns Kubernetes policy,
# durable recovery and exact OCI validation; this file owns phase ordering.

image_prepull_configure() {
  image_prepull_started=false
  image_prepull_cleaned=false
  [[ -n "$TARGET_IMAGE" ]] || return 0
  IMAGE_PREPULL_BIN="${IMAGE_PREPULL_BIN:-/usr/local/bin/kubebrain-image-prepull}"
  IMAGE_PREPULL_PREPARE_TIMEOUT="${IMAGE_PREPULL_PREPARE_TIMEOUT:-600s}"
  IMAGE_PREPULL_VERIFY_TIMEOUT="${IMAGE_PREPULL_VERIFY_TIMEOUT:-30s}"
  IMAGE_PREPULL_CLEANUP_TIMEOUT="${IMAGE_PREPULL_CLEANUP_TIMEOUT:-30s}"
  local variable seconds
  for variable in KUBECONFIG KUBECTL_CONTEXT IMAGE_PREPULL_NAMESPACE_UID IMAGE_PREPULL_RECEIPT_DIRECTORY \
    IMAGE_PREPULL_INDEX_FILE IMAGE_PREPULL_AMD64_DIGEST IMAGE_PREPULL_ARM64_DIGEST; do
    if [[ -z "${!variable:-}" ]]; then
      echo "candidate rollout requires ${variable} for isolated image preparation" >&2
      return 2
    fi
  done
  if [[ "$KUBECONFIG" != /* || "$KUBECONFIG" == *:* || ! -f "$KUBECONFIG" || -L "$KUBECONFIG" ||
    "$(stat -c '%a' -- "$KUBECONFIG")" != 600 ]]; then
    echo "candidate rollout requires one explicit absolute regular 0600 KUBECONFIG" >&2
    return 2
  fi
  if [[ "$IMAGE_PREPULL_BIN" != /* || ! -x "$IMAGE_PREPULL_BIN" ||
    "$IMAGE_PREPULL_RECEIPT_DIRECTORY" != /* || ! -d "$IMAGE_PREPULL_RECEIPT_DIRECTORY" ||
    -L "$IMAGE_PREPULL_RECEIPT_DIRECTORY" ||
    "$(stat -c '%a:%u' -- "$IMAGE_PREPULL_RECEIPT_DIRECTORY")" != "700:$EUID" ]]; then
    echo "candidate rollout requires an absolute executable IMAGE_PREPULL_BIN and an owned private 0700 receipt directory" >&2
    return 2
  fi
  for variable in IMAGE_PREPULL_PREPARE_TIMEOUT IMAGE_PREPULL_VERIFY_TIMEOUT IMAGE_PREPULL_CLEANUP_TIMEOUT; do
    operation_is_positive_go_duration "${!variable}" || {
      echo "${variable} must be a positive bounded Go duration" >&2
      return 2
    }
    seconds="$(image_prepull_seconds "${!variable}")"
    if (( seconds > 3600 )) || [[ "$variable" == IMAGE_PREPULL_CLEANUP_TIMEOUT && "$seconds" -gt 300 ]]; then
      echo "${variable} exceeds the helper phase limit" >&2
      return 2
    fi
  done
  image_prepull_release_args=(--image="$TARGET_IMAGE" --index-file="$IMAGE_PREPULL_INDEX_FILE"
    --amd64-digest="$IMAGE_PREPULL_AMD64_DIGEST" --arm64-digest="$IMAGE_PREPULL_ARM64_DIGEST")
  # No Kubernetes calls, receipt creation or credential mounts in this mode.
  local release approved supplied
  release="$("$TIMEOUT_BIN" --signal=TERM --kill-after=1s 10s "$IMAGE_PREPULL_BIN" \
    --mode=verify-release "${image_prepull_release_args[@]}" | head -c 65537)" || {
    echo "candidate image release identity verification failed" >&2
    return 1
  }
  (( ${#release} <= 65536 )) || { echo "candidate release evidence is oversized" >&2; return 1; }
  approved="$(jq -er --arg image "$TARGET_IMAGE" '
    select(.image == $image) | .runtimeDigests |
    select(keys == ["linux/amd64", "linux/arm64"]) |
    [.[][]] | select(length > 0 and all(.[]; type == "string" and test("^sha256:[a-f0-9]{64}$"))) |
    unique | join(",")
  ' <<<"$release")" || { echo "candidate release evidence is malformed" >&2; return 1; }
  supplied="$(jq -nr --arg digests "$TARGET_RUNTIME_DIGESTS" '$digests | split(",") | sort | join(",")')" || return 1
  if [[ "$supplied" != "$approved" ]]; then
    echo "TARGET_RUNTIME_DIGESTS differs from the verified index/platform digest union" >&2
    return 2
  fi
  image_prepull_budget || return
}

image_prepull_seconds() {
  case "$1" in
    *ms) echo $(( (${1%ms} + 999) / 1000 )) ;;
    *s) echo "${1%s}" ;;
    *m) echo $(( ${1%m} * 60 )) ;;
  esac
}

image_prepull_budget_add() {
  local count="$1" duration="$2" seconds
  operation_is_positive_int64 "$count" && operation_is_positive_go_duration "$duration" || return 2
  # Round up each bounded call, including its GNU timeout kill grace. Reject
  # before multiplying so large otherwise-valid controls cannot wrap.
  seconds=$(( $(image_prepull_seconds "$duration") + 1 ))
  if (( seconds > 86400 || count > (86400 - image_prepull_remaining_seconds) / seconds )); then
    echo "candidate rollout/rollback/cleanup budget exceeds the 24-hour holder limit" >&2
    return 2
  fi
  image_prepull_remaining_seconds=$((image_prepull_remaining_seconds + count * seconds))
}

image_prepull_budget() {
  # Conservative bounds from the current runner, from preparation completion:
  # probe completion + three fixture passes; rollout + rollback; at most 16
  # UID deletion loops (each loop may overrun by one bounded UID-delete call);
  # remaining bounded mutation/evidence calls, including replica inventories.
  # This assumes local shell/jq scheduling progresses; runtime proof is still
  # verified immediately before mutation and the probe is checked throughout.
  image_prepull_remaining_seconds=60
  image_prepull_budget_add 4 "$PROBE_COMPLETE_TIMEOUT" || return
  image_prepull_budget_add 2 "$KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT" || return
  image_prepull_budget_add 16 "$PROBE_DELETE_TIMEOUT" || return
  image_prepull_budget_add 16 "$UID_DELETE_COMMAND_TIMEOUT" || return
  image_prepull_budget_add 64 "$KUBECTL_MUTATION_COMMAND_TIMEOUT" || return
  image_prepull_budget_add 192 "$KUBECTL_EVIDENCE_COMMAND_TIMEOUT" || return
  # Avoid EXPECTED_REPLICAS*8 overflow in the multiplier itself.
  local i
  for ((i = 0; i < 8; i++)); do
    image_prepull_budget_add "$EXPECTED_REPLICAS" "$KUBECTL_EVIDENCE_COMMAND_TIMEOUT" || return
  done
  image_prepull_budget_add 1 "$KUBECTL_READY_WAIT_COMMAND_TIMEOUT" || return
  image_prepull_budget_add 1 "$PROBE_START_TIMEOUT" || return
  image_prepull_budget_add 1 "$IMAGE_PREPULL_VERIFY_TIMEOUT" || return
  image_prepull_budget_add 1 "$IMAGE_PREPULL_CLEANUP_TIMEOUT" || return
  # One source GET after fresh helper verification binds the business patch.
  image_prepull_budget_add 1 "$KUBECTL_EVIDENCE_COMMAND_TIMEOUT" || return
  image_prepull_hold_seconds=$((image_prepull_remaining_seconds + $(image_prepull_seconds "$IMAGE_PREPULL_PREPARE_TIMEOUT") + 60))
  if (( image_prepull_hold_seconds > 86400 )); then
    echo "preparation plus rollout recovery budget exceeds the 24-hour holder limit" >&2
    return 2
  fi
}

image_prepull_prepare() {
  [[ -n "$TARGET_IMAGE" ]] || return 0
  local source_uid="${statefulset_uid:?runner must bind the source StatefulSet UID}"
  # Atomic private subdirectory creation means this run never cleans a receipt
  # owned by a previous attempt, including after an ambiguous prepare failure.
  image_prepull_receipt_directory="$(mktemp -d "$IMAGE_PREPULL_RECEIPT_DIRECTORY/prepull.XXXXXXXXXXXX")" || return 1
  image_prepull_scope_args=(--kubeconfig="$KUBECONFIG" --context="$KUBECTL_CONTEXT"
    --namespace="$KUBEBRAIN_NAMESPACE" --namespace-uid="$IMAGE_PREPULL_NAMESPACE_UID"
    --statefulset="$KUBEBRAIN_STATEFULSET" --statefulset-uid="$source_uid"
    --receipt-directory="$image_prepull_receipt_directory" --receipt-name=attempt
    --client-service="$KUBEBRAIN_CLIENT_SERVICE" --prepare-timeout="$IMAGE_PREPULL_PREPARE_TIMEOUT"
    --verify-timeout="$IMAGE_PREPULL_VERIFY_TIMEOUT" --cleanup-timeout="$IMAGE_PREPULL_CLEANUP_TIMEOUT"
    --minimum-remaining-hold="${image_prepull_remaining_seconds}s" --hold-seconds="$image_prepull_hold_seconds")
  echo "IMAGE_PREPULL_RECEIPT directory=${image_prepull_receipt_directory} name=attempt namespace=${KUBEBRAIN_NAMESPACE} namespace_uid=${IMAGE_PREPULL_NAMESPACE_UID} statefulset=${KUBEBRAIN_STATEFULSET} statefulset_uid=${source_uid}" || return 1
  image_prepull_started=true
  local timeout_seconds result
  timeout_seconds=$(( $(image_prepull_seconds "$IMAGE_PREPULL_PREPARE_TIMEOUT") + $(image_prepull_seconds "$IMAGE_PREPULL_CLEANUP_TIMEOUT") + 5 ))
  result="$("$TIMEOUT_BIN" --signal=TERM --kill-after=1s "${timeout_seconds}s" "$IMAGE_PREPULL_BIN" \
    --mode=prepare --confirm-create-isolated-jobs "${image_prepull_scope_args[@]}" "${image_prepull_release_args[@]}" | head -c 65537)" || return 1
  [[ "$result" == PREPULL_READY ]] || { echo "image preparation did not return its exact success marker" >&2; return 1; }
  echo "$result"
}

image_prepull_verify() {
  [[ -n "$TARGET_IMAGE" ]] || return 0
  local timeout_seconds result current
  local source_uid="${statefulset_uid:?runner must bind the source StatefulSet UID}"
  local source_spec="${initial_spec:?runner must bind the source StatefulSet spec}"
  local source_revision="${current_revision:?runner must bind the source revision}"
  local evidence_dir="${runtime_evidence_dir:?runner must allocate its evidence directory}"
  timeout_seconds=$(( $(image_prepull_seconds "$IMAGE_PREPULL_VERIFY_TIMEOUT") + 5 ))
  result="$("$TIMEOUT_BIN" --signal=TERM --kill-after=1s "${timeout_seconds}s" "$IMAGE_PREPULL_BIN" \
    --mode=verify "${image_prepull_scope_args[@]}" "${image_prepull_release_args[@]}" | head -c 65537)" || return 1
  [[ "$result" == PREPULL_VERIFIED ]] || { echo "image verification did not return its exact success marker" >&2; return 1; }
  current="$evidence_dir/statefulset-prepull-verified.json"
  capture_runtime_evidence "$current" kctl_evidence get statefulset "$KUBEBRAIN_STATEFULSET" -o json || return 1
  # Status-only RV movement is allowed after a long prepare. Refresh it only
  # after confirming the exact source spec/UID/revision is still unchanged.
  # This result is consumed by the sourcing runner's fenced patch.
  # shellcheck disable=SC2034
  IMAGE_PREPULL_SOURCE_RESOURCE_VERSION="$(jq -er --arg uid "$source_uid" --argjson spec "$source_spec" \
    --arg revision "$source_revision" --argjson replicas "$EXPECTED_REPLICAS" '
    select(.metadata.uid == $uid and .metadata.deletionTimestamp == null and .spec == $spec and
      .status.currentRevision == $revision and .status.updateRevision == $revision and .status.readyReplicas == $replicas) |
    .metadata.resourceVersion | select(type == "string" and length > 0)
  ' "$current")" || { echo "source StatefulSet drifted after image preparation" >&2; return 1; }
  echo "$result"
}

image_prepull_cleanup() {
  [[ "$image_prepull_started" == true && "$image_prepull_cleaned" != true ]] || return 0
  local timeout_seconds result
  timeout_seconds=$(( $(image_prepull_seconds "$IMAGE_PREPULL_CLEANUP_TIMEOUT") + 5 ))
  result="$("$TIMEOUT_BIN" --signal=TERM --kill-after=1s "${timeout_seconds}s" "$IMAGE_PREPULL_BIN" \
    --mode=recover-cleanup "${image_prepull_scope_args[@]}" | head -c 65537)" || return 1
  [[ "$result" == PREPULL_CLEANUP_CONFIRMED ]] || { echo "image cleanup did not return its exact success marker" >&2; return 1; }
  image_prepull_cleaned=true
  echo "$result"
  # Receipts deliberately survive cleanup for audit and crash recovery. Never
  # remove the operator's private directory or another attempt's records.
}
