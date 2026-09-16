#!/usr/bin/env bash

# Sourced by apiserver runners after stopping their owned process. A global
# before/after lease-list delta cannot establish ownership, even if attached
# keys happen to be under our prefix: another client may attach keys later.
# Never revoke such leases. Give the stopped apiserver's leases time to expire
# naturally and fail cleanup if the baseline cannot be reconciled read-only.
verify_apiserver_lease_cleanup() {
  local final_lease_ids deadline=$((SECONDS + 60))
  while true; do
    if ! final_lease_ids="$(list_lease_ids)"; then
      echo "failed to list leases during read-only apiserver cleanup" >&2
      return 1
    fi
    # The sourcing runner captures this baseline before claiming its prefix.
    # shellcheck disable=SC2154
    if [[ "$final_lease_ids" == "$baseline_lease_ids" ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "lease set differs from preflight after cleanup; no lease revoked because ownership is unproven" >&2
      return 1
    fi
    sleep 1
  done
}
