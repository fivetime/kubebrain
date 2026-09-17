#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat >&2 <<'EOF'
Usage:
  hack/production/test-shard.sh --list TOTAL
  hack/production/test-shard.sh --verify TOTAL
  hack/production/test-shard.sh INDEX TOTAL

INDEX is zero-based. Test names are assigned by SHA-256 modulo TOTAL, so every
top-level Test* is selected by exactly one shard without a maintained allowlist.
EOF
  exit 2
}

[[ $# == 2 ]] || usage
mode=run
case "$1" in
  --list|--verify) mode="${1#--}"; total="$2" ;;
  *) index="$1"; total="$2" ;;
esac
[[ "$total" =~ ^[1-9][0-9]*$ && "$total" -le 32 ]] || usage
if [[ "$mode" == run ]]; then
  [[ "$index" =~ ^[0-9]+$ && "$index" -lt "$total" ]] || usage
fi

# Process substitution would hide a failed discovery pipeline from mapfile.
# Accept the inventory only after every discovery stage has exited successfully.
inventory="$(
  cd "$ROOT_DIR" || exit 1
  go test ./hack/production -list '^Test' | awk '/^Test[A-Za-z0-9_]+$/ {print}' | LC_ALL=C sort -u
)" || { echo "production test discovery failed" >&2; exit 1; }
[[ -n "$inventory" ]] || { echo "no production tests discovered" >&2; exit 1; }
mapfile -t tests <<<"$inventory"

bucket_for() {
  local digest
  digest="$(printf '%s' "$1" | sha256sum | cut -c1-8)"
  printf '%d' "$((0x${digest} % total))"
}

if [[ "$mode" == list ]]; then
  for test_name in "${tests[@]}"; do
    printf '%s\t%s\n' "$(bucket_for "$test_name")" "$test_name"
  done
  exit 0
fi

if [[ "$mode" == verify ]]; then
  declare -a counts=()
  for ((shard = 0; shard < total; shard++)); do counts[shard]=0; done
  for test_name in "${tests[@]}"; do
    shard="$(bucket_for "$test_name")"
    counts[shard]=$((counts[shard] + 1))
  done
  for ((shard = 0; shard < total; shard++)); do
    [[ "${counts[shard]}" -gt 0 ]] || { echo "production test shard ${shard} is empty" >&2; exit 1; }
  done
  printf 'verified %d production tests across %d non-empty shards:' "${#tests[@]}" "$total"
  printf ' %d' "${counts[@]}"
  printf '\n'
  exit 0
fi

selected=()
for test_name in "${tests[@]}"; do
  [[ "$(bucket_for "$test_name")" == "$index" ]] && selected+=("$test_name")
done
[[ ${#selected[@]} -gt 0 ]] || { echo "production test shard ${index}/${total} is empty" >&2; exit 1; }
regex="$(IFS='|'; printf '%s' "${selected[*]}")"
printf 'running production test shard %d/%d with %d tests\n' "$index" "$total" "${#selected[@]}"
cd "$ROOT_DIR"
exec go test ./hack/production -run "^(${regex})$" -count=1 -timeout=15m
