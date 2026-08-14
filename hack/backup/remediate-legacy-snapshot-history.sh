#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: hack/backup/remediate-legacy-snapshot-history.sh

Diagnoses the upgrade-era retained-history condition that makes Maintenance
Snapshot fail because an old raw/v1 value did not persist its per-version lease.
The default action is read-only. ACTION=compact irreversibly removes only the
MVCC history through the minimum safe revision reported by Snapshot, then
creates and validates a snapshot.

Environment:
  ACTION              diagnose (default) or compact
  ENDPOINT            exactly one etcd endpoint; required
  OUTPUT              snapshot output path; required for ACTION=compact
  ETCDCTL_BIN          etcdctl binary, default etcdctl
  ETCDUTL_BIN          etcdutl binary, default etcdutl
  ETCDCTL_CACERT       optional CA certificate
  ETCDCTL_CERT         optional client certificate
  ETCDCTL_KEY          optional client private key
  ETCDCTL_USER         optional user[:password]

ACTION=compact also requires all of:
  ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true
  CONFIRM_ENDPOINT     exact byte-for-byte match with ENDPOINT
  EXPECTED_CLUSTER_ID  cluster ID printed by a prior diagnose run
  EXPECTED_REVISION    current revision printed by a prior diagnose run

Compaction permanently makes reads/watches at old revisions unavailable. It is
not a repair for corruption and is refused unless the preflight Snapshot fails
with KubeBrain's precise legacy-lease diagnostic.
EOF
}

case "${1:-}" in
  -h|--help) usage; exit 0 ;;
  "") ;;
  *) usage >&2; exit 2 ;;
esac

ACTION="${ACTION:-diagnose}"
ENDPOINT="${ENDPOINT:-}"
OUTPUT="${OUTPUT:-}"
ETCDCTL_BIN="${ETCDCTL_BIN:-etcdctl}"
ETCDUTL_BIN="${ETCDUTL_BIN:-etcdutl}"

case "$ACTION" in
  diagnose|compact) ;;
  *) echo "ACTION must be diagnose or compact, got $ACTION" >&2; exit 2 ;;
esac
if [[ -z "$ENDPOINT" ]]; then
  echo "ENDPOINT is required and must identify exactly one KubeBrain endpoint" >&2
  exit 2
fi
if [[ "$ENDPOINT" == *,* || "$ENDPOINT" == *[[:space:]]* || "$ENDPOINT" == *[[:cntrl:]]* ||
  "$ENDPOINT" == *\"* || "$ENDPOINT" == *\\* ]]; then
  echo "ENDPOINT must contain exactly one endpoint and no unsupported characters" >&2
  exit 2
fi
required_tools=("$ETCDCTL_BIN")
if [[ "$ACTION" == compact ]]; then
  required_tools+=("$ETCDUTL_BIN")
fi
for tool in "${required_tools[@]}"; do
  if [[ "$tool" == */* ]]; then
    [[ -x "$tool" ]] || { echo "required tool is not executable: $tool" >&2; exit 1; }
  elif ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required command: $tool" >&2
    exit 1
  fi
done
status_fields="$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" endpoint status --write-out=fields)"
# The fields printer emits uint64 IDs with fmt's exact decimal representation.
# Parsing JSON through jq would round cluster IDs above IEEE-754's exact range.
cluster_id="$(awk '$1 == "\"ClusterID\"" && $2 == ":" { print $3 }' <<<"$status_fields")"
revision="$(awk '$1 == "\"Revision\"" && $2 == ":" { print $3 }' <<<"$status_fields")"
if [[ "$cluster_id" == *$'\n'* || "$revision" == *$'\n'* ||
  ! "$cluster_id" =~ ^[1-9][0-9]*$ || ! "$revision" =~ ^[1-9][0-9]*$ ]]; then
  echo "endpoint status did not return exactly one positive cluster ID and revision" >&2
  exit 1
fi
printf 'cluster_id=%s\nrevision=%s\nendpoint=%s\n' "$cluster_id" "$revision" "$ENDPOINT"

probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-legacy-snapshot-preflight.XXXXXX")"
probe_snapshot="$probe_dir/preflight.db"
probe_log="$probe_dir/preflight.log"
cleanup() { find "$probe_dir" -depth -delete; }
trap cleanup EXIT

if "$ETCDCTL_BIN" --endpoints="$ENDPOINT" snapshot save "$probe_snapshot" >"$probe_log" 2>&1; then
  if [[ "$ACTION" == compact ]]; then
    echo "refusing compaction: Maintenance Snapshot already succeeds; no legacy-history remediation is needed" >&2
    exit 1
  fi
  echo "snapshot_status=healthy"
  exit 0
fi
if ! grep -Fq "snapshot cannot determine lease for retained legacy version" "$probe_log"; then
  cat "$probe_log" >&2
  echo "snapshot_status=other_failure" >&2
  echo "refusing legacy-history remediation: Snapshot failed for a different reason" >&2
  exit 1
fi
echo "snapshot_status=legacy_lease_history_ambiguous"
probe_diagnostic="$(<"$probe_log")"
if [[ ! "$probe_diagnostic" =~ minimum\ physical\ compact\ revision\ ([1-9][0-9]*) ]]; then
  cat "$probe_log" >&2
  echo "refusing legacy-history remediation: Snapshot diagnostic has no safe compact revision" >&2
  exit 1
fi
compact_revision="${BASH_REMATCH[1]}"
if (( compact_revision > revision )); then
  echo "refusing legacy-history remediation: Snapshot returned an invalid compact revision" >&2
  exit 1
fi
echo "minimum_compact_revision=$compact_revision"

if [[ "$ACTION" == diagnose ]]; then
  echo "No data was changed. To discard history through revision $compact_revision, review the warning and rerun with ACTION=compact and all confirmation fields."
  exit 3
fi

if [[ "${ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION:-false}" != true ]]; then
  echo "refusing irreversible compaction: set ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true" >&2
  exit 2
fi
if [[ "${CONFIRM_ENDPOINT:-}" != "$ENDPOINT" ]]; then
  echo "refusing irreversible compaction: CONFIRM_ENDPOINT must exactly match ENDPOINT" >&2
  exit 2
fi
if [[ "${EXPECTED_CLUSTER_ID:-}" != "$cluster_id" ]]; then
  echo "refusing irreversible compaction: EXPECTED_CLUSTER_ID does not match current cluster" >&2
  exit 2
fi
if [[ "${EXPECTED_REVISION:-}" != "$revision" ]]; then
  echo "refusing irreversible compaction: EXPECTED_REVISION does not match current revision" >&2
  exit 2
fi
if [[ -z "$OUTPUT" ]]; then
  echo "OUTPUT is required for ACTION=compact" >&2
  exit 2
fi
if [[ -e "$OUTPUT" ]]; then
  echo "refusing to overwrite existing OUTPUT: $OUTPUT" >&2
  exit 2
fi
output_dir="$(dirname "$OUTPUT")"
if [[ ! -d "$output_dir" ]]; then
  echo "OUTPUT parent directory does not exist: $output_dir" >&2
  exit 2
fi

# Physical=true is essential: advancing only the logical watermark leaves the
# ambiguous raw/v1 row present until asynchronous GC and cannot unblock Snapshot.
"$ETCDCTL_BIN" --endpoints="$ENDPOINT" compact "$compact_revision" --physical

candidate="$(mktemp "$output_dir/.kubebrain-post-remediation.XXXXXX.db")"
candidate_published=false
cleanup_candidate() { [[ "$candidate_published" == true ]] || rm -f "$candidate"; }
trap 'cleanup_candidate; cleanup' EXIT
rm -f "$candidate"
"$ETCDCTL_BIN" --endpoints="$ENDPOINT" snapshot save "$candidate"
"$ETCDUTL_BIN" snapshot status "$candidate" --write-out=json >/dev/null
# link(2) is an atomic no-clobber publication in the already-validated output
# directory. Unlike mv, it cannot overwrite a path created after the -e check.
if ! ln "$candidate" "$OUTPUT"; then
  echo "refusing to overwrite OUTPUT created during remediation: $OUTPUT" >&2
  exit 1
fi
rm -f "$candidate"
candidate_published=true
echo "snapshot_status=remediated"
echo "compacted_revision=$compact_revision"
echo "snapshot_output=$OUTPUT"
