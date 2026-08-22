#!/usr/bin/env bash
set -euo pipefail

PARAMETERS_FILE="${PARAMETERS_FILE:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
KUBECTL="${KUBECTL:-kubectl}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_EXISTING_SECRET_RESPONSE_BYTES=87396

die() { echo "$*" >&2; exit 1; }
resolve_executable() {
  local value="$1"
  if [[ "$value" == */* ]]; then [[ -x "$value" ]] || return 1; printf '%s' "$value"; else command -v "$value"; fi
}
[[ -f "$PARAMETERS_FILE" ]] || die "PARAMETERS_FILE is required"
command -v stat >/dev/null || die "stat is required"
parameters_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ ]] && ((size <= MAX_OPERATION_PARAMETERS_BYTES)); }
existing_secret_response_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ ]] && ((size <= MAX_EXISTING_SECRET_RESPONSE_BYTES)); }
parameters_size_is_valid "$PARAMETERS_FILE" || die "native PITR parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required (use in-cluster for service-account credentials)"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations ]] || die "native PITR requests are restricted to kubebrain-operations"
KUBECTL="$(resolve_executable "$KUBECTL")" || die "KUBECTL must be executable"
OPERATIONCTL="$(resolve_executable "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"
JQ="$(resolve_executable "$JQ")" || die "JQ must be executable"

temp_dir="$(mktemp -d)"; trap 'rm -rf -- "$temp_dir"' EXIT
parameters_file="$temp_dir/parameters.json"
frozen_parameters="$temp_dir/input.json"
cp -- "$PARAMETERS_FILE" "$frozen_parameters"; chmod 600 "$frozen_parameters"
parameters_size_is_valid "$frozen_parameters" && parameters_size_is_valid "$PARAMETERS_FILE" || die "native PITR parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"
"$JQ" -ceS '
  select(keys == ["backup_ts","pd_addrs","storage_prefix"] or
    keys == ["backup_ts","cipher_method","encryption_key_id","pd_addrs","storage_prefix"]) |
  select(.backup_ts | type == "string" and test("^[1-9][0-9]*$") and
    (length < 20 or (length == 20 and . <= "18446744073709551615"))) |
  select(.storage_prefix | type == "string" and
    test("^s3://[^/@[:space:]]+/") and
    (sub("^s3://[^/]+/"; "") | gsub("/"; "") | length > 0) and index("\u0000") == null and
    (contains("\r")|not) and (contains("\n")|not) and
    (contains("?")|not) and (contains("#")|not)) |
  select(.pd_addrs | type == "array" and length > 0 and length <= 32 and
    all(.[]; type == "string" and
      test("^(\\[[^],[:space:]]+\\]|[^\\[\\]:,[:space:]]+):[1-9][0-9]{0,4}$") and
      (capture(":(?<port>[0-9]+)$").port | tonumber) <= 65535)) |
  select((.pd_addrs | unique | length) == (.pd_addrs | length)) |
  select((has("cipher_method")|not) or
    (.cipher_method == "aes256-ctr" and
      (.encryption_key_id | type == "string" and test("^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$")))) |
  .pd_addrs |= sort
' "$frozen_parameters" >"$parameters_file" || die "native PITR parameter schema is invalid"
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"
[[ "$parameters_sha" =~ ^[a-f0-9]{64}$ ]] || die "cannot calculate canonical parameter digest"
operation_name="native-pitr-full-${parameters_sha:0:20}"
secret_name="${operation_name}-parameters"
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }

existing_secret_file="$temp_dir/existing-secret.response"
if kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.type}{"\t"}{.data.parameters\.json}' >"$existing_secret_file" 2>/dev/null; then
  chmod 600 "$existing_secret_file"
  existing_secret_response_is_valid "$existing_secret_file" || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_secret_file")"
  IFS=$'\t' read -r immutable secret_type encoded <<<"$existing"
  [[ "$immutable" == true && "$secret_type" == Opaque ]] || die "existing native PITR parameter Secret is not immutable Opaque"
  existing_sha="$(printf '%s' "$encoded" | base64 -d | sha256sum | cut -d ' ' -f1)"
  [[ "$existing_sha" == "$parameters_sha" ]] || die "existing native PITR parameter Secret drifted"
else
  kctl -n "$OPERATION_NAMESPACE" create secret generic "$secret_name" \
    --from-file="parameters.json=$parameters_file" --dry-run=client -o json |
    "$JQ" '.immutable=true' |
    kctl -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi

ctl_args=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl_args+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl_args[@]}" --action submit --name "$operation_name" --operation-id "$operation_name" \
  --requested-by platform:native-pitr-full-backup --instance kubebrain --type NativePITRFullBackup \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret_name" \
  --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified Pending NativePITRFullBackup ${OPERATION_NAMESPACE}/${operation_name}"
