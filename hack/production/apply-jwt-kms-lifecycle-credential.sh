#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="$ROOT_DIR/deploy/production/kubebrain-jwt-kms-lifecycle-credential.yaml"
NAMESPACE=kubebrain-kms-lifecycle
SERVICE_ACCOUNT=kubebrain-jwt-kms-lifecycle
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
OPENSSL="${OPENSSL:-openssl}"

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf %s "$1"; else command -v "$1"; fi; }
kc() { "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; }

verify_static() {
  [[ -f "$MANIFEST" && ! -L "$MANIFEST" ]] || die "JWT KMS lifecycle credential manifest is missing or a symlink"
  grep -Fq 'name: kubebrain-kms-lifecycle' "$MANIFEST" || die "lifecycle credential namespace drifted"
  grep -Fq 'automountServiceAccountToken: false' "$MANIFEST" || die "lifecycle identity must not mount a Kubernetes token"
  grep -Fq 'object.immutable == true' "$MANIFEST" || die "lifecycle credential immutability policy drifted"
  grep -Fq 'validationActions: [Deny]' "$MANIFEST" || die "lifecycle credential admission binding must be Deny"
}

usage() { echo "Usage: $0 --verify | --apply | --provision | --check | --activate" >&2; exit 2; }
[[ "$#" == 1 ]] || usage
mode="$1"
case "$mode" in --verify) verify_static; echo "verified isolated immutable JWT KMS lifecycle credential foundation"; exit 0;; --apply|--provision|--check|--activate) ;; *) usage;; esac
verify_static
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
OPENSSL="$(resolve "$OPENSSL")" || die "OPENSSL must be executable"

check_policy() {
  local policy binding answer identity
  policy="$(kc get validatingadmissionpolicy kubebrain-jwt-kms-lifecycle-credential -o json)" || die "lifecycle credential policy is missing"
  "$JQ" -e '.status.observedGeneration == .metadata.generation and ((.status.typeChecking.expressionWarnings // []) | length == 0)' <<<"$policy" >/dev/null || die "lifecycle credential policy is not compiled without warnings"
  binding="$(kc get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-credential -o json)" || die "lifecycle credential binding is missing"
  "$JQ" -e '.spec.policyName == "kubebrain-jwt-kms-lifecycle-credential" and .spec.validationActions == ["Deny"]' <<<"$binding" >/dev/null || die "lifecycle credential binding is not exact Deny"
  policy="$(kc get validatingadmissionpolicy kubebrain-jwt-kms-lifecycle-active -o json)" || die "active lifecycle credential policy is missing"
  "$JQ" -e '.status.observedGeneration == .metadata.generation and ((.status.typeChecking.expressionWarnings // []) | length == 0)' <<<"$policy" >/dev/null || die "active lifecycle credential policy is not compiled without warnings"
  binding="$(kc get validatingadmissionpolicybinding kubebrain-jwt-kms-lifecycle-active -o json)" || die "active lifecycle credential binding is missing"
  "$JQ" -e '.spec.policyName == "kubebrain-jwt-kms-lifecycle-active" and .spec.validationActions == ["Deny"]' <<<"$binding" >/dev/null || die "active lifecycle credential binding is not exact Deny"
  for identity in \
    system:serviceaccount:kubebrain-kms-lifecycle:kubebrain-jwt-kms-lifecycle \
    system:serviceaccount:kubebrain-operations:kubebrain-jwt-key-rotation-requester \
    system:serviceaccount:kubebrain-operations:kubebrain-operation-worker; do
    for verb in get list watch create update patch delete deletecollection; do
      answer="$(kc auth can-i "$verb" secrets -n "$NAMESPACE" --as="$identity")" || die "cannot evaluate lifecycle credential RBAC"
      [[ "$answer" == no ]] || die "$identity must be denied $verb lifecycle credential Secrets"
    done
  done
}

if [[ "$mode" == --apply ]]; then
  kc apply -f "$MANIFEST" >/dev/null
  check_policy
  echo "applied isolated JWT KMS lifecycle credential foundation"
  exit 0
fi

check_policy
credential_id="${CREDENTIAL_ID:-}"
[[ "$credential_id" =~ ^[a-z0-9]([-a-z0-9]{0,38}[a-z0-9])?$ ]] || die "CREDENTIAL_ID must be a 1..40 character lowercase DNS label"
secret="kubebrain-jwt-kms-lifecycle-${credential_id}"

check_secret() {
  local object canonical
  object="$(kc get secret "$secret" -n "$NAMESPACE" -o json)" || die "lifecycle credential Secret is missing"
  "$JQ" -e --arg name "$secret" '
    .metadata.name == $name and .metadata.namespace == "kubebrain-kms-lifecycle" and
    .metadata.labels["dbaas.kubebrain.io/credential-kind"] == "jwt-kms-lifecycle" and
    .immutable == true and .type == "Opaque" and
    (.data | keys | sort) == ["ca.crt","endpoint","lifecycle-token","receipt-public-key.pem"] and
    all(.data[]; (@base64d | length) > 0)
  ' <<<"$object" >/dev/null || die "lifecycle credential Secret shape drifted"
  credential_secret_uid="$("$JQ" -er '.metadata.uid|select(type=="string" and test("^[^[:space:][:cntrl:]]{1,128}$"))' <<<"$object")" || die "lifecycle credential Secret UID is invalid"
  credential_secret_rv="$("$JQ" -er '.metadata.resourceVersion|select(type=="string" and test("^[^[:space:][:cntrl:]]{1,128}$"))' <<<"$object")" || die "lifecycle credential Secret resourceVersion is invalid"
  canonical="$("$JQ" -cS '.data' <<<"$object")" || die "cannot canonicalize lifecycle credential Secret data"
  credential_secret_sha="$(printf '%s' "$canonical" | sha256sum | cut -d ' ' -f1)"
  credential_secret_data="$("$JQ" -cS '.data' <<<"$object")"
}

if [[ "$mode" == --check ]]; then check_secret; echo "checked isolated immutable lifecycle credential $secret"; exit 0; fi

endpoint="${KMS_PROVIDER_ENDPOINT:-}"
token="${KMS_LIFECYCLE_TOKEN_FILE:-}"
ca="${KMS_PROVIDER_CA_FILE:-}"
public_key="${KMS_RECEIPT_PUBLIC_KEY:-}"
[[ "$endpoint" =~ ^https://[^/[:space:]?#]+(:[0-9]{1,5})?$ ]] || die "KMS_PROVIDER_ENDPOINT must be an HTTPS origin"
for file in "$token" "$ca" "$public_key"; do
  [[ "$file" == /* && -f "$file" && ! -L "$file" && "$(realpath -e -- "$file")" == "$file" ]] || die "credential inputs must be canonical absolute regular non-symlink files"
  read -r owner mode_value links bytes < <(stat -Lc '%u %a %h %s' "$file")
  [[ "$owner" == "$(id -u)" && "$mode_value" == 600 && "$links" == 1 && "$bytes" -ge 1 && "$bytes" -le 1048576 ]] || die "credential inputs must be current-user mode 0600, single-link, and 1..1048576 bytes"
done
[[ "$(wc -c <"$token")" -le 16384 && "$(tr -d '\n' <"$token" | wc -c)" -ge 1 ]] || die "lifecycle token size is invalid"
[[ "$(tr -d '\n' <"$token")" != *[[:space:]]* && "$(grep -c . "$token")" -eq 1 ]] || die "lifecycle token must be one non-whitespace line"
"$OPENSSL" verify -CAfile "$ca" "$ca" >/dev/null 2>&1 || die "KMS provider CA must be a valid self-verifying PEM trust anchor"
public_der="$("$OPENSSL" pkey -pubin -in "$public_key" -outform DER 2>/dev/null | od -An -tx1 | tr -d ' \n')" || die "KMS receipt public key is invalid"
[[ "$public_der" =~ ^302a300506032b6570032100[a-f0-9]{64}$ ]] || die "KMS receipt public key must be Ed25519"

if [[ "$mode" == --activate ]]; then
  check_secret
  probed_secret_uid="$credential_secret_uid"; probed_secret_rv="$credential_secret_rv"; probed_secret_sha="$credential_secret_sha"
  desired_data="$("$JQ" -cn --arg endpoint "$(printf '%s' "$endpoint" | base64 -w0)" --arg token "$(base64 -w0 <"$token")" --arg ca "$(base64 -w0 <"$ca")" --arg key "$(base64 -w0 <"$public_key")" '{"ca.crt":$ca,"endpoint":$endpoint,"lifecycle-token":$token,"receipt-public-key.pem":$key}')" || die "cannot construct lifecycle credential binding"
  [[ "$desired_data" == "$credential_secret_data" ]] || die "local credential inputs do not match the immutable Secret"
  probe="$(resolve "${KMS_LIFECYCLE_CREDENTIAL_PROBE:-kubebrain-jwt-kms-lifecycle-credential-probe}")" || die "KMS_LIFECYCLE_CREDENTIAL_PROBE must be executable"
  readiness="${READINESS_RECEIPT_OUTPUT:-}"
  [[ "$readiness" == /* && "$(dirname -- "$readiness")" == "$(realpath -e -- "$(dirname -- "$readiness")")" ]] || die "READINESS_RECEIPT_OUTPUT must have a canonical absolute existing parent"
  probe_args=(--endpoint "$endpoint" --token-file "$token" --ca-file "$ca" --public-key "$public_key" --credential-id "$credential_id" --credential-secret-uid "$credential_secret_uid" --credential-secret-data-sha256 "$credential_secret_sha")
  if [[ -e "$readiness" || -L "$readiness" ]]; then
    [[ -f "$readiness" && ! -L "$readiness" && "$(realpath -e -- "$readiness")" == "$readiness" ]] || die "existing readiness receipt is unsafe"
    verify_dir="$(mktemp -d)"; verified="$verify_dir/verified.json"; "$probe" "${probe_args[@]}" --receipt-input "$readiness" --receipt-output "$verified"; rm -rf -- "$verify_dir"
  else
    "$probe" "${probe_args[@]}" --receipt-output "$readiness"
  fi
  check_secret
  [[ "$desired_data" == "$credential_secret_data" && "$credential_secret_uid" == "$probed_secret_uid" && "$credential_secret_rv" == "$probed_secret_rv" && "$credential_secret_sha" == "$probed_secret_sha" ]] || die "lifecycle credential Secret identity or data changed after readiness probe"
  readiness_sha="$(sha256sum "$readiness" | cut -d ' ' -f1)"; [[ "$readiness_sha" =~ ^[a-f0-9]{64}$ ]] || die "readiness receipt digest is invalid"
  readiness_expiry="$("$JQ" -er '.payload|@base64d|fromjson|.expires_at_unix|select(type=="number" and floor==. and .>0)|tostring' "$readiness")" || die "readiness receipt expiry is invalid"
  activated_at="$(date +%s)"; (( readiness_expiry > activated_at )) || die "readiness receipt expired before activation"
  active_data="$("$JQ" -cn --arg id "$credential_id" --arg name "$secret" --arg uid "$credential_secret_uid" --arg rv "$credential_secret_rv" --arg sha "$credential_secret_sha" --arg readiness "$readiness_sha" --arg expiry "$readiness_expiry" --arg activated "$activated_at" '{format:"kubebrain.jwt-kms-lifecycle-active.v1","credential-id":$id,"secret-name":$name,"secret-uid":$uid,"secret-resource-version":$rv,"secret-data-sha256":$sha,"readiness-receipt-sha256":$readiness,"readiness-expires-at-unix":$expiry,"activated-at-unix":$activated}')"
  if current="$(kc get configmap kubebrain-jwt-kms-lifecycle-active -n "$NAMESPACE" -o json 2>/dev/null)"; then
    active_rv="$("$JQ" -er '.metadata.resourceVersion' <<<"$current")"
    patch="$("$JQ" -cn --arg rv "$active_rv" --argjson data "$active_data" '[{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"replace","path":"/data","value":$data}]')"
    kc patch configmap kubebrain-jwt-kms-lifecycle-active -n "$NAMESPACE" --type=json -p "$patch" >/dev/null || die "active credential CAS failed"
  else
    "$JQ" -cn --argjson data "$active_data" '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:"kubebrain-jwt-kms-lifecycle-active",namespace:"kubebrain-kms-lifecycle"},data:$data}' | kc create -f - >/dev/null || die "active credential pointer creation failed"
  fi
  echo "activated lifecycle credential $secret with provider-signed readiness evidence"
  exit 0
fi

if kc get secret "$secret" -n "$NAMESPACE" -o json >/dev/null 2>&1; then
  die "versioned lifecycle credential already exists; use a new CREDENTIAL_ID"
fi
temporary="$(mktemp -d)"; trap 'rm -rf -- "$temporary"' EXIT INT TERM
printf '%s' "$endpoint" >"$temporary/endpoint"; chmod 600 "$temporary/endpoint"
kc create secret generic "$secret" -n "$NAMESPACE" --type=Opaque \
  --from-file="endpoint=$temporary/endpoint" --from-file="lifecycle-token=$token" --from-file="ca.crt=$ca" --from-file="receipt-public-key.pem=$public_key" \
  --dry-run=client -o json | "$JQ" '.immutable=true | .metadata.labels={"app.kubernetes.io/part-of":"kubebrain","dbaas.kubebrain.io/credential-kind":"jwt-kms-lifecycle"}' | kc create -f - >/dev/null
check_secret
echo "provisioned isolated immutable lifecycle credential $secret; provider readiness and activation remain required"
