#!/usr/bin/env bash
set -euo pipefail

WORKER_ID="${WORKER_ID:-}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"; EXPECTED_DIGEST="${EXPECTED_DIGEST:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; INPUT_ROOT="${INPUT_ROOT:-$WORK_DIR/inputs}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"; PROVISION="${PROVISION:-/opt/kubebrain/hack/production/provision-native-pitr-replacement-target.sh}"
die(){ echo "$*" >&2; exit 1; }; sha(){ sha256sum "$1"|cut -d ' ' -f1; }; runctl(){ "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
[[ -n "$WORKER_ID" ]]||die "WORKER_ID is required"
claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRTargetProvisioning --lease "${LEASE_SECONDS}s")"
name="$($JQ -er .name<<<"$claim")"; operation_id="$($JQ -er .operation_id<<<"$claim")"; instance="$($JQ -er .instance<<<"$claim")"; type="$($JQ -er .type<<<"$claim")"; requester="$($JQ -er .requested_by<<<"$claim")"; attempt="$($JQ -er .attempt<<<"$claim")"; expected_sha="$($JQ -er .parameters_sha256<<<"$claim")"
[[ "$name" == "$operation_id" && "$instance" == kubebrain && "$type" == NativePITRTargetProvisioning && "$requester" == platform:native-pitr-target-provisioning && "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]]||die "claimed target provisioning operation identity is invalid"
[[ -n "$EXPECTED_DIGEST" ]]&&[[ "$EXPECTED_DIGEST" == "$expected_sha" ]]||EXPECTED_DIGEST="$expected_sha"
capture="$(mktemp -d)"; trap 'rm -rf -- "$capture"' EXIT
if [[ -z "$PARAMETERS_INPUT" ]];then PARAMETERS_INPUT="$capture/input.json";runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt">"$PARAMETERS_INPUT";fi
[[ -f "$PARAMETERS_INPUT" && "$(sha "$PARAMETERS_INPUT")" == "$EXPECTED_DIGEST" ]]||die "target provisioning parameters digest mismatch"
params="$capture/parameters.json";cp -- "$PARAMETERS_INPUT" "$params";chmod 600 "$params"
"$JQ" -e --arg root "$INPUT_ROOT" '
 keys==["manifest","manifest_sha256","old_target_provisioning","old_target_provisioning_sha256","retirement","retirement_sha256"] and
 ([.manifest,.old_target_provisioning,.retirement]|all(.[];type=="string" and startswith($root+"/") and length<=4096 and (contains("/../")|not) and (endswith("/..")|not))) and
 ([.manifest_sha256,.old_target_provisioning_sha256,.retirement_sha256]|all(.[];type=="string" and test("^[a-f0-9]{64}$")))' "$params">/dev/null||die "target provisioning parameter schema is invalid"
for key in manifest old_target_provisioning retirement;do path="$($JQ -r ".$key" "$params")";[[ -f "$path" && "$(sha "$path")" == "$($JQ -r ".${key}_sha256" "$params")" ]]||die "$key digest mismatch";done
authorization="$WORK_DIR/${name}.native-pitr-target-provision-authorization.json";dry_run="$WORK_DIR/${name}.native-pitr-target-provision-dry-run.json";creation="$WORK_DIR/${name}.native-pitr-target-provision-creation.json";provisioning="$WORK_DIR/${name}.native-pitr-target-provisioning.json";writers="$WORK_DIR/${name}.native-pitr-writer-exclusion.json";target="$WORK_DIR/${name}.native-pitr-target-empty.json";receipt="$WORK_DIR/${name}.native-pitr-target-qualification.json"
runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s">/dev/null||die "target provisioning ownership was fenced"
RETIREMENT="$($JQ -r .retirement "$params")" OLD_PROVISIONING="$($JQ -r .old_target_provisioning "$params")" MANIFEST="$($JQ -r .manifest "$params")" AUTHORIZATION_ID="$name" AUTHORIZATION_OUTPUT="$authorization" DRY_RUN_OUTPUT="$dry_run" CREATION_OUTPUT="$creation" PROVISIONING_OUTPUT="$provisioning" WRITER_EXCLUSION_OUTPUT="$writers" TARGET_EMPTY_OUTPUT="$target" QUALIFICATION_OUTPUT="$receipt" TLS_DIR="$TLS_DIR" KUBE_CONTEXT=in-cluster "$PROVISION"
runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s">/dev/null||die "target provisioning ownership was fenced"
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$(sha "$receipt")" --message "provisioned and transactionally qualified empty replacement native PITR target">/dev/null
