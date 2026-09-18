#!/usr/bin/env bash
# Dedicated-test adjacent trust phases. No Secret creation or volume cleanup.
set -euo pipefail
umask 077
if [[ $# != 8 || $1 != --execute || ( $2 != expand && $2 != restore ) ]]; then
 echo 'Usage: bash run-peer-trust-expand.sh --execute expand|restore RECEIPT SHA256 NEW_OUTPUT_DIR KUBECONFIG CONTEXT VERIFIER' >&2
 exit 2
fi
mode=$2; receipt=$3; digest=$4; out=$5; kubeconfig=$6; context=$7; verifier=$8
[[ $receipt == /* && $out == /* && $kubeconfig == /* && $verifier == /* && -n $context && $digest =~ ^[a-f0-9]{64}$ ]]
[[ -f $receipt && -f $kubeconfig && -f $verifier ]]
[[ $(sha256sum "$receipt" | cut -d ' ' -f 1) == "$digest" ]]
jq -e '.phase == null or .phase == "roots" or .phase == "members"' "$receipt" >/dev/null
trust_phase=$(jq -r '.phase // "roots"' "$receipt")
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# Exclusive output directory is also the execution claim. Never reuse an attempt.
mkdir -m 700 -- "$out"
trap 'printf "%s\n" "$?" > "$out/exit-code"' EXIT
cp -- "$receipt" "$out/receipt.json"
cp -- "$source_dir/peer-trust-expand-plan.jq" "$out/planner.jq"
cp -- "$verifier" "$out/verifier.sh"
cp -- "${BASH_SOURCE[0]}" "$out/driver.sh"
chmod 600 "$out/receipt.json" "$out/planner.jq" "$out/verifier.sh" "$out/driver.sh"
[[ $(sha256sum "$out/receipt.json" | cut -d ' ' -f 1) == "$digest" ]]
sha256sum "$out/receipt.json" "$out/planner.jq" "$out/verifier.sh" "$out/driver.sh" > "$out/inputs.sha256"
namespace=$(jq -er '.baseline.metadata.namespace | select(test("^[a-z0-9][a-z0-9-]*$"))' "$out/receipt.json")
sts=$(jq -er '.baseline.metadata.name | select(test("^[a-z0-9][a-z0-9-]*$"))' "$out/receipt.json")
old_secret=$(jq -er '.original_secret.metadata.name | select(test("^[a-z0-9][a-z0-9.-]*$"))' "$out/receipt.json")
dual_secret=$(jq -er '.expanded_secret.metadata.name | select(test("^[a-z0-9][a-z0-9.-]*$"))' "$out/receipt.json")
if [[ $trust_phase == members ]]; then
 member_secret=$(jq -er '.member_secret.metadata.name | select(test("^[a-z0-9][a-z0-9.-]*$"))' "$out/receipt.json")
fi
k=(kubectl --kubeconfig="$kubeconfig" --context="$context" --request-timeout=15s -n "$namespace")
mutation_attempted=false
capture() {
 local phase=$1
 mkdir -m 700 "$out/$phase"
 timeout --kill-after=2s 20s "${k[@]}" get namespace "$namespace" -o json > "$out/$phase/namespace.json"
 timeout --kill-after=2s 20s "${k[@]}" get sts "$sts" -o json > "$out/$phase/current.json"
 timeout --kill-after=2s 20s "${k[@]}" get secret "$old_secret" -o json > "$out/$phase/original-secret.json"
 timeout --kill-after=2s 20s "${k[@]}" get secret "$dual_secret" -o json > "$out/$phase/expanded-secret.json"
 if [[ $trust_phase == members ]]; then
  timeout --kill-after=2s 20s "${k[@]}" get secret "$member_secret" -o json > "$out/$phase/member-secret.json"
 else
  printf 'null\n' > "$out/$phase/member-secret.json"
 fi
 timeout --kill-after=2s 20s "${k[@]}" get pods -o json > "$out/$phase/pods.json"
 timeout --kill-after=2s 20s "${k[@]}" get pvc -o json > "$out/$phase/pvcs.json"
 timeout --kill-after=2s 20s "${k[@]}" get pv -o json > "$out/$phase/pvs.json"
 jq --arg mode "$mode" --slurpfile ns "$out/$phase/namespace.json" \
  --slurpfile current "$out/$phase/current.json" \
  --slurpfile original "$out/$phase/original-secret.json" \
  --slurpfile expanded "$out/$phase/expanded-secret.json" \
  --slurpfile member "$out/$phase/member-secret.json" \
  '. + {mode:$mode, namespace:$ns[0], current:$current[0], live_original_secret:$original[0], live_expanded_secret:$expanded[0], live_member_secret:$member[0]}' \
  "$out/receipt.json" > "$out/$phase/input.json"
 jq -er -f "$out/planner.jq" "$out/$phase/input.json" > "$out/$phase/patch.json"
}
finish() {
 local rc=$?
 trap - EXIT INT TERM
 printf '%s\n' "$rc" > "$out/exit-code"
 if [[ $rc != 0 && $mode == expand && $mutation_attempted == true ]]; then
  # Never retry the expansion after an uncertain API outcome. Fresh restore
  # planning accepts only the two known specs, and still refuses external drift.
  local restore_rc=0
  # The saved driver resolves the planner alongside itself.
  cp "$out/planner.jq" "$out/peer-trust-expand-plan.jq"
  bash "$out/driver.sh" --execute restore "$out/receipt.json" "$digest" \
   "$out/recovery" "$kubeconfig" "$context" "$out/verifier.sh" \
   > "$out/recovery.log" 2>&1 || restore_rc=$?
  printf '%s\n' "$restore_rc" > "$out/recovery-exit-code"
  if [[ $restore_rc != 0 ]]; then echo "RECOVERY_REQUIRES_REVIEW $out" >&2; fi
 fi
 # Keep original failure status even if recovery succeeds. Never erase evidence.
 exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
capture before
if [[ $mode == expand && $(jq 'length' "$out/before/patch.json") == 0 ]]; then
 mutation_attempted=true
fi
# Mandatory operator-audited hook: verifies cryptographic receipts and baseline
# (preflight), then actual mounted materials and normal cross-member I/O (after).
# It must be self-contained and accept MODE PHASE EVIDENCE KUBECONFIG CONTEXT.
timeout --kill-after=5s 120s bash "$out/verifier.sh" "$mode" preflight "$out" "$kubeconfig" "$context" > "$out/preflight.log" 2>&1
capture admitted
if [[ $(jq 'length' "$out/admitted/patch.json") != 0 ]]; then
 timeout --kill-after=2s 20s "${k[@]}" patch sts "$sts" --type=json \
  --patch-file="$out/admitted/patch.json" --dry-run=server -o json > "$out/server-dry-run.json"
 # Re-read every guarded object after dry-run; do not retry a stale patch.
 capture fresh
 mutation_attempted=true
 timeout --kill-after=2s 20s "${k[@]}" patch sts "$sts" --type=json \
  --patch-file="$out/fresh/patch.json" -o json > "$out/patch-result.json"
elif [[ $mode == expand ]]; then
 # A resumed, already-expanded spec still needs runtime verification/recovery.
 mutation_attempted=true
fi
timeout --kill-after=5s 310s "${k[@]}" rollout status "sts/$sts" --timeout=300s > "$out/rollout.log" 2>&1
capture after
jq -e 'length==0' "$out/after/patch.json" >/dev/null
jq -e '.metadata.generation==.status.observedGeneration and .status.readyReplicas==3 and
 .status.updatedReplicas==3 and .status.currentRevision==.status.updateRevision and
 (.status.currentRevision|type=="string" and length>0)' "$out/after/current.json" >/dev/null
jq -e --slurpfile sts "$out/after/current.json" '
 $sts[0] as $s |
 [.items[]|select(any(.metadata.ownerReferences[]?;.uid==$s.metadata.uid and .controller==true))] as $pods |
 ($pods|length)==3 and all($pods[];
  .metadata.deletionTimestamp==null and .metadata.labels["controller-revision-hash"]==$s.status.updateRevision and
  any(.status.conditions[]?;.type=="Ready" and .status=="True") and
  any(.status.containerStatuses[]?;.name=="kubebrain" and .ready==true and .state.running!=null) and
  ([.spec.containers[]|select(.name=="kubebrain")|{image,args,volumeMounts}]==
   [$s.spec.template.spec.containers[]|select(.name=="kubebrain")|{image,args,volumeMounts}]) and
  ([.spec.volumes[]|select(.name=="peer-tls")]==[$s.spec.template.spec.volumes[]|select(.name=="peer-tls")]))
' "$out/after/pods.json" >/dev/null
timeout --kill-after=5s 120s bash "$out/verifier.sh" "$mode" after "$out" "$kubeconfig" "$context" > "$out/verify.log" 2>&1
capture final
jq -e 'length==0' "$out/final/patch.json" >/dev/null
jq -e --slurpfile before "$out/after/current.json" '.metadata.uid==$before[0].metadata.uid and .spec==$before[0].spec and .metadata.generation==$before[0].metadata.generation and
 .metadata.generation==.status.observedGeneration and .status.readyReplicas==3 and .status.updatedReplicas==3 and .status.currentRevision==.status.updateRevision' "$out/final/current.json" >/dev/null
jq -e --slurpfile before "$out/after/pods.json" --slurpfile sts "$out/after/current.json" '
 def identities: [.items[]|select(any(.metadata.ownerReferences[]?;.uid==$sts[0].metadata.uid and .controller==true))|
  {uid:.metadata.uid,deleting:.metadata.deletionTimestamp,spec:.spec,status:.status}]|sort_by(.uid);
 identities==($before[0]|identities)' "$out/final/pods.json" >/dev/null
echo "FIRST_TRUST_PHASE_VERIFIED phase=$trust_phase mode=$mode"
