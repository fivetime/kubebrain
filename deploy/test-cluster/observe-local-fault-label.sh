#!/usr/bin/env bash
# 0=matched, 75=stable Pod label but endpoint identity still converging, other=fatal.
set -euo pipefail
umask 077
[[ $# == 4 ]] || exit 2
owner=$1; mode=$2; expected=$3; token=$4
[[ $owner == /* && -d $owner && ! -L $owner && $(stat -c '%a:%u' "$owner") == "700:$EUID" &&
 ($mode == present || $mode == absent) && $expected == /* && -f $expected && ! -L $expected && $token =~ ^term-[a-z0-9]+$ ]] || exit 2
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
pod=$(jq -er '.metadata.name|select(test("^kubebrain-local-[012]$"))' "$expected") || exit 65
out=$(mktemp -d "$owner/identity-observation.XXXXXXXX")
echo "EVIDENCE=$out"
trap 'printf "%s\n" "$?" > "$out/observation.exit"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
sha256sum "$expected" > "$out/expected.sha256"
rc=0
timeout --foreground --kill-after=1s 30s bash "$here/capture-local-cilium-endpoint.sh" "$owner" "$pod" --label-transition "$mode" "$token" > "$out/capture.log" 2>&1 || rc=$?
if [[ $rc != 0 ]]; then [[ $rc != 124 ]] || exit 124; exit 65; fi
capture=$(sed -n 's/^EVIDENCE=//p' "$out/capture.log")
[[ ${capture%/*} == "$owner" && ${capture##*/} =~ ^endpoint\.[a-zA-Z0-9]+$ && -d $capture && ! -L $capture && $(<"$capture/capture.exit") == 0 ]] || exit 65
sha256sum -c "$capture/evidence.sha256" "$out/expected.sha256" > "$out/hash-check.log" 2>&1 || exit 65
for current in pod.json pod-after.json; do
 jq -n --slurpfile a "$expected" --slurpfile b "$capture/$current" \
 'if ($a|length)==1 and ($b|length)==1 then {expected:$a[0],current:$b[0]} else error("one Pod required") end' |
 jq -e -f "$here/../../hack/production/same-pod-process.jq" >/dev/null || exit 65
 jq -e --arg mode "$mode" --arg token "$token" '
 if $mode=="present" then .metadata.labels["kubebrain.io/fault-owner"]==$token
 else ((.metadata.labels//{})|has("kubebrain.io/fault-owner")|not) end' "$capture/$current" >/dev/null || exit 65
done
jq -e --arg mode "$mode" 'select(.mode==$mode and (.state=="matched" or .state=="pending") and
 .scope=="label_identity_transition_only_not_enforcement") |
 .scope="same_process_label_identity_only"' "$capture/label-transition.json" > "$out/result.json" || exit 65
sha256sum "$out/result.json" "$out/capture.log" "$out/expected.sha256" "$capture/evidence.sha256" > "$out/evidence.sha256"
case $(jq -er .state "$out/result.json") in matched) exit 0;; pending) exit 75;; *) exit 65;; esac
