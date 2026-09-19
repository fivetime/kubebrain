#!/usr/bin/env bash
# Typed, read-only observation. 0=matched, 75=identity-verified pending; other=fatal.
set -euo pipefail
umask 077
[[ $# == 5 ]] || exit 2
owner=$1; mode=$2; uid=$3; name=$4; expected=$5
[[ $owner == /* && -d $owner && ! -L $owner && $(stat -c '%a:%u' "$owner") == "700:$EUID" &&
 ($mode == present || $mode == absent) && $uid =~ ^[A-Za-z0-9-]+$ &&
 $name =~ ^kb-term-[a-z0-9]+$ && $expected == /* && -f $expected && ! -L $expected ]] || exit 2
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
pod=$(jq -er '.metadata.name|select(test("^kubebrain-local-[012]$"))' "$expected") || exit 65
out=$(mktemp -d "$owner/policy-observation.XXXXXXXX")
echo "EVIDENCE=$out"
trap 'printf "%s\n" "$?" > "$out/observation.exit"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
sha256sum "$expected" > "$out/expected.sha256"
rc=0
timeout --foreground --kill-after=1s 30s bash "$here/capture-local-cilium-endpoint.sh" "$owner" "$pod" > "$out/capture.log" 2>&1 || rc=$?
if [[ $rc != 0 ]]; then [[ $rc != 124 ]] || exit 124; exit 65; fi
capture=$(sed -n 's/^EVIDENCE=//p' "$out/capture.log")
[[ ${capture%/*} == "$owner" && ${capture##*/} =~ ^endpoint\.[a-zA-Z0-9]+$ && -d $capture && ! -L $capture ]] || exit 65
[[ $(<"$capture/capture.exit") == 0 ]] || exit 65
sha256sum -c "$capture/evidence.sha256" "$out/expected.sha256" > "$out/hash-check.log" 2>&1 || exit 65
for current in pod.json pod-after.json; do
 jq -n --slurpfile expected "$expected" --slurpfile current "$capture/$current" \
  'if ($expected|length)==1 and ($current|length)==1 then {expected:$expected[0],current:$current[0]} else error("one Pod per file required") end' |
  jq -e -f "$here/../../hack/production/same-pod-process.jq" >/dev/null || exit 65
done
jq -e --arg mode "$mode" --arg policy_uid "$uid" --arg policy_name "$name" \
 -f "$here/local-policy-observation.jq" "$capture/endpoint.json" > "$out/result.json" 2> "$out/classifier.stderr" || exit 65
sha256sum "$out/result.json" "$out/capture.log" "$out/expected.sha256" "$capture/evidence.sha256" > "$out/evidence.sha256"
case $(jq -er .state "$out/result.json") in
 matched) echo SAME_PROCESS_POLICY_STATE_MATCHED; exit 0;;
 pending) echo SAME_PROCESS_POLICY_STATE_PENDING; exit 75;;
 *) exit 65;;
esac
