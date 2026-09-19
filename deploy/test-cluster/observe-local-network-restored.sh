#!/usr/bin/env bash
# Composite read-only observation; caller supplies the actual CREATE receipt UID,
# independent expected Pod/targets, exclusive ownership and overall deadline.
# Exit 75 remains pending, not success. No retries and no new fault clock.
set -euo pipefail
umask 077
[[ $# == 6 ]] || exit 2
owner=$1; mode=$2; uid=$3; name=$4; expected=$5; targets=$6
[[ $owner == /* && -d $owner && ! -L $owner && $(stat -c '%a:%u' "$owner") == "700:$EUID" &&
 ($mode == absent || $mode == absent-unlabelled) && $uid =~ ^[A-Za-z0-9-]+$ &&
 $name =~ ^kb-term-[a-z0-9]+$ && $expected == /* && -f $expected && ! -L $expected &&
 $targets == /* && -f $targets && ! -L $targets ]] || exit 2
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
out=$(mktemp -d "$owner/network-observation.XXXXXXXX")
echo "EVIDENCE=$out"
trap 'printf "%s\n" "$?" > "$out/observation.exit"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
sha256sum "$expected" "$targets" > "$out/inputs.sha256"
bash "$here/observe-local-policy-state.sh" "$owner" "$mode" "$uid" "$name" "$expected" > "$out/policy-before.log" 2>&1
bash "$here/observe-local-backend-tcp.sh" "$owner" "$expected" "$targets" > "$out/tcp.log" 2>&1
bash "$here/observe-local-policy-state.sh" "$owner" "$mode" "$uid" "$name" "$expected" > "$out/policy-after.log" 2>&1
sha256sum -c "$out/inputs.sha256" > "$out/input-check.log"
sha256sum "$out/inputs.sha256" "$out/policy-before.log" "$out/tcp.log" "$out/policy-after.log" > "$out/evidence.sha256"
echo SAME_SOURCE_POLICY_ABSENT_AND_BACKEND_TCP_CONNECTED_NOT_PROTOCOL_HEALTH
