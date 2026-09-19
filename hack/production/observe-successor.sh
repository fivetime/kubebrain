#!/usr/bin/env bash
# Observe an original, already-running 30-second fault gate. No fault injection.
# Callback contract: CALLBACK BUDGET_SECONDS OUTPUT_JSON. It must perform one
# authenticated Status request without retries and write its raw JSON response.
set -euo pipefail
umask 077
check_callback() {
 [[ $1 == /* && -f $1 && ! -L $1 && -x $1 ]] || {
  echo 'successor callback must be an absolute executable regular non-symlink file' >&2
  return 1
 }
}
# Admission-only check: call before deployment/fault mutation. Never invoke the
# callback here, and recheck at observation time to catch subsequent mode drift.
if [[ $# == 2 && $1 == --check-callback ]]; then
 check_callback "$2"
 exit
fi
[[ $# == 7 ]] || exit 2
out=$1; start=$2; cluster=$3; observer=$4; old_holder=$5; old_term=$6; callback=$7
[[ $out == /* ]]
check_callback "$callback"
[[ $start =~ ^[1-9][0-9]{18}$ && $start < 9223372006854775807 ]]
for identity in "$cluster" "$observer" "$old_holder" "$old_term"; do
 [[ $identity =~ ^[1-9][0-9]{0,19}$ ]]
 if [[ ${#identity} == 20 ]]; then [[ $identity < 18446744073709551616 ]]; fi
done
deadline=$((start+30000000000))
mkdir -m 700 "$out"
trap 'printf "%s\n" "$?" > "$out/exit-code"' EXIT
jq -n --arg start "$start" --arg deadline "$deadline" --arg cluster "$cluster" --arg observer "$observer" --arg old "$old_holder" --arg term "$old_term" \
 '{fault_start_ns:$start,deadline_ns:$deadline,cluster:$cluster,observer:$observer,old_leader:$old,old_term:$term,gate_seconds:30}' > "$out/input.json"
sha256sum "$callback" > "$out/callback.sha256"
sample=0
while :; do
 now=$(date -u +%s%N)
 [[ $now =~ ^[1-9][0-9]{18}$ && $now -ge $start ]] || exit 3
 (( now < deadline )) || exit 1
 remaining=$((deadline-now))
 budget=$remaining
 (( budget <= 5000000000 )) || budget=5000000000
 printf -v seconds '%d.%09d' "$((budget/1000000000))" "$((budget%1000000000))"
 dir=$out/sample-$sample
 mkdir -m 700 "$dir"
 rc=0
 # Timeout may need to kill a noncooperative callback; a late completion is
 # never accepted even if it happens to contain a successor response.
 timeout --kill-after=1s "$seconds" "$callback" "$seconds" "$dir/status.json" > "$dir/stdout" 2> "$dir/stderr" || rc=$?
 ended=$(date -u +%s%N)
 jq -n --arg begun "$now" --arg ended "$ended" --arg budget "$seconds" --argjson rc "$rc" \
  '{started_ns:$begun,ended_ns:$ended,budget_seconds:$budget,exit_code:$rc}' > "$dir/observation.json"
 [[ $ended =~ ^[1-9][0-9]{18}$ && $ended -ge $now ]] || exit 3
 (( ended <= deadline )) || exit 1
 if [[ $rc == 0 && -f $dir/status.json && ! -L $dir/status.json ]] &&
  [[ $(stat -c %s "$dir/status.json") -le 65536 ]] &&
  jq -se --arg cluster "$cluster" --arg observer "$observer" --arg old "$old_holder" --arg term "$old_term" '
  def uint: type=="string" and test("^[1-9][0-9]{0,19}$") and (length<20 or .<"18446744073709551616");
  def greater($a;$b): ($a|length)>($b|length) or (($a|length)==($b|length) and $a>$b);
  length==1 and (.[0] | .error==null and .header.cluster_id==$cluster and .header.member_id==$observer and
  (.leader|uint) and .leader!=$old and (.header.raft_term|uint) and greater(.header.raft_term;$term))
 ' "$dir/status.json" > "$dir/predicate.stdout" 2> "$dir/predicate.stderr"; then
  decided=$(date -u +%s%N)
  printf '%s\n' "$decided" > "$dir/decision.ns"
  [[ $decided =~ ^[1-9][0-9]{18}$ && $decided -ge $ended ]] || exit 3
  (( decided <= deadline )) || exit 1
  printf '%s\n' "$dir" > "$out/successor-sample"
  echo SUCCESSOR_OBSERVED_WITHIN_ORIGINAL_GATE
  exit 0
 fi
 sample=$((sample+1))
 now=$(date -u +%s%N)
 [[ $now =~ ^[1-9][0-9]{18}$ && $now -ge $ended ]] || exit 3
 (( now < deadline )) || exit 1
 remaining=$((deadline-now))
 (( remaining <= 200000000 )) || remaining=200000000
 printf -v pause '%d.%09d' "$((remaining/1000000000))" "$((remaining%1000000000))"
 sleep "$pause"
done
