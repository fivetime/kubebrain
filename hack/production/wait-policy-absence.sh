#!/usr/bin/env bash
# Read-only recovery observer. Never use to extend the original response deadline.
# Callback: exit 0=verified absent, 75=verified pending, anything else=fatal.
# Caller supplies one fixed absolute deadline and a trusted callback SHA-256.
# Run under an outer process-group watchdog, as for protected-stack-session.sh.
set -euo pipefail
umask 077
[[ $# == 4 ]] || exit 2
out=$1; deadline=$2; callback=$3; expected=$4
[[ $out == /* && ! -e $out && ! -L $out && $deadline =~ ^[1-8][0-9]{18}$ &&
   $callback == /* && -f $callback && ! -L $callback && $expected =~ ^[a-f0-9]{64}$ ]] || exit 2
parent=$(dirname -- "$out")
[[ -d $parent && ! -L $parent && $(stat -c '%a:%u' "$parent") == "700:$EUID" ]] || exit 2
now=$(date -u +%s%N)
[[ $now =~ ^[1-8][0-9]{18}$ ]] || exit 2
(( deadline > now )) || exit 124
(( deadline-now <= 60000000000 )) || exit 2
actual=$(sha256sum "$callback"); [[ ${actual%% *} == "$expected" ]] || exit 65
mkdir -m 700 -- "$out"
printf '%s\n' "$deadline" > "$out/deadline.ns"
printf '%s  %s\n' "$expected" "$callback" > "$out/callback.sha256"
trap 'printf "%s\n" "$?" > "$out/exit-code"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
previous=$now
index=0
while :; do
 now=$(date -u +%s%N)
 [[ $now =~ ^[1-8][0-9]{18}$ && $now -ge $previous ]] || exit 65
 (( now < deadline )) || exit 124
 remaining=$((deadline-now))
 (( remaining <= 30000000000 )) || remaining=30000000000
 printf -v budget '%d.%09ds' "$((remaining/1000000000))" "$((remaining%1000000000))"
 attempt=$out/sample-$index
 mkdir "$attempt"
 printf '%s\n' "$now" > "$attempt/started.ns"
 sha256sum -c "$out/callback.sha256" > "$attempt/hash-check.log" 2>&1 || exit 65
 rc=0
 timeout --foreground --kill-after=1s "$budget" bash "$callback" absent > "$attempt/stdout.log" 2> "$attempt/stderr.log" || rc=$?
 printf '%s\n' "$rc" > "$attempt/exit-code"
 ended=$(date -u +%s%N)
 printf '%s\n' "$ended" > "$attempt/ended.ns"
 [[ $ended =~ ^[1-8][0-9]{18}$ && $ended -ge $now ]] || exit 65
 (( ended < deadline )) || exit 124
 case $rc in
  0)
   sha256sum -c "$out/callback.sha256" > "$attempt/final-hash-check.log" 2>&1 || exit 65
   finished=$(date -u +%s%N)
   [[ $finished =~ ^[1-8][0-9]{18}$ && $finished -ge $ended ]] || exit 65
   (( finished < deadline )) || exit 124
   printf '%s\n' "$finished" > "$out/verified.ns"
   printf '%s\n' "$attempt" > "$out/matched-sample"
   echo POLICY_ABSENCE_VERIFIED_WITHIN_RECOVERY_DEADLINE
   exit 0;;
  75) :;;
  *) exit "$rc";;
 esac
 previous=$ended
 remaining=$((deadline-ended))
 (( remaining <= 250000000 )) || remaining=250000000
 printf -v delay '%d.%09ds' "$((remaining/1000000000))" "$((remaining%1000000000))"
 sleep "$delay"
 index=$((index+1))
done
