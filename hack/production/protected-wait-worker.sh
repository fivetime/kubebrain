#!/usr/bin/env bash
# One preprepared protected-stack observation, compatible with WithPreparedFault.
# No RPC renewal, network mutation, experiment retry or restoration.
set -euo pipefail
umask 077
[[ $# == 5 && $2 =~ ^[a-f0-9]{40}$ && $3 =~ ^[a-f0-9]{64}$ && $4 =~ ^[01]$ ]] || exit 2
wait_pod=$1; wait_source=$2; wait_hash=$3; wait_count=$4; wait_dir=$5
wait_script=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/protected-wait-worker.sh
stack_info_port=${stack_info_port:-18588}
stack_anonymous_port=${stack_anonymous_port:-18589}
source "${wait_script%/*}/protected-stack-session.sh"
stack_session_owner_active || exit 2
[[ $stack_owner == /* && $stack_owner != *$'\n'* && $stack_owner != *$'\t'* &&
 ${wait_dir%/*} == "$stack_owner" && ${wait_dir##*/} =~ ^stack-worker\.[a-zA-Z0-9]{8}$ &&
 -d $wait_dir && ! -L $wait_dir && $(stat -c '%a:%u' "$wait_dir") == "700:$EUID" &&
 -z $(find "$wait_dir" -mindepth 1 -maxdepth 1 -print -quit) ]] || exit 2
mkdir "$wait_dir/started" # Single-use receipt directory, never adopt a prior run.
wait_cleanup() {
 local rc=$?
 trap - EXIT
 stack_session_close
 printf '%s\n' "$rc" > "$wait_dir/exit-code"
 exit "$rc"
}
trap wait_cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
stack_session_verify_inputs
for wait_tool in "$wait_script" "$stack_library_dir/expired-lease-wait-frames.jq"; do
 wait_binding=$(stack_session_run sha256sum "$wait_tool")
 stack_session_run grep -Fxq -- "$wait_binding" "$stack_owner/tools.sha256"
done
stack_session_prepare "$wait_pod"
printf 'FAULT_READY\n'

# A single bounded control record. Parent must additionally enforce its own
# preparation deadline, cancel this process group, and join before restoration.
wait_origin=''
wait_input_deadline=$((SECONDS+60))
while true; do
 stack_session_owner_active || exit 2
 (( SECONDS < wait_input_deadline && ${#wait_origin} < 20 )) || exit 124
 wait_piece=''; wait_rc=0
 IFS= read -r -n "$((20-${#wait_origin}))" -t 1 wait_piece || wait_rc=$?
 wait_origin+=$wait_piece
 (( wait_rc != 0 )) || break
 (( wait_rc > 128 )) || exit 2
done
[[ $wait_origin =~ ^[1-8][0-9]{18}$ ]] || exit 2
(( SECONDS < wait_input_deadline )) || exit 124
stack_session_capture_expired_wait "$wait_origin" "$wait_source" "$wait_hash" "$wait_count"
printf '%s\n' "$stack_capture" > "$wait_dir/capture-path"
printf '%s\n' "$stack_wait" > "$wait_dir/classification-path"
printf '%s\n' "$wait_origin" > "$wait_dir/origin-ns"
stack_session_owner_active
stack_session_budget
printf 'FAULT_DONE\t%s\n' "$wait_origin"
