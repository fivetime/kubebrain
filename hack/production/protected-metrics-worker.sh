#!/usr/bin/env bash
# Fresh controller process; never background a prepared shell's job table.
set -euo pipefail
umask 077
[[ $# == 2 && $2 =~ ^(0|[1-9][0-9]{0,10})$ ]] || exit 2
(( $2 < 30000000000 )) || exit 2
worker_pod=$1
worker_offset=$2
worker_script=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/protected-metrics-worker.sh
stack_info_port=${stack_info_port:-18586}
stack_anonymous_port=${stack_anonymous_port:-18587}
source "${worker_script%/*}/protected-stack-session.sh"
stack_session_owner_active || exit 2
[[ $stack_owner == /* && $stack_owner != *$'\n'* && $stack_owner != *$'\t'* ]] || exit 2
worker_dir=$(mktemp -d "$stack_owner/metrics-worker.XXXXXXXX")
worker_cleanup() {
 local rc=$?
 trap - EXIT
 stack_session_close
 printf '%s\n' "$rc" > "$worker_dir/exit-code"
 exit "$rc"
}
trap worker_cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# The worker itself must be part of the independently admitted tool bundle.
stack_session_verify_inputs
worker_binding=$(stack_session_run sha256sum "$worker_script")
stack_session_run grep -Fxq -- "$worker_binding" "$stack_owner/tools.sha256"
stack_session_prepare "$worker_pod"
stack_session_capture_metrics before-fault
printf '%s\n' "$stack_capture" > "$worker_dir/baseline-path"
stack_session_owner_active
printf 'READY\t%s\t%s\n' "$worker_dir" "$stack_capture"

# One bounded control record, not shell input. EOF, timeout, malformed/future
# origins and repeated scheduling cannot silently start another experiment.
worker_origin=''
worker_input_deadline=$((SECONDS+60))
while true; do
 stack_session_owner_active || exit 2
 (( SECONDS < worker_input_deadline && ${#worker_origin} < 20 )) || exit 124
 worker_piece=''
 worker_read_rc=0
 IFS= read -r -n "$((20-${#worker_origin}))" -t 1 worker_piece || worker_read_rc=$?
 worker_origin+=$worker_piece
 (( worker_read_rc != 0 )) || break
 # A timed-out read can consume a partial record: retain it across checks.
 (( worker_read_rc > 128 )) || exit 2
done
stack_session_owner_active || exit 2
(( SECONDS < worker_input_deadline )) || exit 124
[[ $worker_origin =~ ^[1-8][0-9]{18}$ ]] || exit 2
stack_session_capture_metrics_at "$worker_origin" "$worker_offset"
printf '%s\n' "$stack_schedule" > "$worker_dir/schedule-path"
stack_session_owner_active
stack_session_budget
printf 'CAPTURED\t%s\t%s\n' "$stack_capture" "$stack_schedule"
