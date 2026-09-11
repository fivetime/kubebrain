#!/usr/bin/env bash
# Test-only transparent wrapper: interrupt our runner after an owned create.
set -euo pipefail
[[ "${INTERRUPT_TARGET_PID:-}" =~ ^[1-9][0-9]*$ ]] || exit 2
case "${INTERRUPT_PHASE:-}" in pd|tikv) ;; *) exit 2 ;; esac
args=("$@")
created=false
owned=false
name=''
for ((i=0; i<${#args[@]}; i++)); do
  case "${args[i]}" in
    create) created=true ;;
    --name) name="${args[i+1]:-}" ;;
    --label)
      if [[ "${args[i+1]:-}" =~ ^io\.kubebrain\.local-protocol-owner=[a-f0-9]{32}$ ]]; then owned=true; fi
      ;;
  esac
done
/usr/bin/docker "$@"
if [[ "$created" == true && "$owned" == true && "$name" =~ ^kb-protocol-[a-f0-9]{32}-${INTERRUPT_PHASE}$ ]]; then
  printf 'INTERRUPT_AFTER_CREATE phase=%s\n' "$INTERRUPT_PHASE" >&2
  kill -TERM "$INTERRUPT_TARGET_PID"
fi
