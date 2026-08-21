#!/usr/bin/env bash

# Shared exact-decimal admission for durable Operation worker lease clocks.
# Callers must validate before any Bash arithmetic or operationctl invocation.
OPERATION_MAX_INT64=9223372036854775807

operation_is_positive_int64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,18}$ ]] || return 1
  if (( ${#value} == 19 )) && [[ "$value" > "$OPERATION_MAX_INT64" ]]; then
    return 1
  fi
}

operation_is_nonnegative_int64() {
  local value="$1"
  [[ "$value" =~ ^(0|[1-9][0-9]{0,18})$ ]] || return 1
  if (( ${#value} == 19 )) && [[ "$value" > "$OPERATION_MAX_INT64" ]]; then
    return 1
  fi
}

operation_is_positive_int64_duration() {
  local value="$1" magnitude
  [[ "$value" =~ ^[1-9][0-9]*(s|m|h)$ ]] || return 1
  magnitude="${value%?}"
  operation_is_positive_int64 "$magnitude"
}

operation_is_positive_go_duration() {
  local value="$1" magnitude maximum
  case "$value" in
    *ms) magnitude="${value%ms}"; maximum=9223372036854 ;;
    *s) magnitude="${value%s}"; maximum=9223372036 ;;
    *m) magnitude="${value%m}"; maximum=153722867 ;;
    *) return 1 ;;
  esac
  operation_is_positive_int64 "$magnitude" || return 1
  if (( ${#magnitude} != ${#maximum} )); then
    (( ${#magnitude} < ${#maximum} ))
  else
    [[ "$magnitude" < "$maximum" || "$magnitude" == "$maximum" ]]
  fi
}

operation_is_positive_go_seconds_decimal() {
  local value="$1" whole fraction maximum_whole=9223372036 maximum_fraction=854775807
  [[ "$value" =~ ^(0|[1-9][0-9]{0,18})([.]([0-9]{1,9}))?$ ]] || return 1
  whole="${BASH_REMATCH[1]}"
  fraction="${BASH_REMATCH[3]:-}"
  [[ "$whole" != 0 || ( -n "$fraction" && "$fraction" =~ [1-9] ) ]] || return 1
  if (( ${#whole} != ${#maximum_whole} )); then
    (( ${#whole} < ${#maximum_whole} )) || return 1
  elif [[ "$whole" > "$maximum_whole" ]]; then
    return 1
  fi
  if [[ "$whole" == "$maximum_whole" ]]; then
    fraction="${fraction}000000000"
    fraction="${fraction:0:9}"
    [[ "$fraction" < "$maximum_fraction" || "$fraction" == "$maximum_fraction" ]] || return 1
  fi
}

operation_is_decimal_int64() {
  local value="$1" whole
  [[ "$value" =~ ^(0|[1-9][0-9]{0,18})([.][0-9]{1,9})?$ ]] || return 1
  whole="${value%%.*}"
  if (( ${#whole} == 19 )) && [[ "$whole" > "$OPERATION_MAX_INT64" ]]; then
    return 1
  fi
}

operation_is_positive_decimal_less_than_int() {
  local value="$1" upper="$2" whole fraction=""
  operation_is_decimal_int64 "$value" || return 1
  whole="${value%%.*}"
  [[ "$value" != *.* ]] || fraction="${value#*.}"
  [[ "$whole" != "0" || "$fraction" =~ [1-9] ]] || return 1
  (( ${#whole} < ${#upper} )) && return 0
  (( ${#whole} == ${#upper} )) && [[ "$whole" < "$upper" ]]
}

operation_kill_process_group() {
  local pid="$1"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 0
  kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
}
