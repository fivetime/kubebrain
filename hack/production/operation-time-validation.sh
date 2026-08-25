#!/usr/bin/env bash

# Shared exact-decimal admission for durable Operation worker lease clocks.
# Callers must validate before any Bash arithmetic or operationctl invocation.
OPERATION_MAX_INT64=9223372036854775807
OPERATION_MAX_UINT64=18446744073709551615
OPERATION_MAX_INT32=2147483647
OPERATION_MAX_UINT32=4294967295

# Keep large external identifiers out of ERE repetition bounds. musl regex(3)
# rejects bounds above RE_DUP_MAX (255), while glibc accepts the 511 bound that
# was previously used by the JWT rotation scripts.
operation_is_external_version_id() {
  local value="$1"
  (( ${#value} >= 1 && ${#value} <= 512 )) || return 1
  [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._:/@+=-]*$ ]]
}

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

operation_is_positive_uint64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,19}$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$OPERATION_MAX_UINT64" ]]; then
    return 1
  fi
}

operation_is_nonnegative_uint64() {
  local value="$1"
  [[ "$value" =~ ^(0|[1-9][0-9]{0,19})$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$OPERATION_MAX_UINT64" ]]; then
    return 1
  fi
}

operation_is_nonnegative_uint32() {
  local value="$1"
  [[ "$value" =~ ^(0|[1-9][0-9]{0,9})$ ]] || return 1
  if (( ${#value} == 10 )) && [[ "$value" > "$OPERATION_MAX_UINT32" ]]; then
    return 1
  fi
}

operation_is_nonnegative_int32() {
  local value="$1"
  [[ "$value" =~ ^(0|[1-9][0-9]{0,9})$ ]] || return 1
  if (( ${#value} == 10 )) && [[ "$value" > "$OPERATION_MAX_INT32" ]]; then
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

operation_is_positive_go_duration_hms() {
  local value="$1" magnitude
  if [[ "$value" == *h ]]; then
    magnitude="${value%h}"
    operation_is_positive_int64 "$magnitude" || return 1
    (( magnitude <= 2562047 ))
    return
  fi
  operation_is_positive_go_duration "$value"
}

operation_is_nonnegative_go_duration_hms() {
  local value="$1"
  [[ "$value" == 0 ]] || operation_is_positive_go_duration_hms "$value"
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
