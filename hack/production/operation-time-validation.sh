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
