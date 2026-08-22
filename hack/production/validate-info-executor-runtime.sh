#!/usr/bin/env bash
set -euo pipefail

required_commands=(
  awk basename bash chmod cp curl cut date dirname grep head id jq ln mktemp openssl
  realpath rm sed sha256sum sort stat sync tail tr wc
)
if [[ "${REQUIRE_INFO_EXECUTOR_BINARIES:-false}" == true ]]; then
  required_commands+=(kubectl kubebrain-operationctl kubebrain-operation-worker)
elif [[ "${REQUIRE_INFO_EXECUTOR_BINARIES:-false}" != false ]]; then
  echo "REQUIRE_INFO_EXECUTOR_BINARIES must be true or false" >&2
  exit 2
fi

for command_name in "${required_commands[@]}"; do
  command -v "$command_name" >/dev/null || {
    echo "required info executor command is missing: ${command_name}" >&2
    exit 1
  }
done

umask 077
probe_dir="$(mktemp -d)"
cleanup() { rm -rf -- "$probe_dir"; }
trap cleanup EXIT INT TERM

temporary="$(mktemp "${probe_dir}/.evidence.tmp.XXXXXX")"
printf 'kubebrain-info-executor-runtime\n' >"$temporary"
chmod 600 "$temporary"
attributes="$(stat -Lc '%a:%u:%h:%s' -- "$temporary")"
expected="600:$(id -u):1:32"
[[ "$attributes" == "$expected" ]] || {
  echo "private evidence attributes are unsupported: got ${attributes}, want ${expected}" >&2
  exit 1
}
[[ "$(realpath -m -- "${probe_dir}/nested/..")" == "$probe_dir" ]] || {
  echo "realpath -m does not preserve the executor workspace boundary" >&2
  exit 1
}

sync -f "$temporary"
published="${probe_dir}/evidence"
ln -- "$temporary" "$published"
rm -f -- "$temporary"
sync -f "$probe_dir"
[[ "$(stat -Lc '%a:%u:%h:%s' -- "$published")" == "$expected" ]] || {
  echo "durable no-clobber publication attributes are unsupported" >&2
  exit 1
}
[[ "$(<"$published")" == kubebrain-info-executor-runtime ]] || {
  echo "durable no-clobber publication content changed" >&2
  exit 1
}

echo "info executor runtime tool contract passed"
