#!/usr/bin/env bash
# Read-only Join for a dedicated, non-shared PID namespace. The admitted driver
# must be PID 1 and must have stopped/waited its managed children before calling
# this script. This is not a host cleanup command or an orphan reaper.
set -euo pipefail
[[ $# == 1 && $1 == /* && $1 != / ]] || exit 2
owner=$1
[[ -d $owner && ! -L $owner && $(realpath -e -- "$owner") == "$owner" &&
   $(stat -c '%a:%u' -- "$owner") == "700:$EUID" ]] || exit 2
identity=$owner/join-namespace.tsv
[[ -f $identity && ! -L $identity && $(stat -c '%a:%u' -- "$identity") == "600:$EUID" &&
   $(stat -c '%s' -- "$identity") -le 256 ]] || exit 2
# This file is captured before workers start and independently pinned in the
# command plan's Files map. Never discover/refresh it during recovery.
exec {identity_fd}<"$identity"
IFS=$'\t' read -r version namespace boot started directory extra <&"$identity_fd"
[[ $version == v1 && $namespace =~ ^[0-9]+:[0-9]+$ &&
   $boot =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ &&
   $started =~ ^[1-9][0-9]*$ && $directory =~ ^[0-9]+:[0-9]+$ && -z $extra ]] || exit 2
tail=
if IFS= read -r tail <&"$identity_fd" || [[ -n $tail ]]; then exit 2; fi
exec {identity_fd}<&-
[[ $PPID == 1 && $(stat -f -c '%T' /proc) == proc &&
   $(stat -Lc '%d:%i' /proc/1/ns/pid) == "$namespace" &&
   $(stat -Lc '%d:%i' /proc/self/ns/pid) == "$namespace" &&
   $(stat -c '%d:%i' -- "$owner") == "$directory" ]] || exit 2
IFS= read -r current_boot </proc/sys/kernel/random/boot_id
IFS= read -r init_stat </proc/1/stat
# comm (field 2) may contain spaces or ')'; remaining fields start at state.
read -r -a fields <<<"${init_stat##*) }"
[[ $current_boot == "$boot" && ${fields[19]:-} == "$started" ]] || exit 2
# No subprocesses from this point to exit. A zombie also refuses success: the
# driver must reap it. Escaped process groups/sessions are still visible here.
# Admission must enforce an ordinary, unmasked /proc and a private PID namespace
# (no hostPID/shareProcessNamespace/sidecars/exec sessions) for this exact driver.
for entry in /proc/[0-9]*; do
  pid=${entry##*/}
  if [[ $pid != 1 && $pid != "$$" ]]; then
    printf 'JOIN_REFUSED_PROCESS_REMAINS pid=%s\n' "$pid" >&2
    exit 75
  fi
done
printf 'ISOLATED_PID_NAMESPACE_EMPTY_NOT_FAULT_ACCEPTANCE\n'
