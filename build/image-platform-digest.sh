#!/usr/bin/env bash
# Resolve one runtime child, not an index that a classic Docker image store
# cannot associate with two different local platform images simultaneously.
set -euo pipefail
[[ $# == 2 ]] || { echo 'usage: image-platform-digest.sh INDEX_JSON ARCH' >&2; exit 2; }
case "$2" in amd64|arm64) ;; *) echo 'unsupported runtime architecture' >&2; exit 2 ;; esac
jq -ers --arg arch "$2" '
  if length != 1 then error("expected one index JSON document") else .[0] end
  |
  [.manifests[] | select(.platform.os == "linux" and .platform.architecture == $arch)]
  | if length != 1 then error("expected exactly one runtime manifest for " + $arch)
    else .[0] end
  | if .mediaType != "application/vnd.oci.image.manifest.v1+json" and
       .mediaType != "application/vnd.docker.distribution.manifest.v2+json"
    then error("runtime descriptor is not an image manifest") else . end
  | .digest
  | if type != "string" then error("runtime digest is not a string")
    elif length == 71 and test("^sha256:[0-9a-f]{64}$") then .
    else error("invalid runtime sha256 digest") end
' "$1"
