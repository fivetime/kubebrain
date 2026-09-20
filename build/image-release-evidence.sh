#!/usr/bin/env bash
# Emit release metadata only after the workflow's actual image checks succeed.
# This formatter alone cannot authenticate CI or authorize an experiment.
set -euo pipefail
if [[ $# != 6 ]]; then
  echo 'require image, index file, source SHA, repository, run ID and attempt' >&2
  exit 2
fi
image=$1
index=$2
source_sha=$3
repository=$4
run_id=$5
attempt=$6
[[ "$image" =~ ^ghcr\.io/fivetime/kubebrain@sha256:[a-f0-9]{64}$ ]]
[[ "$source_sha" =~ ^[a-f0-9]{40}$ ]]
[[ "$repository" == fivetime/kubebrain ]]
[[ "$run_id" =~ ^[1-9][0-9]*$ && "$attempt" =~ ^[1-9][0-9]*$ ]]
[[ ${#run_id} -le 20 && ${#attempt} -le 10 ]]
[[ -f "$index" && ! -L "$index" ]]
[[ $(stat -c %s -- "$index") -le 1048576 ]]
index_sha=$(sha256sum -- "$index")
index_sha=${index_sha%% *}
[[ "$image" == "ghcr.io/fivetime/kubebrain@sha256:$index_sha" ]]
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
amd64=$(bash "$script_dir/image-platform-digest.sh" "$index" amd64)
arm64=$(bash "$script_dir/image-platform-digest.sh" "$index" arm64)
[[ "$amd64" != "$arm64" ]]
jq -n --arg image "$image" --arg source "$source_sha" \
  --arg repository "$repository" --arg run_id "$run_id" --arg attempt "$attempt" \
  --arg index_sha "$index_sha" --arg amd64 "$amd64" --arg arm64 "$arm64" \
  '{schema_version:1,repository:$repository,source:$source,run_id:$run_id,
    run_attempt:$attempt,workflow:".github/workflows/image.yml",image:$image,
    index_sha256:$index_sha,platforms:{"linux/amd64":$amd64,"linux/arm64":$arm64}}'
