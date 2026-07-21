#!/bin/bash
# Copyright 2022 ByteDance and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export pkg="github.com/kubewharf/kubebrain/cmd/version"
export version="${KUBEBRAIN_VERSION:-$(git describe --abbrev=0 --tags 2>/dev/null || git rev-parse --abbrev-ref HEAD 2>/dev/null || true)}"
export sha="${KUBEBRAIN_GIT_SHA:-$(git rev-parse --short HEAD 2>/dev/null || true)}"
go_version=$(go env GOVERSION)
go_os=$(go env GOOS)
go_arch=$(go env GOARCH)
export go_version go_os go_arch
export go_os_arch="$go_os/$go_arch"
export storage="$1"
export date="${KUBEBRAIN_BUILD_DATE:-$(date -u "+%Y-%m-%dT%H:%M:%SZ")}"

if [[ "${REQUIRE_BUILD_METADATA:-false}" == "true" ]]; then
  if [[ -z "$version" || -z "$sha" || -z "$date" ]]; then
    echo "KUBEBRAIN_VERSION, KUBEBRAIN_GIT_SHA, and KUBEBRAIN_BUILD_DATE are required" >&2
    return 1 2>/dev/null || exit 1
  fi
  if ! [[ "$sha" =~ ^[0-9a-fA-F]{40}$ ]]; then
    echo "KUBEBRAIN_GIT_SHA must be a full 40-character hexadecimal commit ID" >&2
    return 1 2>/dev/null || exit 1
  fi
fi

printf '\033[32m\n'
printf 'build env\n'
printf 'version   \t%s\n' "$version"
printf 'sha       \t%s\n' "$sha"
printf 'go_version\t%s\n' "$go_version"
printf 'go_os     \t%s\n' "$go_os"
printf 'go_arch   \t%s\n' "$go_arch"
printf 'storage   \t%s\n' "$storage"
printf '\033[37m\n'

ldflags="-X $pkg.Version=$version -X $pkg.Storage=$storage -X $pkg.GoOsArch=$go_os_arch"
ldflags=$ldflags" -X $pkg.GoVersion=$go_version -X $pkg.GitSHA=$sha -X $pkg.Date=$date"
export ldflags
