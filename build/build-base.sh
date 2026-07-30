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

set -euo pipefail

pkg="github.com/kubewharf/kubebrain/cmd/version"
version="$(git describe --tags --always --dirty 2>/dev/null || printf 'dev')"
sha="$(git rev-parse --short HEAD 2>/dev/null || printf 'unknown')"
go_version="$(go env GOVERSION)"
go_os="$(go env GOOS)"
go_arch="$(go env GOARCH)"
go_os_arch="${go_os}/${go_arch}"
storage="${1:?storage backend is required}"
date="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

echo -e "\033[32m"
echo -e "build env "
echo -e "version   \t"$version
echo -e "sha       \t"$sha
echo -e "go_version\t"$go_version
echo -e "go_os     \t"$go_os
echo -e "go_arch   \t"$go_arch
echo -e "storage   \t"$storage
echo -e "\033[37m"

ldflags="-s -w -X $pkg.Version=$version -X $pkg.Storage=$storage -X $pkg.GoOsArch=$go_os_arch"
ldflags="$ldflags -X $pkg.GoVersion=$go_version -X $pkg.GitSHA=$sha -X $pkg.Date=$date"
export ldflags
