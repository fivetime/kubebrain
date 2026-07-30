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

BIN_NAME="kube-brain"
BIN_DIR="./bin"
WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

cd "$WORK_DIR"
mkdir -p "$BIN_DIR"

storage=TiKV
source "$WORK_DIR/build/build-base.sh" "$storage"

CGO_ENABLED=0 go build -trimpath --tags tikv -o "$WORK_DIR/$BIN_DIR/$BIN_NAME" -ldflags "$ldflags" ./cmd
