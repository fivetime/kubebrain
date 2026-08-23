// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataplaneProductionBuildsExcludeForeignStorageAndRaftImplementations(t *testing.T) {
	for _, buildTag := range []string{"tikv", "badger"} {
		t.Run(buildTag, func(t *testing.T) {
			out, err := exec.Command("go", "list", "-tags", buildTag, "-deps", ".").CombinedOutput()
			require.NoError(t, err, string(out))
			dependencies := strings.Split(string(out), "\n")
			for dependency, reason := range map[string]string{
				"go.etcd.io/etcd/server/v3/storage/backend":         "upstream bbolt registers colliding process-global etcd metrics",
				"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp": "KubeBrain uses TiKV/PD and must not expose an upstream Raft transport",
				"go.etcd.io/etcd/client/v3/leasing":                 "the experimental client cache is a consumer library, not dataplane code",
			} {
				require.NotContains(t, dependencies, dependency, reason)
			}
		})
	}
}
