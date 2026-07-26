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

package etcd

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBaaSCompatibilityPlanKeepsPhysicalRestorePITRIncomplete(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	planPath := filepath.Join(repoRoot, "docs", "dbaas_compatibility_plan_cn.md")
	data, err := os.ReadFile(planPath)
	require.NoError(t, err)
	plan := string(data)

	require.Contains(t, plan, "物理 full snapshot 和日志型 PITR 均未完成")
	require.Contains(t, plan, "仍不替代真实 CSI 隔离恢复演练，也不关闭 PITR 缺口")
	require.Contains(t, plan, "仍不关闭真实 CSI 隔离恢复或 PITR 缺口")
	require.Contains(t, plan, "不得用 TiDB BR full/PITR 的成功状态关闭该缺口")
}
