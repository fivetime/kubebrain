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

func TestDBaaSCompatibilityPlanKeepsColdCSIIncompleteAndRejectsTiDBPITRSubstitution(t *testing.T) {
	plan := readDBaaSCompatibilityPlan(t)

	require.Contains(t, plan, "cold CSI")
	require.Contains(t, plan, "CSI 多 PVC 恢复仍是独立未完成演练")
	require.Contains(t, plan, "pinned v7.5.1 受限链已完成功能闭环")
	require.Contains(t, plan, "不得用 TiDB BR")
	require.Contains(t, plan, "替代 KubeBrain exact receipt 与最终语义门禁")
}

func TestDBaaSCompatibilityMatrixDescribesBoundedPDIsolationReads(t *testing.T) {
	plan := readDBaaSCompatibilityPlan(t)

	require.Contains(t, plan, "受 GC safepoint 保护的 serializable checkpoint")
	require.Contains(t, plan, "Range、read-only Txn 与 RangeStream")
	require.Contains(t, plan, "Region merge、store replacement/address change")
	require.NotContains(t, plan, "serializable Range 不具备 upstream 本地 applied backend 的隔离成员可读性")
}

func TestDBaaSReadinessDescribesRestrictedNativePITRAsFunctionallyClosed(t *testing.T) {
	readiness := readRepoDocument(t, "docs", "production_readiness_cn.md")
	audit := readRepoDocument(t, "docs", "audit_remediation_todo_cn.md")

	require.Contains(t, readiness, "受支持的受限 native PITR 链已完成功能闭环")
	require.Contains(t, readiness, "`pitr_complete=true`")
	require.NotContains(t, readiness, "仍需实现和验证的生产闭环包括")
	require.Contains(t, audit, "native PITR 功能闭环已完成，剩余为生产规模化验证")
	require.NotContains(t, audit, "task/safepoint 生命周期、full+log manifest、transactional restore 及指定时间点语义验收尚未闭环")
}

func TestDBaaSReadinessRequiresFullBackupEncryptionAttestation(t *testing.T) {
	readiness := readRepoDocument(t, "docs", "production_readiness_cn.md")
	plan := readDBaaSCompatibilityPlan(t)

	for _, evidence := range []string{
		"docker build --target native-pitr-full-backup",
		"/usr/local/bin/kubebrain-native-pitr-full-backup",
		"kubebrain.native-pitr-full-backup-attestation.v3",
		"--crypter.method=plaintext",
		"kubebrain.native-pitr-full-artifacts.v5",
		"不能再用 `backupmeta.cipher_iv` 推断加密状态",
	} {
		require.Contains(t, readiness, evidence)
	}
	require.Contains(t, plan, "A4532 关闭 native PITR full backup")
	require.Contains(t, plan, "A4533 补齐 A4532 只有 attestation schema/consumer")
	require.Contains(t, plan, "A4534 补齐 A4533 的可信 producer 仍只能通过源码 `go run`")
	require.Contains(t, plan, "A4535 关闭 A4534 明确保留的 native PITR full backup 控制面编排缺口")
	require.Contains(t, plan, "共同支持 `NativePITRFullBackup`")
	require.Contains(t, plan, "A4536 关闭 A4535 executor 要求 `<operation>-parameters`")
	require.Contains(t, plan, "A4537 关闭 A4536 仍要求操作者手工计算摘要")
	require.Contains(t, plan, "A4538 关闭 operation audit admission 只要求“包含”归档 finalizer")
	require.Contains(t, plan, "A4539 开始关闭 native PITR 只支持 plaintext 的加密备份缺口")
	require.Contains(t, plan, "A4540 关闭 A4539 的 durable `NativePITRFullBackup` Operation")
	require.Contains(t, plan, "A4541 完成固定 BR/TiKV/PD v7.5.1 的真实 AES-256-CTR")
	require.Contains(t, plan, "A4542 关闭 A4541 保留的“错误密钥只有合成失败")
	require.Contains(t, plan, "不证明历史 MVCC/raw key 完全无变化")
	require.Contains(t, plan, "A4543 将 A4541/A4542 的 AES 正向与错误 key 写前安全证据从单副本提升到对称三副本")
	require.Contains(t, plan, "fully replicated 无故障基线")
	require.Contains(t, plan, "A4544 关闭 A4543 明确保留的“加密恢复只证明 fully replicated 无故障基线”")
	require.Contains(t, plan, "不证明双 store quorum loss")
	require.Contains(t, plan, "A4545 继续关闭 A4544 保留的加密 import 窗口 store quorum loss 缺口")
	require.Contains(t, plan, "target-two-store-quorum-loss-during-br-resume")
	require.Contains(t, plan, "A4546 关闭 A4545 明确保留的加密 import 窗口 PD quorum loss 缺口")
	require.Contains(t, plan, "target-two-pd-quorum-loss-during-br-resume")
	require.Contains(t, plan, "底层 BR 是否在恢复前退出")
	require.Contains(t, plan, "不能把测试框架等待恢复的时间冒充")
	require.Contains(t, plan, "A4547 把 A4546 的双 PD process pause 提升为加密 import 窗口内的真实 TCP 网络 quorum loss")
	require.Contains(t, plan, "target-pd-network-quorum-loss-during-br-resume")
	require.Contains(t, plan, "证明失败来自仍运行进程的 Raft 无多数派")
	require.Contains(t, plan, "A4548 开始关闭 A4547 保留的 restore-side durable Operation 缺口")
	require.Contains(t, plan, "NativePITRFullRestore")
	require.Contains(t, plan, "第二 claim 只复算该 receipt")
	require.Contains(t, plan, "绝不在可能部分 import 的目标上自动二次执行")
	require.Contains(t, plan, "A4549 补齐 A4548 留下的可部署执行面和 receipt-after-crash 证据")
	require.Contains(t, plan, "restore 调用日志精确只有一次")
	require.Contains(t, plan, "receipt-before-publish 窗口仍")
	require.Contains(t, plan, "不冒充尚未实现的加密备份密钥托管与恢复能力")
}

func readDBaaSCompatibilityPlan(t *testing.T) string {
	return readRepoDocument(t, "docs", "dbaas_compatibility_plan_cn.md")
}

func readRepoDocument(t *testing.T, path ...string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	planPath := filepath.Join(append([]string{repoRoot}, path...)...)
	data, err := os.ReadFile(planPath)
	require.NoError(t, err)
	return string(data)
}
