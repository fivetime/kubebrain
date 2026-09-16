# 2026-09-16 dbaas 增量发布前验证

验证对象：从远端 bb89c3f8 到本地 de56eeea 的增量，包括 KeepAlive 入口取消修复、
TopoLVM 专用测试配置、apiserver 测试安全性/完整性/归档及验收记录。
本报告不代表新镜像已通过 CI 或真实集群升级验收；当前实例仍运行旧固定镜像。

## 已通过

- 根模块 `go vet ./...`。
- 兼容性模块 runner/apiserver/version-matrix 相关 race 测试（18.479 秒）。
- 本轮所有变更 shell 脚本通过固定 ShellCheck v0.11.0 的 warning 门限。
- `git diff --check`、变更产品 Go 文件格式检查。
- 全仓测试中除 `hack/production` 外的根模块包均通过或无测试；使用 `go list ./...`
  与完整测试日志逐项比对，集合完全一致。etcd 服务包用时 145.758 秒。
- 本轮 production 变更涉及的 TiKV/backend 诊断采集、diagnostic phase/sampler
  以及全包超时瞬间正在执行的 `TestRolloutAvailabilityRunnerBoundsRuntimeEvidence`
  单独进行 race 回归，通过。精确命令和耗时保存在 production-focused.log。

## 未通过或未完成的全仓项

`go test ./... -count=1 -timeout=15m` 最终退出 1：`hack/production` 包累计耗尽
900 秒预算。超时时该包正在执行 BoundsRuntimeEvidence（11 秒）/probe-log（5 秒），
没有依据认定这个用例自身卡死；后续单独验证通过也**不等于整个 production 包通过**。
不删除这次失败记录，也没有延长产品请求/故障恢复验收门限。

全仓 ShellCheck 退出 1。本轮新增 helper 的 SC2154 属于调用脚本提供的 baseline
变量，已补充明确说明；剩余诊断涉及 backend-integration、backup 和 production
脚本，所有这些文件均核对为与 origin/dbaas 一致。部分诊断涉及故意使用十进制
字符串规避整数溢出的比较，不能未经语义核对就照建议改成整数比较。既有告警尚未
整体消除，不能将“增量无告警”写成“全仓 lint 通过”。

当前常规 CI 的 Test all packages 步骤明确排除 `hack/production` 包；dbaas
自动触发的是独立 self-hosted 镜像流程及路径匹配的 probe regression。必须分别
记录各流程结果，不能用镜像构建成功替代完整测试或运行时验收。

## 证据和提交边界

证据目录 `/root/.local/state/kubebrain/pre-ci-validation.jMLuRJRb/`，包含
root-tests、compat-tests、vet、shellcheck、changed-shellcheck、production-focused
的日志和退出码，以及接续状态记录。全包超时后检查未发现遗留 bash/sleep 子进程。

用户已有的 `docs/dbaas_acceptance_status_cn.md` 未提交改动保持原样，不纳入本轮
提交或推送。新增代码差异未发现私钥 PEM 标记；这仅是一项检查，不冒充完整秘密扫描。
允许将已验证增量推送到 dbaas 触发测试镜像 CI，但不据此自动部署到集群。
