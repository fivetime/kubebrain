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
- 后续按现有 CI 的四分片方式完成整个 production 包：745 个顶层测试分配为
  180 / 204 / 188 / 173 个，各分片均退出 0，耗时分别为 538.176 / 599.549 /
  423.657 / 802.911 秒。最后一个分片于 2026-09-17 00:00 UTC 确认终态。
  使用 `bash hack/production/test-shard.sh --verify 4` 检查分配，再运行索引 0–3；
  每片保持脚本既有 15 分钟超时，不裁剪用例。分片本身不带 race，不能据此宣称
  全包 race 通过。测试源码为 `8ac67ca3b21423e3d87c3c882850ce655eb23dd9`，
  运行期间只有文档改动，产品及测试源码未变。
- 同一源码的 `go test -race ./build ./hack/production/internal/imageprepull
  ./hack/production/cmd/image-prepull -count=1 -timeout=5m` 通过，各包耗时
  2.573 / 5.027 / 6.499 秒。这不代替已发布镜像字节和来源校验。

## 未通过或未完成的全仓项

`go test ./... -count=1 -timeout=15m` 最终退出 1：`hack/production` 包累计耗尽
900 秒预算。超时时该包正在执行 BoundsRuntimeEvidence（11 秒）/probe-log（5 秒），
没有依据认定这个用例自身卡死；后续单独验证通过也**不等于整个 production 包通过**。
不删除这次失败记录，也没有延长产品请求/故障恢复验收门限。
整个包随后通过的是上述四分片运行，不是重写此前单包超时为成功。

全仓 ShellCheck 退出 1。本轮新增 helper 的 SC2154 属于调用脚本提供的 baseline
变量，已补充明确说明；剩余诊断涉及 backend-integration、backup 和 production
脚本，所有这些文件均核对为与 origin/dbaas 一致。部分诊断涉及故意使用十进制
字符串规避整数溢出的比较，不能未经语义核对就照建议改成整数比较。既有告警尚未
整体消除，不能将“增量无告警”写成“全仓 lint 通过”。

当前常规 CI 的 Test all packages 步骤明确排除 `hack/production` 包，但另有
`production-test-shards` 作业，使用已有 `hack/production/test-shard.sh` 运行全部
四个分片。前述单包超时是本次本地验证未采用已有分片方式，不能据此声称常规 CI
没有覆盖这个包。分片按测试名 SHA-256 分配，无手工用例白名单。
dbaas 自动触发的是独立 self-hosted 镜像流程及路径匹配的 probe regression。必须分别
记录各流程结果，不能用镜像构建成功替代完整测试或运行时验收。

## 证据和提交边界

证据目录 `/root/.local/state/kubebrain/pre-ci-validation.jMLuRJRb/`，包含
root-tests、compat-tests、vet、shellcheck、changed-shellcheck、production-focused
的日志和退出码，以及接续状态记录。全包超时后检查未发现遗留 bash/sleep 子进程。
四分片日志和退出码保存在 `/root/.local/state/kubebrain/production-shards.2c0Lvg9l/`，
四个原始会话均已收取终态，不再重复启动。

截至 2026-09-17 00:00 UTC，精确源码的镜像 CI `35163623338` 和探针回归 CI
`35163623307` 均仍在运行；不能把本地测试通过写成 CI 完成或新镜像已部署。

00:02 UTC 后续核验：探针回归 CI `35163623307` 已 completed/success，head SHA
与上述源码完全一致。workflow contracts、etcd service/Watch、follower proxy、
全探针 race 步骤均 success。镜像 CI 当时仍在构建，镜像校验和部署尚未执行。

00:15 UTC 后续核验：镜像 CI `35163623338` completed/success（29m10s），
独立发布审计退出 0。审核的 index 为
`sha256:99535eb62b41d78de573c1d7162a20946424d33a4aa30e8324a3ef2adaf644a7`；
精确源码 tag 与 dbaas tag 的摘要一致，原始 index 字节摘要及 amd64/arm64 描述符
验证通过，实际 amd64 binary 的版本、源码、构建时间、fork 客户端和 gRPC 模块
符合预期。审计提取的 binary 及临时容器已清理。证据目录：
`/root/.local/state/kubebrain/release-8ac67ca3.wQWBaSiN/audit.tfRXNJze/`。
隔离实例升级的只读预检通过；这仍不代表升级或回退运行时验收完成。

用户已有的 `docs/dbaas_acceptance_status_cn.md` 未提交改动保持原样，不纳入本轮
提交或推送。新增代码差异未发现私钥 PEM 标记；这仅是一项检查，不冒充完整秘密扫描。
允许将已验证增量推送到 dbaas 触发测试镜像 CI，但不据此自动部署到集群。
