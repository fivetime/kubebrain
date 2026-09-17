# Production 测试发现失败不得通过 CI

2026-09-17 修复 `hack/production/test-shard.sh` 的退出状态传播问题。
原代码使用 `mapfile -t tests < <(go test ... | awk ... | sort ...)`，进程替换
中的失败不会成为 `mapfile` 的退出状态。即使 Go 命令在输出部分测试名后失败，
脚本仍可能把部分列表视为完整列表，并通过 list、verify 或执行分片模式。

先添加回归测试模拟“输出一个合法用例名后退出 17”：三个模式在旧代码上均错误
返回成功，回归测试退出 1。修复改为先用命令替换接收列表，显式检查整个 pipeline
（保留 pipefail）的退出状态和非空结果，成功后才调用 mapfile。
分片哈希算法、用例选择方式和每片 15 分钟时限未改变。

新增三个顶层回归覆盖 Go 部分输出后失败、awk/sort 部分输出后失败、成功但无用例
三类情况；每类覆盖 list、verify、run 模式。Go 失败测试还确认没有启动测试执行。

验证结果：

- `go test -race ./hack/production -run 'Test(ProductionTestShards|CIExcludesMonolith)' -count=1 -timeout=3m` 通过，10.519 秒。
- 真实 `bash hack/production/test-shard.sh --verify 4` 成功，748 个顶层测试，分片为 181 / 206 / 188 / 173。
- bash 语法检查、固定 ShellCheck v0.11.0 warning 门限、git diff 检查通过。

上述最初的 748 仅是发现/分配验证。随后使用修复后的脚本执行全部四个分片，
2026-09-17 00:56 UTC 已收齐四个原始会话的成功终态：

| 分片 | 顶层测试数 | 耗时 | 退出码 |
| --- | --- | --- | --- |
| 0 | 181 | 546.167s | 0 |
| 1 | 206 | 604.271s | 0 |
| 2 | 188 | 428.164s | 0 |
| 3 | 173 | 805.355s | 0 |

分片命令保持 `bash hack/production/test-shard.sh INDEX 4`，各片原 15 分钟预算，
不带 race；不能称为全包 race 通过。源码为 `df1717d41b586940a3cf600298db6fb3d96bf6ba`。
期间另一个提交仅修改嵌套兼容性模块的测试和历史报告，已核对根模块 production
目录与该源码无差异。日志/退出码位于
`/root/.local/state/kubebrain/discovery-regression.ErZjtAj3/`。
此时镜像 CI `35167604378` 仍在运行，不能把本地全量成功写成镜像 CI 完成。

此前 745 个测试的四分片成功证据属于修复前的准确源码，见[发布前验证](pre_ci_validation_20260916_cn.md)。
本修复只加强测试门禁，不改变 KubeBrain 协议、存储或集群部署；不把潜在门禁漏洞
推断成此前真实列表获取失败，已有成功记录不因此被改写。
