# 2026-09-20 本地全仓库验证

在 `100fc75f` 上运行 `go test -count=1 -timeout=10m ./...`，测试子进程
清除了 `KUBEBRAIN_*` 环境变量，未启用依赖这些变量的真实 TiKV/PD 实验。
该次验证退出 1，不能记为全仓库通过：

- `hack/production` 在 600.074 秒达到包级超时；现场为
  `TestRolloutAvailabilityRunnerObserveOnlyDoesNotRollStatefulSet`，当时仅
  执行约 4 秒。它随后以 1 分钟包预算单独复测通过（4.749 秒）。该包有
  775 个顶层测试声明，超时原因仍须进一步分析，不能仅按现场用例归因。
- `pkg/server` 失败。单独以 JSON 日志复现仍失败（75.759 秒），明确为
  `TestReloadedControlHTTPRefreshesEveryRequest` 的 rotated、recovered
  分支。完整复现日志保存在私有状态目录
  `server-package-recheck.uW9geAGX/results.jsonl`。

证书轮换测试让七种场景共享突发容量 2 的处理器，每种场景立即请求释放和
发现接口，因而其预期成功依赖令牌在 TLS 握手期间补充。测试夹具容量改为
14，覆盖所有调用的最坏上限，不修改生产限流默认值、认证规则或响应处理。
生产限流拒绝仍由 `TestPeerRetirementHandlerAdmission` 独立覆盖。

修正后轮换测试连续 20 次通过（0.572 秒）；轮换及处理器系列测试连续
3 次竞态检测通过（3.303 秒）。这些局部结果不抹去原全仓库失败，也不
替代后续 server 全包、production 包及真实集群验收。

修正后的 server 全包以同样的 3 分钟预算复测通过（75.631 秒），JSON
终态记录为 `server-package-fixed.DYQFguNs/results.jsonl`。

进一步检查发现，标准 `.github/workflows/ci.yml` 并非把 production 包
放入单次全包测试：它显式排除该包，再通过 `test-shard.sh` 完整分成四组，
每组预算 15 分钟。此前将单次 `go test ./...` 描述为标准 CI 入口不够
准确。原 10 分钟超时仍保留为失败记录；后续应按已有四组入口验证完整
覆盖，而非根据超时现场认定某个用例死锁。该测试预算与真实故障的原
30 秒验收门限无关，后者没有变更。
