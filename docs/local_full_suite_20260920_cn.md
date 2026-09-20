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

四组完整验证结果（日志目录 `production-shards.5JTpA2TZ`）：分组校验
确认 775 个顶层用例分别分配为 188、211、192、184 个。第 0、1、2 组
退出 0，分别耗时 638.140、648.464、435.064 秒。第 3 组退出 1，
在 900.024 秒达到原有 15 分钟包级预算；现场为
`TestValidateInstanceReady`（58 秒），其
`client_EndpointSlice_changes_during_validation` 子用例当时执行约 1 秒。
因此四组验证仍未全部通过，不能以三个成功组代替完整结果。尚需收集
逐用例耗时，区分分组累计时间与具体用例问题；未延长现有预算。

超时现场 `TestValidateInstanceReady` 单独使用 3 分钟包预算复测通过，
包耗时 106.202 秒，顶层测试 106.180 秒；219 个子用例均通过，最慢
子用例 `tls_release_baseline` 为 1.250 秒。逐事件 JSON 和退出码保存在
`ready-test-profile.PVvZ2XEW`。这支持累计耗时的判断，但尚不能证明
第 3 组能在预算内完成。该组另以原 184 个用例、原 15 分钟预算采集
逐用例 JSON 日志（`shard3-profile.X0Au36mP`），结果仍待收取。

该组逐用例日志发现 `TestRolloutAvailabilityRunnerRejectsCleanupBoundToDifferentReceipt`
通过但耗时 60.760 秒。检查确认：恢复的 owner receipt 引用一个已不存在的
probe，而 fake kubectl 在无 probe 状态文件时，即使收到 `--ignore-not-found`
也返回 1；退出清理将其视为读取失败，重试到默认 60 秒期限。这不是已证实
的生产 API 故障，也不应通过缩短生产超时掩盖。

夹具现对缺失 probe 的 `get --ignore-not-found` 返回空输出和成功，普通
`get` 仍失败，显式配置的读取故障仍在缺失判定前生效。回执不匹配用例改用
已有的进程组超时助手（10 秒外层保护），并断言不会出现 probe 删除失败；
仍验证拒绝错误回执、不消费 cleanup 日志且保留 owner receipt。
该用例连续三次通过，总耗时 2.829 秒。生产脚本、包级测试预算和原 30 秒
真实故障验收门限均未修改；完整分组结果仍未证明通过。

后续该用例竞态检测连续三次通过（3.898 秒）；相关回执、cleanup、瞬时
删除失败、非所属 probe、已所属 probe 及 probe 删除失败场景合并复测
通过（58.749 秒），命令为：

```sh
go test -count=1 -timeout=3m ./hack/production -run '^TestRolloutAvailabilityRunner.*(Receipt|Cleanup|Transient.*Delete|UnownedProbe|OwnedProbe|ProbeDeletionFails)'
```

原始第 3 组耗时采集已终止：仍在 900.026 秒达到包级预算，退出 1，
完整 JSON 与退出码位于 `shard3-profile.X0Au36mP`。该进程在夹具修正前
编译启动，因此仍包含 60.760 秒的旧回执用例，不能用于评判修正后分组。
其中 `TestValidateDataplaneReadonlyProbe` 的 433 个子用例全部通过，
顶层耗时 310.730 秒；超时现场再次为 `TestValidateInstanceReady`。
这支持累计耗时因素，但不能证明所有剩余用例正确或修正后预算足够。

此前 `ed20263b77468c9eda2597643794e10b13e8c113` 的回归 CI
`35483393491` 和镜像 CI `35483393445` 均成功，终态日志分别归档至
`ci-35483393491-terminal.7FweZeS1`、`ci-35483393445-terminal.5U9OuN3O`。
这些工作流的成功不抹去本地完整 production 分组失败。

修正后的同一第 3 组（原 184 个顶层用例）复测退出 0，包级终态 pass，
耗时 899.733 秒，日志和退出码在 `shard3-fixed.s9QhuiwA`。回执不匹配
用例此次为 0.950 秒。此前失败记录保留；这不是同一提交下重新执行全部
四组的结果，更不是全仓库或真实故障验收通过。

该组仅比 900 秒预算低约 0.27 秒，不具备稳定 CI 的余量。标准 CI 的
production 矩阵因此由 4 组拆为 8 组：沿用完整发现后的 SHA-256 取模分配，
不增删用例、不引入排除列表，单组仍是 15 分钟，作业仍是 20 分钟。
同步修改矩阵及分组覆盖回归测试。八组完整执行结果尚待验证；配置检查
通过不能替代八组测试通过。

分组配置回归通过（7.958 秒）；`test-shard.sh --verify 8` 确认全部
775 个顶层用例分配到八个非空分组，数量为 108、107、101、101、80、
104、91、83，总数不变。随后按新配置启动完整八组验证，本地最多同时
运行四组，保存各组日志和退出码；不将仍在执行的组记为通过。
