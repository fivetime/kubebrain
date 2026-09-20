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

在 `408c86d6` 按标准 CI 包集合（`go list ./...` 排除唯一的
`/hack/production` 包）启动全仓库非 production 回归，清除测试子进程的
`KUBEBRAIN_*` 环境变量。日志位于 `nonproduction-full-suite.dieGr3nV`。
该轮已发现 `deploy/test-cluster` 的 `TestObserveLocalNonces/agent-restart`
失败：启动刚写入的 fake kubectl 生成初始 Pod 夹具时，`exec` 返回
`text file busy`，尚未进入观察器和 agent 重启断言。不能将该失败解释为
真实集群重启观察失败，也不能把该轮记为全仓库通过。

修正仅让该初始夹具由 Bash 读取执行，避免直接执行新写入的脚本 inode；
观察脚本、故障注入场景及全部断言不变。`TestObserveLocalNonces` 连续
20 次通过，耗时 53.813 秒。已另启该包完整复测并保存 JSON 日志，
结果仍待收取；局部复测不能抹去原全仓库失败。

上述非 production 全仓库回归现已结束，退出 1；逐事件结果中唯一失败
包为 `deploy/test-cluster`，唯一失败叶子用例为上述
`TestObserveLocalNonces/agent-restart`。最后结束的包通过不代表整条命令
通过，应以保存的 `exit-code` 及所有包终态判断。修正后的该包完整复测
仍在独立执行，不能将两次不同源码状态的结果合并成一次全仓库通过。

修正后的 `deploy/test-cluster` 整包复测已退出 0，包级 pass，耗时
96.321 秒，日志目录 `test-cluster-fixture-fixed.u1jsV4NU`。随后在包含
夹具修正的统一源码上重新启动同一非 production 全仓库命令，结果待收取。

该统一源码复测（`e235bd5d`）现已完成，退出 0：149 个包以 pass 结束，
没有失败包；日志、源提交及退出码在 `nonproduction-full-fixed.v1qg6iZ0`。
包集合通过 `go list ./...` 排除唯一的 `/hack/production` 包，命令使用
`go test -json -count=1 -timeout=10m`，仍清除 `KUBEBRAIN_*` 环境变量。
这是一轮完整的非 production 包集合回归，不是完整 production 或在线验收。

八组测试前四组已全部退出 0：第 0/1/2/3 组分别耗时
269.545/254.324/293.848/620.723 秒；后四组仍在运行。相较此前四分组下
第 3 组的 899.733 秒，新分组已有实际耗时改善，但八组全部通过尚未证明。

`440e0ad29c670f439b802a419b0ceaa6025c08ec` 的回归 CI `35484696403`
与镜像 CI `35484696401` 均成功。该 CI 提交不包含后续嵌套事务修复和
八分组配置，不能将其成功结论应用于后续提交。

## 八组完整回归终态

`408c86d6` 启动的八组 production 回归已全部完成，八个分组退出码及
总退出码均为 0。证据目录 `production-eight-shards.bCkUkgzw` 保存源提交、
每组原始日志和退出码。分组 0 至 7 的耗时依次为：
269.545、254.324、293.848、620.723、318.486、400.587、148.213、
288.403 秒。完整发现的 775 个顶层用例仍按 SHA-256 取模分配到唯一分组，
未删减失败用例，未增加每组 15 分钟预算。

最慢组 620.723 秒，相比预算有约 279 秒余量；这是本机此次执行结果，
不保证不同 Runner 的相同耗时。此验证仅覆盖 production 包；上述统一
源码非 production 回归与 etcd 全包竞态结果分别保留其来源和执行范围。
它们都不证明真实故障实验入口完整，也不代表原 30 秒在线验收通过。

## 后续 production 命令包竞态回归

在 `1c6f118a` 对全部 50 个 `hack/production/cmd/...` 包执行
`go test -json -race -count=1 -timeout=5m`，结果退出 1。39 个包通过、
10 个包无测试，唯一失败包为 `rollout-availability-probe`，在 300.383 秒
达到累计包级超时。现场为客户端 TLS 快照恢复中使用证书管理员的子用例，
该子用例当时仅执行 22 秒；没有已报告的其他用例断言失败。证据目录
`production-commands-regression.tekzvx9W` 保留提交号、包清单、JSON 输出
及退出码，不将局部通过合并成整轮通过。

检查 `.github/workflows/probe-regression.yml` 确认该探针包原有完整竞态
入口使用 `-timeout=20m`。上面的本地统一 5 分钟不是 CI 的既有预算，
不能据此断言生产代码死锁。现以同一源码、完整 50 包集合和原 CI 的
20 分钟预算重新运行竞态验证，证据目录为
`production-commands-ci-budget.T02ncEpy`；结果仍待收取。没有修改测试
代码或断言，没有改变真实故障的原 30 秒门限。

上述同一源码（`1c6f118a1fb02512c8d1202d338818dfde1eac0f`）的完整
50 包竞态重测现已退出 0：40 个包 pass，10 个包无测试文件，没有失败。
`rollout-availability-probe` 终态耗时 340.898 秒，超过先前自行设置的
5 分钟，但低于该包现有 CI 的 20 分钟预算。旧超时记录保留，这一结论
仅覆盖 production 命令包集合，不是最新全仓库或真实故障验收通过。

已推送源码 `30fe197ab845ed494e7490ecffbef8b1628d12c8` 的回归 CI
`35487555733` 与镜像 CI `35487555716` 均成功；镜像构建推送、发布后
校验和 dbaas 标签提升步骤均通过。完整终态证据分别位于私有目录
`ci-35487555733-terminal.YYjTRZ3j` 与
`ci-35487555716-terminal.zJCD41na`，含 run/jobs JSON、日志及摘要清单。
该 CI 提交不含随后本地新增的留存、Join、实时进程核对及指标映射修正，
不能把其成功结论应用于待推送修改。测试集群未部署此镜像。

随后源码 `dc2025a9f8db1c92e7d11d2a39778cdb9e6f366c` 的回归 CI
`35488997898` 与镜像 CI `35488997885` 均成功。终态证据分别归档至
`ci-35488997898-terminal.GOA8nj0p` 与
`ci-35488997885-terminal.PVWB1BZ6`，含源提交核对、run/jobs JSON、完整
日志及 SHA-256 清单。该版本包含此前留存、Join、实时进程检查组件及
指标 Pod 映射修正，但不包含其后本地的准入接线、组合入口、统一来源
复核及资源准备；后续修改仍需新的 CI，测试集群未部署该候选版本。
