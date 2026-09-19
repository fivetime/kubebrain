# 故障恢复组件 CI 核验（2026-09-19）

源码 `1e2d228c3ba7505916fe6e3982c4d074e0e3ce6c` 的
[回归工作流 35446396456](https://github.com/fivetime/kubebrain/actions/runs/35446396456)
已完成且成功，probe-tests 作业用时 26 分 30 秒。终态 API、作业列表
与完整日志保存在本机私有证据目录
`/root/.local/state/kubebrain/probe-ci-35446396456-terminal.Rn9g1swz`。
已核对 head_sha、工作流及全部作业的成功终态，并验证 SHA256SUMS。

本次包含持久化协议恢复意图、协议恢复读写及真实 KubeBrain/memkv
服务回归；完整回归中的 race 检查通过。但不覆盖此后本地提交的
网络策略/标签恢复计划以及 `23cfaad8` 的子进程超时恢复组合测试。
这些改动只有本地测试结果，不能借用本次 CI 结论。

同源码的
[镜像工作流 35446396480](https://github.com/fivetime/kubebrain/actions/runs/35446396480)
已终态成功，build-and-push 作业用时 29 分 55 秒，发布镜像校验及
晋级步骤均成功。终态 API、全部作业及完整日志已保存至
`/root/.local/state/kubebrain/image-ci-35446396480-terminal.Owde96rh`，
核对源码与成功状态并通过 SHA256SUMS。此结果仍仅覆盖上述源码，
不覆盖后续本地提交。本次未部署任何候选镜像，也未重跑已退役实验。

后续仍需完整真实故障控制器接入、新源码 CI/镜像核验、独立准入及
真实集群原 30 秒全门限验收。此前真实实验整体超时失败的结论不变，
不能把组件回归成功等同于故障验收或生产就绪。

## e671a7a5 恢复计划与子进程回归

源码 `e671a7a5f2c90c8c9eb2dbfccc4097728bd4d4c3` 的
[回归工作流 35447965018](https://github.com/fivetime/kubebrain/actions/runs/35447965018)
已成功，作业用时 25 分 12 秒。新增的策略删除、标签恢复计划及
真实服务上的子进程超时恢复组合测试包含在此源码中；最终 race
回归通过。终态 API、全部作业和完整日志保存在私有目录
`/root/.local/state/kubebrain/probe-ci-35447965018-terminal.270wsBj4`，
源码、成功状态及 SHA256SUMS 均已核对。

同源码镜像工作流 35447965065 随后终态成功，作业用时 31 分 9 秒，
发布镜像校验及晋级步骤均成功。终态 API、全部作业和完整日志保存在
`/root/.local/state/kubebrain/image-ci-35447965065-terminal.95tEaar1`，
源码、成功状态及 SHA256SUMS 均已核对。归档时本地后续
`2400cafc` 的删除请求 HTTP 传输回归尚未推送，不在这轮 CI 范围内。
该新测试三轮 race 通过，删除工具完整 race 回归也通过（1.901s），
但不能因此认定新源码已获 CI 或镜像验证。当前未进行新的集群实验。

## 0fa3b639 删除请求传输回归

源码 `0fa3b6396d002b283e614d547a44cf4ff90d95b8` 的
[回归工作流 35449612111](https://github.com/fivetime/kubebrain/actions/runs/35449612111)
终态成功，作业用时 26 分 46 秒，覆盖删除请求 HTTP 传输回归及最终
race 测试。终态 API、全部作业和完整日志保存在私有目录
`/root/.local/state/kubebrain/probe-ci-35449612111-terminal.TGZj8044`，
源码、成功状态及 SHA256SUMS 均已核对。

同源码镜像工作流 35449612150 随后终态成功，作业用时 33 分 24 秒，
发布镜像校验、晋级及清理均成功。终态 API、全部作业与完整日志保存于
`/root/.local/state/kubebrain/image-ci-35449612150-terminal.STKtHw2R`，
源码、成功状态及 SHA256SUMS 均已核对。
归档时后续本地 `dc4179df`、`66bad509` 的父进程网络恢复意图及其标签计划
衔接测试尚未推送，不属于本次 CI 验证范围。真实策略创建回执持久化、
完整故障控制器接入和原 30 秒集群验收仍未完成；本轮未修改集群。

## 5930c340 网络恢复意图回归

源码 `5930c34043b4bbcdb4618a232ccb29cf98bb4d42` 的
[回归工作流 35451417137](https://github.com/fivetime/kubebrain/actions/runs/35451417137)
已终态成功，作业用时 24 分 50 秒，包含最终 race 回归。
终态 API、全部作业及完整日志已保存至私有目录
`/root/.local/state/kubebrain/probe-ci-35451417137-terminal.rTHogRs4`，
已核对源码、成功状态及 SHA256SUMS。

该源码覆盖父进程网络恢复意图及其标签计划衔接测试，但不包含后续
`0a5d7f13` 的策略创建回执持久化/有效链接拒绝修复，也不包含
`641a0a1d` 的实际动态客户端预留流程。这两个提交只有本地测试通过，
尚未推送，不得借用本轮 CI 结论。归档时镜像工作流 35451417106 仍在运行。
完整控制器接入及真实集群原 30 秒全门限验收仍未完成，本轮未修改集群。

同源码镜像工作流 35451417106 随后终态成功，作业用时 32 分 58 秒，
发布镜像校验、标签晋级及清理全部通过。终态 API、全部作业和完整日志
保存于 `/root/.local/state/kubebrain/image-ci-35451417106-terminal.PSpInraO`，
源码、成功状态及 SHA256SUMS 均已核验。此结果仍不覆盖上述后续提交；
它们需要独立的新源码 CI 验证，也未在测试集群执行。

## 05c2395d 策略预留与回执持久化回归

源码 `05c2395d75e7d707db6158c95bd8ec753ae5257c` 的
[回归工作流 35453208129](https://github.com/fivetime/kubebrain/actions/runs/35453208129)
已终态成功，作业用时 26 分 12 秒，最终 race 测试通过。
终态 API、全部作业及完整日志位于私有目录
`/root/.local/state/kubebrain/probe-ci-35453208129-terminal.Zw6525BL`，
源码、成功状态及 SHA256SUMS 已核对。

该源码包含策略创建回执持久化、有效符号链接拒绝修复及动态客户端
未激活策略预留流程，不包含后续本地 `ce18c70a` 的条件化策略激活。
归档时镜像工作流 35453208152 仍在构建推送；后续激活代码尚未推送。
组件 CI 成功不等于完整控制器接入或真实集群 30 秒验收通过，本轮未操作集群。

同源码镜像工作流 35453208152 随后终态成功，作业用时 32 分 1 秒，
发布校验、标签晋级与清理全部通过。终态 API、全部作业及完整日志保存至
`/root/.local/state/kubebrain/image-ci-35453208152-terminal.DV8Wsidm`，
源码、成功状态与 SHA256SUMS 均已核对。后续激活代码推送前恢复组件
race 回归通过（5.027s），metricsworker 回归通过（缓存），build 回归
通过（2.639s），vet 和 diff 检查通过；仍需新源码 CI，不能借用本轮结论。

## 733e2f3f 条件化策略激活回归

源码 `733e2f3f85952c53a9865617a03cdd5b482bba65` 的
[回归工作流 35454989798](https://github.com/fivetime/kubebrain/actions/runs/35454989798)
已终态成功，作业用时 25 分 41 秒，包含最终 race 回归。
终态 API、全部作业与完整日志已保存至私有目录
`/root/.local/state/kubebrain/probe-ci-35454989798-terminal.tUf85gpl`，
源码、成功状态及 SHA256SUMS 均已核验。

本轮覆盖 `ce18c70a` 的条件化策略激活及其模拟客户端回归，但不证明
真实 API 准入、Cilium 隔离生效或完整 30 秒故障验收通过。
归档时镜像工作流 35454989738 仍在构建推送。完整控制器接入仍未完成，
本轮未操作测试集群。

同源码镜像工作流 35454989738 随后终态成功。终态 API、全部作业及完整日志
保存于 `/root/.local/state/kubebrain/image-ci-35454989738-terminal.RCnJmtgj`，
源码、全部作业成功状态及 SHA256SUMS 均已核验。
后续新增 `RemoveNetworkPolicy` API 删除恢复执行代码仅完成本地验证：
定向 race 测试重复三次通过，全包 race 通过（5.124s），vet 通过。
该代码不属于上述 CI 的验证范围；完整控制器、Cilium 撤销隔离与真实集群
原 30 秒验收仍未完成。本轮没有修改测试集群。

## 191afe8b 策略删除与不确定激活恢复回归

源码 `191afe8b2ed543a304f68cc82e489c18b86df09a` 的
[回归工作流 35457120500](https://github.com/fivetime/kubebrain/actions/runs/35457120500)
终态成功，作业用时 26 分 59 秒，最终 race 回归及清理均通过。
终态 API、全部作业与完整日志保存于私有目录
`/root/.local/state/kubebrain/probe-ci-35457120500-terminal.0RsgFWfb`，
源码、成功状态与 SHA256SUMS 均已核验。

此源码包含 `RemoveNetworkPolicy` 和动态客户端 HTTP 生命周期回归，
不包含后续本地实时身份检查、标签执行、统一恢复入口和组合观察器。
后续代码尚未推送，不能借用此次 CI 结论。归档时同源码镜像工作流
35457120550 仍在构建推送；完整故障控制器和真实集群原 30 秒验收未完成。

同源码镜像工作流 35457120550 随后成功，作业用时 33 分 15 秒。
终态 API、全部作业和完整日志保存在私有目录
`/root/.local/state/kubebrain/image-ci-35457120550-terminal.zifOr9GO`，
源码、成功状态与 SHA256SUMS 均已核验。此结论仍不覆盖后续本地提交。

### 后续本地全包门限检查：未通过

后续恢复/观察代码尚未推送时，按 CI 原命令执行
`go test -race -count=1 -timeout=2m ./deploy/test-cluster`，两次均在 120 秒
全包超时。首次运行到 DiagnosticDriverRecovery，随后仅将新增隔离用例
并行化，第二次推进到 PeerTrustMaterialCheck 但仍超时。不得将定向回归
成功替代全包通过，也不据此推断上述单个用例死锁；还需定位整体耗时。

并行化后，三个相关观察测试的定向 race 通过（22.082s），vet/diff 检查
通过。断言、CI 的 2 分钟上限和真实故障 30 秒门限均未放宽。后续本地
提交暂不推送；现有远端 `191afe8b` 的成功 CI 不覆盖这些更改。

进一步 JSON 耗时记录保存在私有目录
`/root/.local/state/kubebrain/test-cluster-timing.y3plddfR/events.jsonl`。
记录中 LocalDropAndLabelIdentity 为 47.03s，endpoint 采集为 9.18s。
审核两组用例的独立目录与子进程环境后，改为各最多 4 个用例并行，
不更改断言或用例期限；两组定向 race 合计 15.656s 通过，vet 通过。
再次全包运行仍在 120 秒超时，已推进到 PeerTrustStagePreflight。
该调整只证明局部执行时间改善，不证明全包门限已满足；还需完整耗时
分析，不能将诊断运行或定向结果冒充 CI 全包通过。

### 全包累计耗时修复后通过原门限

仅用于诊断的 5 分钟上限运行在 132.785s 通过；JSON 时间线保存在
`/root/.local/state/kubebrain/test-cluster-full-timing.IBiVY2OU/events.jsonl`。
此结果本身不算原门限通过。时间线显示策略观察约 19s、成员密码身份
预检约 16s、协议制品预检约 14s，未发现单项挂死。

审核这些用例均使用独立临时目录、证书与子进程环境后，三组均调整为
最多 4 个用例并行，保留所有断言。随后重新执行原命令
`go test -race -count=1 -timeout=2m ./deploy/test-cluster`，97.549s 通过；
vet 和 diff 检查通过。CI 超时配置和真实故障原 30 秒门限均未修改。
这解除了后续代码推送前的本地全包验证问题，不代表真实故障验收完成。

## f6daf9d6 完整回归终态

源码 `f6daf9d63443bf392a655ab918d4f164a6cec10d` 的
[回归工作流 35459391683](https://github.com/fivetime/kubebrain/actions/runs/35459391683)
已完成且全部作业成功。终态 API、作业列表和完整日志位于
`/root/.local/state/kubebrain/probe-ci-35459391683-terminal.FF6W9o0A`；
已核对源码、成功终态与 SHA256SUMS。归档时镜像工作流 35459391712
仍在运行，不能宣告该镜像已验证，也不能将本次结果用于后续本地代码。

后续本地准备流程组合测试使用真实 KubeBrain/memkv RPC 和模拟
Kubernetes API，验证策略预留、标签、协议准备及恢复的组合，包括
CREATE 响应丢失、策略替换和租约创建后准入失败。leasefault、metricsworker、
build 的完整 race 回归分别为 7.454s、10.771s、2.640s，vet 通过。
这些结果仍非真实集群故障验收；本轮没有部署镜像或执行故障注入。

同源码镜像工作流 35459391712 随后全部作业成功，终态 API、作业列表
和完整日志已保存至
`/root/.local/state/kubebrain/image-ci-35459391712-terminal.CS7zJbFa`，
源码、终态和 SHA256SUMS 已核验。这仍只覆盖 f6daf9d6，不覆盖后续
本地准备/恢复组合及完整生命周期库入口；新代码需要下一轮 CI 验证。

## 66460de1 生命周期入口回归

源码 `66460de150aa6c25ad8b3cf4c14d78a35881af71` 的
[回归工作流 35461153611](https://github.com/fivetime/kubebrain/actions/runs/35461153611)
已终态成功，全部作业通过。终态 API、作业列表和完整日志保存至
`/root/.local/state/kubebrain/probe-ci-35461153611-terminal.M6rL7sQR`，
源码、终态与 SHA256SUMS 已核对。归档时镜像工作流 35461153617
仍在构建，尚不能宣告对应镜像验证完成。

该源码包含准备/子进程协调/恢复的库入口，不包含此后本地网络观察
回调、操作队列锁释放 fencing、原响应后状态校验及集群侧合作式认领。
认领功能只在模拟 API 中验证，未在测试集群创建对象；最新本地恢复、
子进程、构建模块 race 回归分别为 11.930s、10.787s、2.630s，vet 通过。
本轮没有故障注入或镜像部署，原 30 秒真实集群验收仍未通过。

同源码镜像工作流 35461153617 随后终态成功，全部作业及发布镜像
验证通过。终态 API、作业列表和完整日志已保存至
`/root/.local/state/kubebrain/image-ci-35461153617-terminal.aGBklcbl`，
源码、成功终态与 SHA256SUMS 已核验。该镜像结果仍只对应 66460de1，
不能用于证明后续认领、响应后门限和锁释放修复已通过 CI。

## 8ea1cc52 回归与镜像终态

源码 `8ea1cc5278c921bcdc676d96d88f6ec483869bc8` 的
[回归工作流 35462830397](https://github.com/fivetime/kubebrain/actions/runs/35462830397)
和[镜像工作流 35462830441](https://github.com/fivetime/kubebrain/actions/runs/35462830441)
均已终态成功，全部作业通过。完整日志、终态 API、作业列表分别归档至：

- `/root/.local/state/kubebrain/ci-35462830397-terminal.aDvfaXsa`
- `/root/.local/state/kubebrain/ci-35462830441-terminal.lMkNcI0M`

已核对精确源码、全部作业终态及 SHA256SUMS；目录 0700、文件 0600。
这轮覆盖认领、原响应后门限及操作队列锁释放修复，但不覆盖此后的
HTTP 并发认领测试、恢复命令、恢复命令真实 RPC 测试以及客户端
隐式重试修复（91f82be3、0d5d51e5、a0c47919、78ea8093）。这些提交
将随下一批推送进入 CI，不能借用旧源码的成功结果。

最新客户端修复通过本地 HTTP 对照测试证实：标准 dynamic 客户端会在
Retry-After 响应后重复写请求；故障工具客户端逐请求设置 MaxRetries(0)，
并拒绝 HTTP 重定向。GET/POST/PUT/PATCH/DELETE 在 429、503、307、308
响应下均只发出一次请求。leasefault 完整 race 回归 15.254s，恢复命令
race 回归及两包 vet 通过。返回错误仍可能代表写入已提交，必须核对状态，
不意味着服务端 exactly-once。真实集群仍未执行新故障实验，原 30 秒门限
未通过；本轮 CI 成功不是部署、行为兼容或生产就绪的证明。
