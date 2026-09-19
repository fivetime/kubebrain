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
