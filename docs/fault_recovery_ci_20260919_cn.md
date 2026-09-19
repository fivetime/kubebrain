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
