# 退让通知预算修正的 CI（2026-09-19）

候选 `198463901c577866024d0dc73c40b52726ddd604` 已推送至远端 `dbaas`。
推送包含已提交的产品修复、工具回归与实验记录，不包含本机未提交的
`docs/dbaas_acceptance_status_cn.md`。预算修正的本地验证与限制见
[故障实验后续代码检查](peer_fault_anonymous_rearm_20260919_cn.md)。

下列各触发一次，读取 API 均确认 head_sha 为上述候选；当前状态仅为
记录时快照，后续须轮询原 run ID，不重复触发：

- [镜像构建 35420491977](https://github.com/fivetime/kubebrain/actions/runs/35420491977)：push 触发，运行中。
- [探针回归 35420491991](https://github.com/fivetime/kubebrain/actions/runs/35420491991)：push 触发，运行中。
- [后端协议集成 35420501469](https://github.com/fivetime/kubebrain/actions/runs/35420501469)：手动触发一次，运行中。

没有可据此准入的新镜像，也未开始集群实验。仍须全部相关 CI 通过、
镜像源码与多架构摘要核验，以及新 owner 的完整准入/恢复准备。
不能把旧候选 `2ad79751` 的成功 CI 用作此候选的验证。

补充组合回归 `go test -race -count=1 -timeout=3m ./hack/production
./deploy/test-cluster`：部署测试包通过（164.955 秒），实验工具包在
180.055 秒触及包级总超时；当时 `TestColdRestoreExecute` 子用例仅运行
约 1 秒。命令终态为失败，**不是完整工具包通过**，亦不足以定位该
子用例缺陷。这个本地包级超时与原 30 秒集群故障门限不是同一项约束。
后续单独复核中断用例及本批相关工具测试，保留该全包未完成记录。

单独复核已通过（74.188 秒）：同样使用 race、count=1、包级 3 分钟上限，
测试选择为 `^Test(ColdRestoreExecute|ProtectedStackSession|SamePodProcess|Successor|WaitPolicyAbsence|ExpiredLeaseWait)`。
覆盖中断处的 ColdRestoreExecute 与本批相关诊断/观察工具；此结果仍不
替代完整实验工具包的回归，也不替代上述候选 CI 或真实集群验收。
