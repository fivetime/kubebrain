# 退让通知预算修正的 CI（2026-09-19）

候选 `198463901c577866024d0dc73c40b52726ddd604` 已推送至远端 `dbaas`。
推送包含已提交的产品修复、工具回归与实验记录，不包含本机未提交的
`docs/dbaas_acceptance_status_cn.md`。预算修正的本地验证与限制见
[故障实验后续代码检查](peer_fault_anonymous_rearm_20260919_cn.md)。

下列各触发一次，读取 API 均确认 head_sha 为上述候选；当前状态仅为
记录时快照，后续须轮询原 run ID，不重复触发：

- [镜像构建 35420491977](https://github.com/fivetime/kubebrain/actions/runs/35420491977)：push 触发，运行中。
- [探针回归 35420491991](https://github.com/fivetime/kubebrain/actions/runs/35420491991)：push 触发，已失败。
- [后端协议集成 35420501469](https://github.com/fivetime/kubebrain/actions/runs/35420501469)：手动触发一次，已成功完成。

后端 workflow 与全部 job 的终态及候选 SHA 经独立 API 复查，真实协议/
race、中断启动清理等步骤均成功，证据保存在私有
`peer-budget-ci.zmNqdVeq/backend-terminal.whzAZTcY`。它不替代镜像/
探针 CI 各自的结果，也不证明专用集群的 30 秒故障验收通过。

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

完整实验工具包另以 `go test -json -race -count=1 -timeout=30m
./hack/production` 重新运行，未过滤测试或修改断言；枚举有 760 个顶层
测试。私有证据 `production-full-regression.Yn9yRX9d`，会话 54056。
这是全包总运行预算，不是改变任何单用例或集群验收门限。最新轮询仍
存活，506 个通过事件（含子测试）、未见失败；ColdRestoreExecute 已
通过（17.43 秒）。尚未取得全包终态，不能计为全包通过。

## 探针 CI 失败与隔离修正

失败位于传输/凭据检查步骤的 `deploy/test-cluster` 包：
`TestLocalDropAndLabelIdentity/drop//stable` 和 `ready-change` 均退出
127，原因是 `capture-local-cilium-drops.sh` 第 47 行调用了不存在的 `rg`。
日志不是 Runner 未接活，也不是包级超时；其余身份拒绝负例通过不能
掩盖正向流程不可用。私有证据 `peer-budget-ci.zmNqdVeq/probe-failure.Hgbau7yu`。

在独立工作树 `cilium-capture-portability.HOAHeGcZ/source` 以候选源码
复现缺失依赖：测试放置返回 127 的 rg shim，并断言采集不得调用它，
旧实现的两个正向用例均失败。修正改用 `grep -Fxq` 检查同一个完整
字面行，不放松远程 timeout 退出码/显式标记、Pod/agent/CEP 身份或
摘要要求；新增前缀/后缀伪标记拒绝用例。全场景三轮 race 通过
（141.980 秒），Bash 语法、vet 和 diff 检查通过。

该修正尚未合入主工作树或推送，以免修改运行中的全量工具回归源码。
后续需等原任务终态再整合验证；当前候选的探针 CI 仍为失败，即使镜像
构建成功也不具备集群准入条件。没有重触发 CI 或启动新实验。

隔离修正提交 `1909d7127ea9a37dee7063818460e5c7af017f37` 的补充全包
验证已完成：`go test -race -count=1 -timeout=5m ./deploy/test-cluster
./build` 两包均通过（168.194 / 2.519 秒，session 11987 终态 0）。
没有使用用例过滤或跳过原 CI 失败用例；主工作树的全量工具测试未被
修改。此结果不将旧候选失败 CI 转为成功，后续仍需整合后的 CI 验证。
