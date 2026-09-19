# 退让通知预算修正的 CI（2026-09-19）

候选 `198463901c577866024d0dc73c40b52726ddd604` 已推送至远端 `dbaas`。
推送包含已提交的产品修复、工具回归与实验记录，不包含本机未提交的
`docs/dbaas_acceptance_status_cn.md`。预算修正的本地验证与限制见
[故障实验后续代码检查](peer_fault_anonymous_rearm_20260919_cn.md)。

下列各触发一次，读取 API 均确认 head_sha 为上述候选；当前状态仅为
记录时快照，后续须轮询原 run ID，不重复触发：

- [镜像构建 35420491977](https://github.com/fivetime/kubebrain/actions/runs/35420491977)：push 触发，已成功完成。
- [探针回归 35420491991](https://github.com/fivetime/kubebrain/actions/runs/35420491991)：push 触发，已失败。
- [后端协议集成 35420501469](https://github.com/fivetime/kubebrain/actions/runs/35420501469)：手动触发一次，已成功完成。

后端 workflow 与全部 job 的终态及候选 SHA 经独立 API 复查，真实协议/
race、中断启动清理等步骤均成功，证据保存在私有
`peer-budget-ci.zmNqdVeq/backend-terminal.whzAZTcY`。它不替代镜像/
探针 CI 各自的结果，也不证明专用集群的 30 秒故障验收通过。

镜像 workflow 与全部 job 的成功终态、候选 SHA 经独立 API 复查；构建、
发布后校验与标签提升步骤均成功。私有证据为
`peer-budget-ci.zmNqdVeq/image-terminal.UzXhEOLL`。探针 CI 仍失败，
因此这个镜像不具备集群准入条件。

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
这是全包总运行预算，不是改变任何单用例或集群验收门限。会话现已
终态退出 1：在 1800.052 秒触及包级总超时，718 个顶层测试通过；
ColdRestoreExecute 已通过（17.43 秒）。中断时
TestValidateDataplaneReadonlyProbe 已运行约 2 分 16 秒，未见用例
断言失败，但本次完整命令失败，不能计为全包通过。原始证据哈希已复核。

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

隔离验证期间未改动主工作树测试源码。待全量工具回归与原镜像 CI
均终态后，修正已合入主工作树为 `1b1e8d01`，并确认部署测试包、
工具测试包、build 与模块依赖均与隔离验证版本一致。旧候选的探针
CI 仍为失败；后续须使用整合后的候选重新验证，未启动新集群实验。

隔离修正提交 `1909d7127ea9a37dee7063818460e5c7af017f37` 的补充全包
验证已完成：`go test -race -count=1 -timeout=5m ./deploy/test-cluster
./build` 两包均通过（168.194 / 2.519 秒，session 11987 终态 0）。
没有使用用例过滤或跳过原 CI 失败用例；主工作树的全量工具测试未被
修改。此结果不将旧候选失败 CI 转为成功，后续仍需整合后的 CI 验证。

从同版本枚举的 760 个顶层测试减去原始 JSON 中 718 个顶层通过项，
生成剩余 42 项（含中断的 TestValidateDataplaneReadonlyProbe，完整
重跑其所有子用例）。补充测试清单与源码绑定保存于私有
`production-remaining-regression.IvGDFpmp`。后续即使补充通过，也只
能报告分批覆盖结果，不能把原包级超时改写为单次全包通过。

## 整合候选的独立验证

整合候选 `2fd00721b2c95e8dd7708b5d1e400a852873b6f7` 已推送；
以下 run 的 API head_sha 均独立核验与候选一致，各仅触发一次：

- [镜像构建 35422204061](https://github.com/fivetime/kubebrain/actions/runs/35422204061)：push 触发，已成功完成。
- [探针回归 35422204070](https://github.com/fivetime/kubebrain/actions/runs/35422204070)：首次尝试失败；第二次尝试已成功完成，详见下文。
- [后端协议集成 35422215771](https://github.com/fivetime/kubebrain/actions/runs/35422215771)：手动触发，已成功完成。

补充本地回归会话 94729 已终态退出 0（736.547 秒），使用精确 42 项
顶层名称选择表达式、race、count=1 与 20 分钟包级预算，不过滤其
子用例。42 项全部通过，包含上次中断的只读探针完整测试（294.73 秒）。
证据摘要已独立复核。私有目录记录源码 `1b1e8d01`；与候选间仅有
文档提交，已核验工具及模块源码一致，且与原候选 `19846390` 的工具
包和模块文件仍一致。两个 JSON 测试结果的顶层通过名称并集为 760，
与枚举清单作差为空：这是 **760 项分批通过覆盖**，不是单次全包通过。

后端 CI 的候选 SHA、workflow 和全部 job 成功终态经独立 API 复查；
私有证据为 `corrected-candidate-ci.C6iAqJeq/backend-terminal.NCz8Acz4`。
镜像 CI 已成功，探针 CI 已失败，因此尚无新一轮集群准入结论。本轮未部署或
修改集群；不会把旧候选的 CI 结果套用于当前候选或重复触发 CI。

镜像候选 SHA、workflow 与全部 job 成功终态已独立 API 复查；日志中
发布后镜像的 Git SHA 与候选一致。API、作业及完整日志摘要保存于
`corrected-candidate-ci.C6iAqJeq/image-terminal.4ZqsmsR0`。仍须完成
探针失败调查与后续准入，不能仅凭镜像 workflow 成功部署。

### 整合候选的探针终态失败

原监控会话 43113 已终态退出 1，run 与候选 SHA 独立 API 复查一致。
上次失败的传输/凭据步骤本次成功，相关阶段证据保存于私有
`corrected-candidate-ci.C6iAqJeq/probe-transport-stage.mrzcKwUf`。
最终失败位于 `Race test all probe regressions`，包耗时 348.430 秒，
不是该命令的 20 分钟包级超时，也没有据此归因于 Runner。

`TestRestoredAuthOfficialConcurrentStreamRefresh` 在第 8 轮
KeepAliveOnce 断言处收到 `context deadline exceeded`，最近认证 RPC
为 DeadlineExceeded；测试共享客户端并发建立 Watch 和 KeepAlive，
每轮原上限为 5 秒。此前跨所有成员对的原始 RPC 测试已通过，但不能
替代这个失败。日志/API/摘要证据为
`corrected-candidate-ci.C6iAqJeq/probe-failure.waKTM0rU`。

保持原源码及单轮门限，启动该用例三次 race 复现（包级 5 分钟），
会话 30045，私有目录 `stream-refresh-ci-repro.0VkOGeGE`；现已终态
退出 0，三次均通过（150.208 秒）。这表明本地定向运行尚未复现该
CI 失败，不证明其原因或已修复。未重触发 CI、放宽断言或部署候选。

进一步按 CI 相同参数运行完整探针包：`go test -race -count=1
-timeout=20m -v ./hack/production/cmd/rollout-availability-probe`。
会话 20464，私有目录 `stream-refresh-full-probe.drl79mJW`；启动前
已确认包源码与模块文件相对候选无差异。现已终态退出 0，完整包通过
（306.098 秒），其中并发认证刷新用例通过（49.41 秒）。摘要已复核。
此检查覆盖相同包内前序测试影响，但不能单独证明 CI 运行负载是根因。

在原用例三轮与完整包均通过、没有改动源码和门限的前提下，仅对
失败的探针 run 执行一次 `gh run rerun 35422204070 --failed`。
命令会话 33318 退出 0，独立 API 确认 run_attempt=2、状态 queued、
head_sha 仍为 `2fd00721b2c95e8dd7708b5d1e400a852873b6f7`。
镜像和后端 CI 未重跑；首次失败证据保留。第二次尝试是受控复验，
不是已经修复的证明，也不采用反复重跑直到变绿的策略。等待该次终态，
本轮未部署集群或推送另一个候选。

### 独立镜像仓库核验

不部署集群，直接按不可变摘要读取仓库元数据：原始索引文件的 SHA256
与引用一致，Linux 平台恰好为 amd64/arm64，按仓库内选择器解析得到：

- index：`sha256:f8e6cbcb8e5102dca059fedc9d4aba177a220d318816fc5c987c8679251148f9`
- amd64：`sha256:e0a949b80ba094c856c8154f14fe0e6fd4168d96af3e5874b51255c7e091fcee`
- arm64：`sha256:2d3c275f1d9e1a3df38e26bf0925e5f89af395b87e6e75a1664a592533538c55`

两个子镜像的架构、Linux OS、源码 revision 均匹配候选，运行用户均为
`65532:65532`。证据为私有 `corrected-candidate-ci.C6iAqJeq/registry-audit.y5TTDdIL`。
这是独立元数据核验，不是本机执行二进制或集群拉取/准入证明。探针
第二次尝试当前已开始运行，尚无成功终态。

后续本机 amd64 子镜像执行核验通过（会话 49780 退出 0）：按上述
不可变子摘要拉取，以禁网、只读根文件系统、cap-drop ALL、
no-new-privileges 和资源限制运行 `kube-brain version`，输出源码
SHA 匹配候选、Storage 为 TiKV。使用 `--rm`，退出后按精确镜像
复查没有残留容器。日志及摘要保存于私有
`corrected-candidate-ci.C6iAqJeq/runtime-image-audit.XtAaNhi6`。
本机镜像缓存保留；未挂载测试集群凭据或访问集群，仍不构成集群准入。

### 下一轮的本机准备（保持 HOLD）

独立目录 `fault-peer-budget.mjraWKki` 从候选 `2fd00721` 生成源码
归档和双源码树，未复用旧实验的证书、执行凭据或已消费状态。
会话 35822 已退出 0，从该源码编译六个辅助工具（image-prepull、
info-diagnostic-probe、lease-term-probe、peer-control-probe、
peer-retirement-test-pki、uid-delete）；使用 `-mod=readonly`，编译
前后源码与归档逐项比较通过，六个二进制摘要均已验证。编译器版本
单独记录。本机准备没有创建实验 PKI 或运行时执行凭据，没有集群
变更。HOLD 保持，尚需探针第二次尝试成功及全套新基线/准入核验。

### 第二次探针 CI 成功及初步基线复查

监控会话 37066 已终态退出 0；attempt=2 的 API、作业与完整日志独立
核验为成功，head_sha 匹配 `2fd00721`。所有作业步骤成功；先前失败的
并发认证刷新完成全部 20 轮（41.59 秒），完整探针包通过（293.266 秒）。
证据为 `corrected-candidate-ci.C6iAqJeq/probe-attempt2-terminal.KCHMhmwB`。
至此候选的三个 CI 均有成功终态。首次认证超时原因仍未确定，失败
记录保留；此次通过不能证明其根因已修复，也不替代原 30 秒故障验收。

新 owner 的初步只读基线检查 `baseline-check.ZaJQG5Rg` 已通过：
namespace UID 不变；原 StatefulSet UID/spec 不变，generation 与
observedGeneration 为 130、3 Ready/updated；原 peer Secret 的 UID、
resourceVersion、type、data 与前次基线相同。当前本地 PV 为 12 Bound /
86 Released。原始快照及摘要私有保存，未修改工作负载或 Secret。
这只是初步复查，后端身份、证书、完整运行时/恢复准备与最终准入仍
待核验，HOLD 尚未解除。
