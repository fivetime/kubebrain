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

随后新 owner 的完整基线采集（session 71692）通过：PD 的精确 uint64
集群 ID 为 `7686251028133611667`，请求前后同一 PD 进程不变；
TiDBCluster UID 匹配，PD/TiKV 均使用 `kubebrain-local-lvm`。
`baseline.initial` 绑定原配置、Secret 与卷快照。

公开证书采集（session 45594）通过：三个 Pod 前后进程身份一致，
挂载证书与原 CA 匹配，health 客户端用途与 info 服务端用途正确，
证书剩余有效期至少 24 小时，原 StatefulSet 不变。首次检查误用了
`.svc.cluster.local` 名称而失败；实际原配置 ServerName 为
`kubebrain-local-info.kubebrain-dbaas-test.svc`，在 SAN 中，重新按原
配置核验通过，未修改证书或跳过名称校验。失败部分证据保留在
`public-tls.VjhD4wHl`，成功摘要为 `diagnostic-identity.sha256`。
探针客户端证书的客户端用途与 24 小时有效期亦通过。HOLD 仍保留，
尚未生成新实验 PKI 或上传 Secret，未进行任何部署/故障变更。

随后已完成本机新 PKI 准备（session 13457 退出 0），新叶证书到期时间
为 **2026-09-20 05:50:13 UTC**，准入前必须重新检查有效期。没有复用
旧实验的 CA/成员密钥；私钥仅保存于受限私有目录。三成员的新旧双根
信任过渡校验、成员身份与两个不可变 Secret 的离线计划检查均通过
（session 45825）。使用 `/dev/null` kubeconfig 和 client dry-run，
没有上传 Secret；计划不包含 CA 私钥，成员文件逐项绑定。

只读检查 `absence.jb5NKjNK` 证明旧/新 owner 对应的临时 Secret、
故障策略均不存在，命名空间 Pod 没有 fault-owner 标签。全部材料
摘要复核通过；HOLD 保持，下一步准备并冻结运行时、恢复和清理流程，
执行完整离线验收后才考虑预拉取及最终准入。

本轮 owner 专属镜像审计（session 18232）通过，生成阶段校验器所需
`image-check/image-audit.json`；重新绑定精确候选、索引/子摘要、三个
成功 CI，并明确保留探针 attempt 1 失败与 attempt 2 成功的区别。
本机镜像配置及隔离 `--version` 校验通过，摘要复查通过。

11 个运行时源码依赖已逐字节与候选 Git 对象比较，再写入
`source-binding.sha256`；包括本次修正后的丢包采集脚本。两个回调
直接取自冻结源码，内容一致、权限 0700，回调准入检查通过。完整
运行时依赖闭包尚未冻结，仍需绑定剩余控制器与计划并运行离线套件。
HOLD 保持，未进行部署、Secret 上传、故障注入或执行凭据消费。

### 新 owner 离线验收与运行时冻结（2026-09-19 06:04 UTC）

已复核此前 `offline-core.IJMJsEBe` 六套测试的终态与摘要：标签准备
4、恢复 8、Secret 清理 13、部署包装器 14、策略计划 10、策略控制器
6，共 55 个用例通过。不存在仍运行的上一轮核心测试进程。

本轮会话 35685 终态退出 0，`offline-integrated.34EdjGAo` 保存集成
故障控制器 27 个及会话重置 3 个用例的通过记录；与核心套件合计
85 个。测试逐套在独立网络命名空间执行，模拟器绑定新 owner、候选
与租约 ID。历史快照仅作为离线夹具，不作为本轮实时准入证据。
等待栈分类器取 owner 中已核验的候选版本，未修改冻结源码树。

会话 53766 退出 0，50 项运行时文件已生成并复核 `tools.sha256`、
`recovery-inputs.sha256` 与 `cleanup-inputs.sha256`；随后在无网络
环境通过信任阶段材料预检，证据 `stage-preflight.KYLf2J1G`。这只
验证离线材料、身份策略及绑定关系，不是集群阶段已执行的证明。

`callback-admission-test.dVEPql9T` 负向检查通过：回调不可执行时，
在集群/GitHub 调用、最终准入目录创建或 HOLD 解除之前拒绝。准入
脚本仅已准备，尚未执行；测试及准备脚本由 `admission-and-tests.sha256`
绑定。会话 38394 退出 0，两个源码树完整 `tar --compare`、运行时
摘要、上述证据摘要和 HOLD/未领取执行凭据状态均复核通过。

本轮未部署候选、上传 Secret 或注入故障。仍需镜像预拉取及精确清理
证明、完整现场准入与恢复准备，之后才能执行原 30 秒/default 2PC
故障验收。不得把本轮离线通过写成生产就绪或真实故障门限通过。

### 固定镜像预拉取与精确清理（2026-09-19 06:06 UTC）

会话 88038 终态退出 0，`prepull-execute.WkJhzeC0` 的主流程和清理
退出码均为 0。三节点隔离 Job 拉取候选索引
`sha256:f8e6cbcb8e5102dca059fedc9d4aba177a220d318816fc5c987c8679251148f9`，
验证实际平台镜像后完成清理。前后原 StatefulSet UID/spec/generation
保持不变（generation 130，3 Ready/updated）；没有切换工作负载。

会话 60022 独立复核清理：三条精确 Job 身份回执均 removed，最新
Job/Pod 列表中没有相应 Job 名称或由其 UID 拥有的 Pod；证据为
`cleanup-fresh.json`、`cleanup-fresh.sha256`。临时拉取 Job/Pod 已删除，
节点镜像缓存保留，未删除业务数据或卷。预拉取不是应用验收结果，
也不保证未来节点镜像缓存不会被回收。

临时卷清理脚本及其三个规划器已为新 owner 准备并绑定摘要，但尚未
对真实卷执行。会话 59959 退出 0，`scratch-controller-tests.8TnXO3hf`
六个无网络离线用例通过：只读规划、成功清理、挂载占用、补丁冲突、
基线变化、恢复失败。原有 85 个流程用例与这 6 个清理用例分别保留
证据，不将其计为真实存储故障验收。

运行时/恢复/清理摘要再次通过，HOLD 仍在，尚未领取部署凭据、上传
实验 Secret 或注入故障。下一步完成现场准入和恢复核验准备后，再
执行原 30 秒门限实验；整体生产就绪目标仍未完成。

### 本轮已通过现场准入，部署仍在运行

`final-admission.aJMgWHN1` 完成实时基线、三条候选 CI 成功终态、
证书至少四小时剩余有效期、端口及故障资源缺席检查。唯一执行会话
为 **79201**，控制器已解除 HOLD 并开始部署；不可依据之前的 HOLD
记录重新启动一轮。双根信任阶段 `roots-expand` 已通过滚动及阶段后
验证（`verify-after/exit-code=0`），当前进入 `members-expand`。

这只是运行中记录，尚无整轮成功/失败或恢复结论。原 30 秒故障门限
尚未执行。独立终态恢复检查脚本已准备并绑定摘要；后续需跟踪同一
会话到终态，再核验原工作负载/Secret 恢复、故障资源消失及精确临时卷
清理。不能把本节解读成已经恢复或整体生产就绪。

### 本轮故障门限失败，自动恢复仍在运行

同一会话 79201 随后完成 `members-expand`、`protocol-expand`、
`diagnostics-expand` 的验证，进入 `deploy-execute.z3z8kYNU` 实验。
故障起点为 `1789798688811007697` ns；原流在故障后
**29.974426963 秒**返回 TTL 10、term 268。虽然单条响应在 30 秒内，
完整控制器 `fixture/run.exit=124`，故**原门限验收失败**，不能以
单条响应替代全部断言。`successor/exit-code=0` 也不改变整体失败。
具体超时位置仍需完整时间线核对，暂不归因于存储或发送预算修复。

`fixture/cleanup.exit=0`，上层 `exit-code=124`；自动恢复已启动，
目前运行 `recovery/diagnostics`，执行会话仍存活。尚未取得全部恢复
阶段、Secret 清理或独立恢复核验的终态，不能宣称原镜像已恢复。
后续继续跟踪 **79201**，不得启动新实验覆盖本轮失败证据。

只读时间线核对已定位直接超时点，证据 `failure-timeline.vHpwTPN1`：
successor 第 47 次采样的确认发生在故障后 **29.821388370 秒**，
此时剩余 **0.178611630 秒**；随后 `stack.AjFl72mz` 开始采集时
只剩约 0.150 秒。`verify-inputs` 用时约 0.678 秒，下一步
`snapshot-before` 返回 124，没有 `COMPLETE`，也未执行保护栈探针。
故没有 `frames-demoted.json` 可证明目标成员的过期续租等待已解除。
两个故障前栈采集完整成功，不能替代故障后的必要检查。

上述说明失败发生在哪一步，不证明新 leader 确认偏晚的根因，亦不
证明发送预算修复无效或存储是唯一瓶颈。保留完整原门限和校验要求。
恢复进展：`recovery/diagnostics/exit-code=0`；协议恢复已完成三个
副本滚动，尚需该阶段后验证与后续证书恢复、Secret 清理终态。
