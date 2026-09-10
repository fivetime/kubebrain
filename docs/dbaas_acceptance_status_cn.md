# DBaaS 验收状态

核验日期：2026-09-10。当前验收环境为 `tk-001-003`；旧 `kind-kubebrain-dbaas` 结果单独保留在本页历史部分，不作为新环境现状。
产品要求及兼容性矩阵见 [兼容性计划](dbaas_compatibility_plan_cn.md)。本页列出当前证据的边界与下一步验收条件，不能代替完整矩阵。

总体状态：**尚未通过生产就绪验收**。已完成迭代编号、提交数和单元测试数量都不是整体完成百分比。

客户端测试维护进展（2026-09-10）：fork 提交
`6539bbb040f152980492281148d9d7dd289473b0` 已推送至 `kubebrain-v2.0.7`。
该提交前后均通过 fork build/vet/unit/race、专用 mock 1PC 普通 10 轮/race 3 轮，
以及 KubeBrain 各 720 项四分片门禁和摘要核验。但 CI 34483731290 **失败**：
security/test 作业成功，onepc 的普通用例通过后，非 root Runner 无法清理复制而来的
只读依赖目录，后续 race 未执行。不能将本地 root 下通过等同于 CI 通过。
当前修复只给私有临时副本的目录恢复所有者写权限、不跟随符号链接；非 root 回归验证了
成功/失败退出码保留、嵌套只读目录删除及外部链接目标不被修改，并有失败复现和反向测试。
清理修复已提交并推送为 `2155365951a870077becc05af4a49c8ed90231bf`。
该修复提交前后均通过非 root 清理契约、fork build/vet/unit/race、专用 mock 1PC
普通 10 轮/race 3 轮，以及 KubeBrain 各 720 项四分片门禁和最终源码摘要核验。
新 CI [34490393381](https://github.com/fivetime/tikv-client-go/actions/runs/34490393381)
已全部通过：test/security/onepc 三个作业均成功，日志确认非 root 清理契约通过，
且 onepc 普通与 race 两轮命令都执行成功。这关闭了旧 CI 的临时目录清理问题。
清理修复门禁证据：`/root/.local/state/kubebrain/onepc-cleanup-gates.WzjzlOOO/`。
KubeBrain 产品依赖仍固定在 fork `832b70fd622f`，未启用生产 1PC。
证据目录：`/root/.local/state/kubebrain/onepc-integration-gates.5V2i9vl7/`。

另在仓库外原型中，以真实 KubeBrain 存储适配器/后端连接 unistore，按调用上下文仅对
用户 1PC 提交丢弃已成功的响应，已验证 `ErrUncertainResult`、已提交见证解析指标、
两键同修订号及下一次写入只增加一个修订号；普通一次和三轮 race 通过。
后续补齐发送前失败：解析为未提交、两键均不存在、公共修订号不前进、下一次写入复用
预留修订号；两种情况普通 10 轮与 race 3 轮通过。进一步注册真实后端 watch，验证已提交
两键同批同修订号，并以随后一次确认成功的写入作为有序边界，检查此前没有额外事件；
未送达场景的首个事件只能来自下一次写入。该扩展普通 10 轮/race 3 轮通过。
初版 watch 断言混淆内部 CREATE 与对外 etcd PUT，失败后按现有适配层映射修正；未改产品代码。
隔离 overlay 删除已提交解析路径的事件发布后，测试在公共修订号恢复断言处失败，
说明能够检测该解析回归；另一个 overlay 仅发布两键中的一键，测试明确在“两事件同批”
断言处失败。两次反向测试均未修改实际仓库源码。该原型仍不是正式集成回归、外部 etcd Watch API 验收或真实 TiKV
持久性证明；尚缺默认重试开启路径和真实环境验证。
代码/日志在 `/root/.local/state/kubebrain/backend-onepc-integration.HabFHNJ0/`。
生产协议、集群配置和下述 900 秒验收失败结论均未改变。

最新终态（2026-09-10，c7d9905e 正式复验）：配额批读优化的发布源码
`c7d9905e57526c4d2868f96805b7e276f37888c0` 已通过 CI 34470793561，
实际镜像身份、两种架构发布及非 root 运行配置已核验。但原 6000 次操作、
public 5s / direct 30s、滚动完成后 900s 完成窗口的升级测试仍因完成窗口超时失败。
执行器 exit 1，未取得完整 PROBE_SUMMARY，不认定通过或性能改善。
已核验自动回滚后的 generation/observed 18/18、Ready/updated 3/3，
current/update revision 均为 `kubebrain-855b5bfb88`；三 Pod 实际镜像及 imageID
均为修复版 `sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`，
无删除标记且重启数为 0。fixture 全零，探针及三个本轮预拉取 holder 已清理。
同一候选进程两次采样间 603 次 Put 平均约 87.17ms、959 次批提交约 45.77ms；
与上一轮采样涉及不同 TiKV store，不能据此作受控 A/B 结论或把存储耗时归因于物理盘。
下一步继续定位串行操作耗时，并开展隔离的真实 Kubernetes 接入回归；不盲目重跑或放宽门限。
提交协议优化的前置缺口：固定 fork `832b70fd622f` 的 `integration_tests` 是独立
Go 模块，不能用根模块 `go test ./integration_tests` 覆盖。进入该模块执行
`go test -mod=readonly . -run '^TestOnePC$' -count=1 -timeout=180s -args -with-tikv=false`
在测试执行前失败，提示需要更新 go.mod；这不是 1PC 行为测试失败，也不是通过。
源码中的 `Test1PCLinearizability` 使用 `begin()`，而非显式启用 1PC 的 `begin1PC()`，
且没有断言实际使用 1PC，不能仅凭用例名称宣称覆盖该协议的一致性。
后续若评估 1PC，需先修复隔离测试依赖/协议断言，再覆盖跨 Region 回退、响应丢失的不确定
提交及 KubeBrain 持久见证解析，最后做真实环境测量。当前没有启用 1PC 或 async commit。
隔离诊断补充（2026-09-10）：已复现上述覆盖缺口的 RED/GREEN——给原用例增加
实际 1PC 断言即失败，改用 `begin1PC()` 并增加提交后新事务的时间戳/读可见性检查后，
mock-store 用例通过 10 轮。旧 TiDB 测试依赖另有 `runtime.buildVersion` 私有链接问题；
仅在仓库外副本中改用 `runtime.Version()` 并对齐测试依赖后，默认链接检查及
`-mod=readonly` 下再次通过 10 轮。没有修改模块缓存、正式 fork 或生产提交协议。
KubeBrain 原有不确定提交见证/错误分类专项另通过三轮 race；这些证据不覆盖真实 TiKV
1PC 响应丢失、持久性或性能，不能据此启用 1PC。诊断输入、结果及后续条件见本机
`/root/.local/state/kubebrain/onepc-review.sQ6wPqWW/REVIEW.md`；副本不作为权威源码。
响应丢失诊断补充：隔离 mock 新增发送前失败和服务端 1PC 提交后丢失响应两例，
用普通传输错误验证客户端自行分类为不确定结果，并核对数据/见证两键的全无或全有。
两例通过 10 轮；整个修正后的 TestOnePC 组通过三轮 race（默认链接检查、只读依赖）。
临时移除客户端针对 1PC 的不确定分类，两例均在预期断言失败，证明能检测该回归。
测试禁止隐式成功重试，未覆盖开启重试后的恢复；也不等于 KubeBrain 实际解析器、
真实 TiKV Raft 持久性或性能验收。新增测试仍在上述隔离目录，尚需规范化集成到 fork。
以下均为历史阶段记录，集群现状以上述终态为准。

本地进展（历史）：`260d51e17b9b4c3d99ee95978770645d4c299425` 将启用配额时的
QuotaStatus 三个元数据点读合并为一次批量读取，固定快照及无批读能力的路径保持原行为。
提交前后各 720 项生产测试、inventory、专项 race/vet 均已通过；新增基准仅量化
本地存储 API 调用，不证明 TiKV 延迟改善。当时发布和真实环境验证尚未完成；后续结果见上文。

上一轮实际终态：scale-lab 整合已推送，发布源码 `3b15bd16` 的 CI
[34459431691](https://github.com/fivetime/kubebrain/actions/runs/34459431691) 全部成功。
从维护修复版基线执行的原 30s/900s 正式升级复验失败：候选三副本滚动完成，
但探针未在滚动完成后的 900s 内完成 6000 次操作。已自动回滚修复版 dd339bc1，
generation/observed 16/16、Ready/updated 3/3、current/update revision 均为
`kubebrain-855b5bfb88`，实际三 Pod 镜像及 imageID 均为
`sha256:0ce85e66320b27bf4cdb8f11981a76cba58926fa0e835fc47dc663f709b588ce`。
fixture 全零，探针和本次预拉取 holder 已清理。原正式验收仍未通过，不重复盲跑或放宽预算。

本轮新增局部证据：维护修复版旧 Pod2 同 UID 状态确认 exit 0 Completed，日志包含
三端口 root server shutdown 与 shutdown complete；真实候选三副本客户端指标采样通过。
同一进程两次采样间 896 次 Put 的服务端平均耗时约 79.93ms，1402 次批提交约
41.24ms；这不是尾延迟或根因证明，各层指标不可直接相加。详细证据和边界见测试环境交接记录。
现有 Kubernetes 接入与 KWOK 测试代码应复用并针对本环境回归，不应描述成尚未开发的能力。
以下“最新/当前”字样均为保留的历史阶段，以上述正式复验回滚终态及测试环境交接记录为准。

2026-09-10 最新终态：dd339bc1 的 CI 34417282642 和镜像验证全部成功，但真实升级
在 iteration=117 因 direct watch 恢复超时失败。已确认回滚原 a245c95f 镜像，
generation/observed 13/13、Ready/updated 3/3。旧实例仍以 15s 超时 exit 1；
候选实例在回滚时取得同 UID exit 0 的证据，局部退出改善不等于升级验收通过。
以下状态是历史阶段；完整新时间线见测试环境交接记录。

当前最新：TLS 最终关闭循环已复现并修复于 `12b4aedc`，提交前后各 720 项生产测试、
inventory、endpoint race/vet 全部通过，准备推送并恢复 CI，尚未部署。
4f2 的第二次受控升级仍因 direct watch 恢复超时失败，但取得旧进程耗尽 15s
退出窗口的明确日志与同 UID 终止状态；已确认回滚终态 generation/observed 11/11、
Ready/updated 3/3、原 a245c95f 镜像。以下记录均为历史阶段，详细证据见测试环境交接记录。

最新补充：4f2d2402 的 CI 34407926007 已全部成功，镜像身份和只读部署前检查通过。
实际受控升级却在 iteration=116 因 direct watch 恢复超时提前失败，未完成指标采样。
已回滚原 a245c95f 镜像，generation/observed 9/9、Ready/updated 3/3，临时资源清理完成。
探针超时早于候选容器启动，事件显示 Pod 替换期间的临时卷重建和挂载/启动间隔；
镜像已缓存，不能把本次失败归因于业务 Pod 下载镜像或候选启动后的指标注册。
以下 34057ee8 与待发布文字保留上一阶段记录，当前终态以本段及测试环境交接记录为准。

最新终态：34057ee8 在线升级验收失败，原因首先是探针未在 900 秒完成窗口内结束；
已回滚原镜像并确认三副本 Ready/updated，隔离 holder/探针清理完成。
generation/observed 7/7，current/update revision 回到 `kubebrain-696c87f8f9`。
下文升级执行中的观察是历史中间状态，不代表当前仍运行或通过。完整时间线见测试环境交接记录。

后续诊断提交 `36545f8107ca9231db01df54d8f0b7f270679f2b` 增加有界探针进度和阶段耗时，
未修改原有 SLO/次数/间隔。提交前后各 720 项四分片、inventory、探针包 race/vet 已通过；
CI [34394834203](https://github.com/fivetime/kubebrain/actions/runs/34394834203)
以 docs-only 后继 `34057ee8743c28753f9a078268a12783381dbe04` 构建发布成功，
索引为 `sha256:f4fc874cf18633b2cc65afc52113d1c98f20bf63439929ea95238408cd94b637`。
实际新候选预拉取及滚动覆盖通过，进度诊断已运行，但完整探针仍超时；不据此认定已修复。
下一步是细分 Put/存储提交耗时并核对串行负载与完成预算，不降低 SLO 或操作次数来覆盖失败。

后续产品提交 `826fd14cb5854b9d0da5a8cdc155fdcc9b49b36b` 修复 TiKV 客户端指标
未注册到默认 registry 的监控缺口。提交前后各 720 项四分片及 inventory 均通过，
适配器 race/vet 通过；新版本 CI 发布、真实指标采样及升级验收尚未完成。
这不改变上述回滚后的服务版本，也不证明 Put 耗时根因已关闭。

用户授权使用 `root@10.32.32.66` 控制的 `tk-001-003` 集群，并明确只使用 rook-ceph 消费者存储、禁止使用 rook-ceph-secondary。连接路径、安全边界、资源 UID、日志和执行结果统一记录于 [tk-001-003 测试环境交接记录](test_environment_tk_001_003_cn.md)。凭据与验收工具保存在仓库外私有目录，不写入本页。

## 历史阶段快照：84d22dcc 发布及升级启动时

以下表格及“下一步”保留当时证据，不表示当前进程仍在执行；最新终态以上文及测试环境交接记录为准。
测试通过不能跨版本、拓扑或故障范围外推。

| 验收项 | 已取得的证据 | 仍未证明的范围/下一步 |
| --- | --- | --- |
| 分支与源码对标 | 本轮待发布的产品/测试提交为 `a98b6db1a5ff2d186bb8730d1b59f0558ec60d9d`；推送前远端为 998b977e，生产逻辑仍与 05032758 相同；本地只读 etcd 基线仍为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，etcd 工作树干净；当前 go.mod 的 etcd API/client/server 为 v3.7.1 | a98b6db1 的提交前后检查已全过；发布源码以随后新 CI 的完整 headSha 为准（可含 docs-only 记录），固定对标版本不表示已完成支持版本矩阵 |
| 实际服务版本 | KubeBrain 三副本已部署，当前仍使用 `339381af` 的不可变镜像 `sha256:a245c95fea36c387358d86e3808a9d29073a327028d5a4e3a80e4d272663e865`；StatefulSet generation/observed 为 3/3、current/update revision 均为 `kubebrain-696c87f8f9` | 尚未部署本次 05032758 产品候选，不能将本地修复视为线上已生效 |
| 副本与放置 | 新鲜只读查询：KubeBrain/PD/TiKV 各 3/3 Ready、各容器 restart 0，同组件三副本分布在 `k8s3-worker1/2/3` | 节点无 topology zone/region 标签；物理宿主机及跨可用区独立性未证明，节点/网络故障须先确认授权范围 |
| 数据卷与后端健康 | 六个 PD/TiKV 数据 PVC 均 Bound，使用 `nvme-rep3-rbd-pool`；driver 为 `rook-ceph.rbd.csi.ceph.com`、clusterID 为 `rook-ceph`；本轮原样后端健康门禁 exit 0，含六卷实际 CSI 身份/容量隔离/Retain 与连续三次无异常 Region 检查 | StorageClass 默认 reclaimPolicy 仍为 Delete，不能与六个现存数据 PV 的 Retain 混为一谈；新增数据卷仍需逐卷验证保留策略；瞬时健康不等于故障恢复和长期稳定性 |
| 已有受控可用性验收 | 当前服务版本此前通过受控 leader Pod 删除：900/900 操作、public watch 900、三个 direct watch 各 900、lease 存活、官方 etcdutl restore 校验 | 不覆盖无主动释放的崩溃、节点失联、网络分区，也不是新候选升级验收 |
| 冷镜像在线升级 | de8a9e1f 的真实冷升级曾因 direct watch 超过 30s 门限失败并回滚；镜像拉取约 36.973s | 失败仍未关闭。05032758 增加隔离预拉取、运行时摘要核验与失败清理，但尚需新候选真实准备及完整升级验证，不能仅以缓存已热的重跑证明修复 |
| 当前候选本地门禁 | 05032758 提交前/后各 720 项四分片及 inventory 均通过，组件/CLI/build race 通过；已预编译恢复工具并验证缺少镜像回执时入口停止 | 本地测试不替代实际控制器、CRI 拉取、真实业务探针和回滚清理证据 |
| 当前镜像发布 | CI [34382354317](https://github.com/fivetime/kubebrain/actions/runs/34382354317)（源码 `84d22dcc424245128f05802a424ee598e7a20364`）全部成功；独立核对索引/双平台摘要、promotion、实际 amd64 版本/标签和 fork 客户端模块后已建立镜像回执；索引 `sha256:b3b5c25ac815f5b9388be6aa3f987a3dd6c602e3378f4fb142b6fa1f822ca725` | 正在执行只读集群升级前检查，尚未创建 holder/探针或切换业务镜像；镜像发布成功不等于升级或生产就绪验收成功。两次旧失败和本次 Node 弃用警告保留 |
| 总体验收 | 原环境的兼容性、恢复和压力实验仍作为各自范围的历史证据 | 新环境真实 apiserver 路径、新版本在线升级、后端 TLS/轮换、数天级 watch/故障恢复 soak、生产规模/版本矩阵，以及管理面、计量和外部系统验收仍开放；以完整兼容性计划逐项验收 |

本轮只读证据位于私有交接目录的 `security-prepull-acceptance-{serving-state,pods,storageclass,nodes}.json`；
最新执行补充：84d22dcc 的新候选准入检查与正式隔离准备/再次核验均通过，6000 次探针和
滚动升级已实际启动。当前正在从表中旧服务基线切换到新索引，最近中间状态为 generation 4、
Ready 3、updated 2；尚无完整升级与清理成功结论。执行日志为
`security-prepull-84d22dcc-upgrade.log`，进程句柄 93250，恢复 journal 为
`prepull.YczW7ofYlsNf/attempt`。切勿重复执行旧基线入口。

后端健康日志为 `security-prepull-acceptance-backend-health.log`。该日志的 `max_disk_used_percent=90` 是拒绝阈值，
不是实际磁盘使用率。当前交接证据不依赖旧 `/tmp` 镜像归档，但仍需按正式发布要求持久归档。

下一步：完成精确集群/存储/健康身份与新候选准入检查，执行隔离预拉取和
6000 次探针的真实在线升级；其后继续完整计划中的恢复、TLS、真实消费者和长时间验收。未获得授权前不操作
worker、PD/TiKV 重启或网络故障，不使用 secondary Ceph。进行中的状态文档暂不 push，避免取消原 CI。

## 历史基线：A5788 / kind（2026-09-08）

以下表格和收尾记录仅描述当时的 kind 环境，其单节点、hostPath、镜像和部署待办不能套用到当前 tk-001-003。

| 验收项 | 当前证据 | 尚需取得的证据 |
| --- | --- | --- |
| 分支与源码对标 | 当前为 `dbaas`；本地 etcd 基线为 `5cd9f4ee13801e18825d661e5005ae599460bc3a`，etcd 工作树干净 | 支持版本窗口内的完整差分结果；本地 HEAD 不代表已查询远端最新版本 |
| A5788 镜像引用门禁 | 代码 `aca51b7f`；三个 RED 已复现误放行；修复后 focused/race、完整 readonly probe、提交前后四分片通过；候选 generation 1011 完整门禁及回滚后 generation 1012 稳定适用门禁通过，均 3/3 Ready、restart 0 | 本轮子项已完成；不证明整个产品已生产就绪 |
| 数据面副本健康 | 3 KubeBrain、3 PD、3 TiKV 当前 Ready | Ready 只证明当前服务状态，需另外验证隔离、恢复、故障历史和 SLO |
| 故障域隔离 | 集群只有 `kubebrain-dbaas-control-plane` 一个节点，无 zone 标签；6 个 PD/TiKV Pod 均运行在该节点 | 真实独立节点/故障域部署及对应分区、节点故障、恢复测试；同宿主多容器不足以证明跨可用区能力 |
| 持久卷与容量隔离 | 唯一 StorageClass 为 `standard`，provisioner 为 `rancher.io/local-path`；PD/TiKV 数据 PV 为 hostPath，共享宿主文件系统 | 独立 CSI 卷的 driver/volumeHandle/claimRef、实际容量及剩余空间通过严格存储门禁 |
| 存储生产门禁 | 清理后原样执行 `KUBE_CONTEXT=kind-kubebrain-dbaas hack/production/validate-tikv-region-health.sh` 仍返回 1；磁盘压力已消除，6 个卷各有 CSI 身份和容量隔离错误 | 修复实际基础设施后原样通过；不能放宽阈值或改写 hostPath 为 CSI 元数据 |
| 恢复与长期验证 | 计划 P2 明确保留真实 CSI target retirement→provisioning→durable restore、加密 key promotion/撤权、版本矩阵、生产规模和长时间 soak | 对每个开放项提供新鲜、可追溯的真实执行结果；已有实验结果不能外推未测拓扑/规模 |
| 管理面与计量 | 计划 P1 仍列出跨 cluster/region 调度、管理面/外部 IdP HA soak、预生产 Prometheus evaluation/连续 24 小时采样及外部财务系统真实回执等开放项 | 本轮未验证这些外部系统；须按完整计划分别取得实际证据，不能由数据面门禁替代 |

下一步顺序：完成[安全升级门禁](security_baseline_20260908_cn.md)，取得新基线 CI 镜像后部署 KubeBrain 并完成客户端语义测试，再推进 P2 恢复、故障历史与长时间测试。原 kind 环境及其 hostPath 卷保留，不迁移或覆盖；新环境固定使用仓库外独立 kubeconfig。三 worker 的物理宿主机/可用区独立性尚未证明，节点级故障还涉及用户已有工作负载，需先确认具体授权范围。

原 kind 部署入口复核：`deploy/production/kubebrain.yaml` 和 `deploy/production/tidb-cluster.yaml` 均要求同组件副本按 `kubernetes.io/hostname` 硬反亲和，不能在该单节点上满足三副本调度；zone 规则仅为偏好，模板本身不证明跨区放置。该 kind 集群 `CSIDriver` 数量为 0，CRD 清单没有 VolumeSnapshot、Prometheus 或 ServiceMonitor API。现有模板仍需目标环境注入镜像、存储、证书、网络与监控配置，不能直接 apply 到实验实例。上述是相应部署/恢复/计量验收的环境前置缺口，不应记作已经复现的 etcd API 语义缺陷，也不代表所有产品代码工作都依赖这些环境。

2026-09-08 构建清理：按明确 ID 回收 84 条 KubeBrain 编译缓存（约 348 GB），删除 1,744 个旧 OCI/解包临时文件（41,548,937,075 bytes）及 3 个本地编译二进制；未删除源码、测试记录或数据卷。根分区可用空间由约 1.7 GiB 恢复至 327 GiB；本轮镜像导入后约 324 GiB。旧实施记录中的 `/tmp` 镜像归档位置是历史位置，清理后不再代表可恢复实物；对应构建产物需要从记录的提交重新生成。

当前 OCI 和日志保存在 `/tmp` 的 tmpfs 上，只能视为本次运行的临时证据。重启或清理会丢失，生产验收还需要持久保存镜像、日志和验收结果。

A5788 回滚终态：StatefulSet generation/observed 为 `1012/1012`、resourceVersion `9119057`，22 个启动参数不变；runtime 恢复稳定摘要 `sha256:bc6b443ff3508482908155bfaf234d924305f1dce215fd8f7de14093e83d899b`。候选与稳定门禁的 revision/HashKV 均为 `75044/1984703050`，term 随滚动选主由 `894` 变为 `896`。候选 CRI container/image、containerd 镜像引用及四份关键 manifest/config content 已清零；六个临时端口监听均已关闭，PD/TiKV 六副本仍 Ready，三个 store 为 Up。未触碰其他轮次运行时索引。
