# 过期租约续租等待的任期退出

2026-09-17，在已完成 `31c0a1fb` 集群验收之后继续对照固定参考
`/root/etcd`（`5cd9f4ee13801e18825d661e5005ae599460bc3a`）。
etcd `server/lease/lessor.go:Renew` 等待过期租约撤销时，同时监听 `demotec`
和 `stopC`，并在任期结束后返回 `ErrNotPrimary`。

KubeBrain 原 `refreshLeaseHoldingLocks` 的过期分支仅监听 `st.revoked` 和
请求取消。任期结束会取消旧任期的撤销工作并撤下租约快照，因此旧请求可能一直
等待不会再关闭的撤销通道，直到客户端自行取消。

新增确定性测试 `TestExpiredLeaseRenewStopsWithLeadershipTerm`：在释放续租
准入锁的回调处确认进入过期等待，再取消所属任期，不依赖 sleep 猜测执行位置。
修复前失败（session 94666，1.054s）：任期取消一秒后请求仍未退出。
修复捕获受 leaseMu 保护的原任期 Done 通道，等待期间任期结束返回既有
`errLeaseDemotedDuringRenew`，由既有调用层处理路由；不关闭 revoked，
不宣称持久租约已被删除，也不提前返回 lease-not-found。

针对 `TestExpiredLease(Renew|KeepAlive)` 的 race 重复十次通过
（session 53379，3.460s），包含撤销失败后等待及请求取消测试。
完整服务测试通过（session 4692，148.478s）；使用 CI 同样筛选条件
`(Lease|Revoke|Expiry|Checkpoint|Attachment)` 的扩展 race 回归通过
（session 94608，81.325s）；vet 通过（session 21131）。
代码对照确认既有 LeaseKeepAlive 调用层将此错误交给降主转发或 Unavailable
分支，而不是转换成 lease-not-found/TTL=0；这不替代真实多节点路由验证。
这是内部边界验证，不是完整生产故障证明。CI、镜像和集群验证尚未完成；
此前成功的 31c 集群验收不覆盖本修改。

## 后续服务层路由覆盖

新增 `TestExpiredLeaseKeepAliveRoutesAfterTermEnds`，直接调用完整
`leaseKeepAlive` 服务工作循环，在过期分支取得所属任期 Done 通道的确定边界
结束任期并切为 follower。禁用代理时要求 Unavailable、无响应、无转发；
启用代理时要求恰好转发一次并返回远端 ID/TTL（37），不能误发 TTL=0。
这是内存 peer 替身测试，不是实际网络或多节点部署。
与任期退出用例一起 race 重复 20 次通过（session 58361，2.905s）。
该测试补充在后续独立测试提交中记录；当前 `142d44e8` 的 CI 不包含这项后续补充。

通过私有 Go overlay 保留 Done 观察点、仅移除 select 中任期退出分支，两个
服务路由用例均在任期取消后未完成路由而失败（session 15616，2.072s）。
更早的 overlay 同时删掉观察点，失败于等待准备阶段（session 58488），不计为
有效行为负例。真实产品文件未替换；新增测试的 vet 通过（session 77593）。

## df783b7f 发布与真实后端回归

上述后续测试提交 `df783b7f` 的两项 CI 已成功，原始日志确认两个新测试的普通和
race PASS；独立镜像审计通过。随后在独立本地盘 PD/TiKV 上完成 6000 次持续
负载回滚，原延迟门限、Watch/Lease/流验证及独立诊断审核通过，原完整配置已恢复，
本轮临时资源已清理。详见[本轮验收记录](acceptance_local_rollback_df783b7f_20260917_cn.md)。
这取代上文当时“CI、镜像和集群验证尚未完成”的发布状态，但不等于真实集群命中
了“已过期、撤销仍阻塞、此时任期结束”的精确竞态；该故障注入覆盖仍开放。

## 官方客户端 gRPC 边界补充

后续新增 `TestClientExpiredLeaseKeepAliveOnceRoutesAfterTermEnds`：官方 client/v3
通过 gRPC Server 与 bufconn 执行 Grant、KeepAliveOnce，在确定已进入过期等待后
取消所属任期并切换为 follower，要求一秒内取得原 ID、TTL=37 和合法响应头，
恰好转发一次，且不把任期退出伪装为持久撤销完成。请求取消与 goroutine join
保证失败路径也能收尾。对照仍为固定 etcd `5cd9f4ee` 的
`server/lease/lessor.go:396`，特别是 `demotec` / `stopC` 的 ErrNotPrimary 分支。

仓库内最终源码验证（证据 `expired-renew-client-final.qH03qSnJ`，session 61561）：
仅通过私有 overlay 移除产品 termDone select 分支、保留等待观察点时，测试按预期
在任期结束后阻塞而失败（1.056s），不是编译或准备阶段失败；产品文件未替换。
新用例与两个已有任期/路由用例 race 重复 20 次通过（3.700s），vet 通过，
完整服务回归 147.682s、扩展 Lease/Revoke/Expiry/Checkpoint/Attachment race
80.676s 通过，前后源码 SHA 一致。

这是实际 gRPC 编解码及服务调用，但仍使用内存存储和 peer 替身，不覆盖 TCP/TLS、
真实 peer 转发连接或 TiKV 撤销阻塞故障。它不属于此前 df783b7f 的 CI/镜像/回滚；
该新增测试提交的 CI 需独立跟踪，不能借此前成功结果宣称已通过。

### 防止客户端重试掩盖原流退出

后续收紧同一用例：服务端 interceptor 统计 LeaseKeepAlive 流数，成功时必须
恰好为一条，不能只检查最终响应及转发次数。私有负例 overlay 将降主分支
改为直接返回 Unavailable，官方客户端随后重试并成功，但新增断言捕获到
两条流而失败（0.069s）；这证明仅检查成功响应会漏掉原流退出。
负例只替换私有编译映射，未改产品文件；注入无条件 return 的负例关闭 vet，
正常源码仍独立执行 vet。

证据 `expired-renew-same-stream.pD0JG9po`：session 70107 终态 0，三项相关
任期/路由用例 race 重复 20 次通过（3.739s）、vet 通过、源码和日志 SHA 校验
通过。此补充不在此前已启动的 `43a0df5f` CI 中，也仍不等于真实 TiKV 故障覆盖。

## 精确真实后端实验的设计约束（尚未执行）

2026-09-17 后续只读检查确认，隔离实例 `kubebrain-local` 仍为 generation 28、
三个 Ready 副本、原固定 bb89c3f8 镜像。实际参数为租期 30 秒、续约截止 25 秒、
重试间隔 500 毫秒，不能按代码默认 8/5/1 秒设计故障观察窗口，也不能为实验
擅自修改原验收门限。进程存活探针使用 HTTPS `/ping`；pprof 关闭。

选主记录存储在 TiKV，而非 Kubernetes Lease。只隔离旧 leader 的 PD/TiKV
连接虽可促使其任期结束，也会阻止它刷新新 leader 地址：
`etcdproxy.updateClient` 在缓存地址未知时调用 `RefreshLeaderInfo`。
因此“任期等待已退出”和“客户端已经收到转发响应”是不同观察点；不能仅凭
隔离期间没有响应认定仍困在旧 `revoked` 通道。

`leaseKeepAlive` 对转发的 Unavailable 保留已消费的请求并重试，内部单次转发
有 5 秒超时；这不是外部请求必须在 5 秒内结束的保证。实验应保留原始请求和
同一条 RPC 流，在确认任期丢失后恢复该成员后端访问，再验证原请求的完成，
不能用新发起的 KeepAlive 或官方客户端自动重连代替原流恢复。

候选撤销阻塞机制是公共 API 的 CORRUPT 告警，已有内存测试验证它能延迟自然
过期撤销。源码 `InitializeLeadershipRevision` 明确允许有效 CORRUPT 告警下
的只读 leader，但后续完整初始化及真实 peer 转发仍需验证；新 leader 的
`ReloadLeases` 会重建期限，不能先验要求响应一定为 TTL=0。

目前没有创建网络策略、激活告警或执行此故障。开始前仍须证明选定网络策略
只影响目标成员到 `kb-local` 后端、确认请求已进入过期等待、固定 Pod/容器与
连接身份，并准备独立于实验成功与否的策略撤除、告警恢复和租约清理。
这份可行性检查不是故障验收结果，也不关闭精确真实后端覆盖缺口。

后续服务端 dry-run 已通过（私有证据 `expired-renew-fault-preflight.ZqTuFfRZ`）：
候选 Cilium 策略要求目标 Pod 带本轮唯一标签，只拒绝同命名空间 `kb-local`
PD 的 TCP/2379 和 TiKV 的 TCP/20160，并显式关闭该规则对 ingress/egress 的
默认拒绝模式。命名空间 UID 已校验，目标标签匹配零个 Pod，策略在 dry-run
前后均不存在，未设置 Pod 标签或安装策略。实际 Cilium DaemonSet 为 v1.19.4、
11/11 Ready；CRD 接受字段不证明实际流量已被阻断，现有连接的策略生效及
恢复仍需独立确认。公开 TTL 为负且租约/附属键仍存在可证明租约过期未删，
但不能单凭 TTL 推断请求已经进入内部等待分支。

## 单流实验探针（尚未执行真实故障）

新增 [lease-term-probe](../hack/production/cmd/lease-term-probe/README.md)，强制
完整 TLS、指定成员及集群身份、已有过期租约和附属键预检。它仅发送一次原始
gRPC KeepAlive 消息，禁用配置的 gRPC 重试并拒绝第二次 TCP 拨号；不使用
clientv3 KeepAliveOnce 的重连循环。JSON 阶段记录不把 Send 成功当作服务端
进入等待的证据，也不把收到响应当作故障验收通过。创建/清理租约、告警和
网络策略仍由后续带独立恢复流程的实验驱动负责，探针本身不执行这些操作。

本地 session 82215 终态 0：bufconn 模拟服务的正常响应、Unavailable 不重试、
截止时间、错误集群、未过期/不存在租约、错误附属键/响应及输出失败用例，
race 重复十次通过；工作流契约 race 和 vet 通过，源码/日志 SHA 已校验。
首次新增工作流断言误用了 map 字段访问而编译失败，修正后重新验证通过；
不将该失败算作有效行为负例。CI 工作流新增该探针路径触发和独立 vet/race
步骤，但尚未推送，不属于正在执行的 43a0df5f CI。

后续补充本机真实 TCP/mTLS 测试：临时 CA/证书、强制客户端证书的 gRPC Server
通过 CLI 完成成功、错误服务名及服务端断开三个场景。成功只建立一个连接；
后两者必须失败且不能输出成功响应，服务端没有接收替代连接。session 9443
的完整探针 race 重复十次通过（12.004s），vet 通过。首次测试夹具重复嵌入
接口导致编译失败，去除重复嵌入后重跑；它不是产品行为负例。这仍是模拟服务，
没有验证真实 PD/TiKV 或故障策略生效。

43a0df5f 的回归 CI `35245499360` 已成功；镜像 CI `35245499187` 在此次记录时
仍运行。这两个任务均不包含上述后续单流断言、探针及 TCP/mTLS 测试。

再检查发现探针预检只核验 TTL 的 cluster ID，可能接受 Status 与 TTL 之间的
成员/任期切换。新增 changed-member、changed-term、zero-term 三个用例，
修复前均错误返回成功而失败（session 9854，0.029s）；修复要求 Status 与 TTL
响应的成员和非零 RaftTerm 一致，并将集群/任期写入预检 JSON。修复后的完整
探针 race 重复十次 12.156s、vet 及工作流契约 race 2.506s 通过（session 84136）。
这不是服务端原子选主栅栏，不能证明预检之后至请求处理之间未发生任期切换。

## 43a0df5f 发布镜像独立核验完成

两项固定源码 CI `35245499187` / `35245499360` 均成功；归档日志 SHA 校验通过，
三项任期相关测试的普通和 race PASS 已确认。首次独立核验在镜像拉取 600 秒
上限处退出 124，没有进入二进制检查；保留原失败记录，不能追溯改成成功。
确认原命令终止后，第二次核验只将下载上限改为 1800 秒，产品验收门限未变。

第二次 session 14078 终态 0，证据 `release-43a0df5f.G04E3cJa/audit.jLj1Sm9x`：
OCI index `sha256:0e29b78341844a2d638d573946543509634096466168e8f9e970e3912cfec392`，
amd64 `sha256:92fa031086ac8da9990ff2f3c424cdddcf5095d7132c1a847ab67adf499ade06`，
arm64 `sha256:a0a2c604f9401f31f30cabee3cef5e46c9ccbec8604f7aef2dba589d33d3b6b0`。
实际执行 amd64 二进制确认 Git SHA `43a0df5f0fb58c0d4364a360eff6369ad143f5d5`、
版本 `0.0.0-dbaas-43a0df5f0fb5`、Go 1.26.8、非 root 用户及 OCI 标签一致，
内嵌依赖确认固定 TiKV fork 与 grpc 1.83.2。此次独立核验没有执行 arm64
二进制；临时容器和抽取的二进制已清理，证据保留。

尚未部署该镜像或执行精确故障实验。上述核验不覆盖后续本地新增的单流断言、
专用探针、TCP/mTLS 测试和预检任期校验，后续提交须等待各自 CI。

## 0a92a4f3 发布镜像独立核验完成

2026-09-17，固定源码 `0a92a4f37cccfa6d71c0053be1aea6f168b1a896` 的镜像 CI
`35250071444` 和回归 CI `35250071515` 均成功。归档日志 SHA 校验通过，
本次已包含单流断言、专用探针、TCP/mTLS 测试及预检任期校验。

独立核验 session 67650 终态 0，证据目录
`release-0a92a4f3.hDcmVjEM/audit.0noVIspi`：

- OCI index：`sha256:20f2850aeb096e7615e243365b04b85faa61f01a11c7d1b650a4f7fe3a2d2bea`。
- amd64：`sha256:988142d38c595dc6f315830d2f63b95188b30b44214ba733d336b0e213fb5edf`。
- arm64：`sha256:d9936492b1966f0d05371173972e4f863072c4e261271ef40587def5afd0e7d0`。

实际执行 amd64 二进制确认版本 `0.0.0-dbaas-0a92a4f37ccc`、完整源码 SHA、
Go 1.26.8、BuildTime `2026-09-17T17:01:16Z`，并核对 OCI 标签、非 root 用户、
固定 TiKV fork 和 grpc 1.83.2。未在本机执行 arm64 二进制。临时容器、抽取的
产品二进制及本轮镜像核验辅助二进制已清理；日志和身份记录保留。

此结果仅证明发布镜像身份和对应 CI，不证明部署或精确故障验收。集群尚未
部署该候选版本；过期租约等待期间的任期退出、原 RPC 存续和完整恢复仍待
真实 PD/TiKV 实验验证。诊断配置与恢复规划已通过离线检查及服务端 dry-run，
没有实际开启 pprof、改变健康探针或安装故障策略。

## 真实过期租约夹具：观察成功，清理失败，恢复中

在原 bb89 固定镜像的 `kubebrain-local` 上，使用独立测试键和 10 秒租约，
确认初始无告警后临时激活 CORRUPT。真实 PD/TiKV 后端上已观察到负 TTL、
正 GrantedTTL 及保留的附属键。没有发送 KeepAlive 或注入主节点切换。

但解除告警经非主成员请求在 10 秒超时，直接连接当前主成员也在 45 秒超时；
首次夹具脚本 session 50048 因清理失败退出 70，直接恢复 session 44978
退出 28。首次清理后的读取仍显示本轮告警、租约和键存在，不能称为测试通过
或已恢复。另有 TTL 为零时 JSON 省略字段导致探针解析报错，保留原始记录。

证据为 `expired-lease-fixture.xuRFxavl`。当前通过正常维护 API 进行更长预算的
定向恢复，不绕过事务记录验证、不直接删除后端内部键；300 秒仅为恢复请求
预算，不改变原验收门限。恢复终态和独立清理验证仍待记录，在此之前暂停
其他故障实验。上述超时原因尚未确定，不能仅凭源码推断为事务记录扫描慢。

后续恢复已完成：session 63431 终态 0，正常维护 API 清除本轮精确成员的告警，
读取确认租约不存在、测试键不存在。独立 `postflight.sh` 改用容器内完整 TLS
连接再次核验告警为空、租约及键消失；StatefulSet 完整配置和 generation 28
未变，入口 Pod UID/容器状态未变，三个本地实例 StatefulSet 均为 3/3 Ready，
本机 18483 端口已关闭。只清除了本轮可丢弃的测试租约和键，未改后端内部数据。

原始退出 70、直接恢复退出 28 的失败记录保留。较长恢复预算下成功不代表
通过原时间门限，也不能证明超时原因；解除告警的延迟问题须继续定位。

本地补充 `BenchmarkDisarmCorruptHistory`，直接执行完整 DisarmCorrupt 路径，
不以使用不同校验模式的启动测试代替。session 86191 终态 0：64、256、1024
笔单键历史事务分别产生 66、258、1028 次 BatchGet。测量使用内存存储，仅能
证明存储调用数随历史增长，不能把本地耗时换算成真实 TiKV 延迟或据此认定
集群超时原因已完全确定。当前未优化产品实现；后续须保留对象、事务见证及
索引一致性验证，不能以跳过验证来降低调用数。

后续 race 基准与已有告警/索引测试通过（session 88831，3.102s）。新增调用数
回归测试在不同键、重复写同一键两种情况下均有效失败：600 笔历史事务触发
604 次 BatchGet，高于有界窗口目标 8 次（session 53584）。该失败发生在实际
解除告警成功后的调用数断言，不是构建或夹具错误。测试先保留为待修复负例，
尚未修改产品实现，也未提交或推送为通过版本。

随后实现了按现有 512 项索引校验窗口批量读取历史对象：只去重物理读取，
保留每个历史版本的验证，以及告警前的事务见证、压缩进度和精确证据复查。
相关普通测试通过（session 36454），相关 race 与基准通过（session 78084，
6.962s）；64、256、1024 笔事务分别为 3、3、6 次 BatchGet。该结果尚未经真实
TiKV 故障复验。新增历史对象损坏用例及完整 backend 测试仍在运行，未推送。

测试文件编辑期间曾误用已有文件名，原 `corrupt_alarm_batch_test.go` 已恢复至
HEAD 原内容并验证无差异；新增用例改放 `corrupt_disarm_history_test.go`，后续
上述测试包含恢复后的原覆盖。追加损坏用例时的一次缺括号编译错误也已修正，
不将编译失败计为行为负例。

完整 backend 与新增损坏用例的首轮运行失败（session 89586 / 7068）：新增测试
把单字节原始值当成损坏元数据，但它可合法走旧格式值路径，夹具假设错误。
缺失对象用例未报失败。已改为截断的保留元数据头，并在注入前断言解码确实
返回无效元数据；保留原失败日志，重新运行完整 backend、race 和 vet。

修正夹具后，新增解除告警场景 race 重复三次通过（session 83945，12.455s），
vet 通过（session 2037），完整 backend 测试通过（session 42060，52.384s）。
本地验证覆盖不同键和重复键的读取次数，以及窗口起始、边界、尾部旧对象
缺失/元数据损坏时保留告警、修复后解除告警。真实 TiKV 的延迟改善和精确
租约任期切换仍未验证，不能据此宣称整体生产就绪。

## 2026-09-17：批量历史校验的真实 TiKV 复验通过

产品提交 `df2fbd7bd087d7e8d1167cef0353f84f8c23e79a` 的镜像 CI
`35256530134`、backend CI `35256530100`、probe CI `35256530092` 均成功。
独立发布审计 `release-df2fbd7b.UactPyJN/audit.Buk3VCbU` 核验源码、版本、
依赖与实际 amd64 镜像；发布 index 为
`sha256:4b0f33251ceca499a2fb28e8eb3aaed172bbc90d11db91b4e1fc4679b2c596f2`，
amd64 摘要为
`sha256:0aeff678fb6e1248e220d14b3319ed3d1a5666131aae7a36b0bb872f1a20e6a3`。
CI 覆盖双架构，本机独立运行验证只覆盖 amd64，不扩大验证结论。

实际部署与恢复证据位于本机私有状态目录
`local-disarm-df2fbd7b.gQCywg1U/deploy-execute.QJYXwKHT`。部署前检查、
三个节点的固定摘要预拉取通过后，仅滚动替换 `kubebrain-local` 的镜像。
使用本地 TopoLVM、独立 PD/TiKV 及原默认 2PC；未更改后端配置，未操作
旧 Ceph 前端或 secondary 集群。

10 秒租约到期后，`ttl-11.json` 同时记录 TTL=-1、GrantedTTL=10 和保留的
附属键，证明命中真实后端的过期但尚未撤销状态。随后经成员 0 入口解除
本轮 CORRUPT 告警，耗时 **5.224856 秒**，满足原 **10 秒**请求门限。
没有放宽验收门限；300 秒预算仅保留给异常恢复。此单次结果证明当前
样本通过，不是延迟分布或长期稳定性结论，也不能单独证明之前超时的全部原因。

夹具 `run.exit=0`、`cleanup.exit=0`；最终读取确认告警为空，测试租约及键
不存在。总流程 `operation_exit=0`、`restore_exit=0`、`holder_cleanup_exit=0`。
原 bb89 固定镜像及完整 StatefulSet 配置恢复，generation 30，三个副本就绪。
独立复查 `postflight.xSAA3Yjt` 与清理后的 `postflight.8TzuL92o` 均通过：
后端 Pod 身份/容器状态保持不变，无残留租约、告警或本轮测试前缀。

凭历史 Bound PV/PVC 身份、当前 Released 状态及替代卷绑定凭据，仅删除本轮
滚动更新产生的 12 个临时卷（`proven-scratch.S8ZQrTb2`，数据不可恢复）；
其他 PV 配置未变，保留 12 个 Bound 卷和 2 个历史 Released 卷。

本轮未发送 KeepAlive、未注入任期退出，不证明原始单流 RPC 已在过期续租
等待路径中跨越任期切换。该精确实验及整体生产就绪验收仍未完成。

后续新增 `TestExpiredLeaseWaitRuntimeStack`，验证已知本地调用在过期等待时，
完整运行时栈可呈现 `[select]` 状态和顶层 `refreshLeaseHoldingLocks` 帧；
不接受仅在祖先调用链出现该函数，也不接受截断栈。随后取消任期，要求该调用
返回 demotion 错误。普通测试通过（session 50741，0.058s）；与已有任期退出、
路由测试一起 race 重复十次通过（session 89097，2.364s）。现有 CI 的 `Lease`
筛选包含该测试，但本轮新增测试尚未推送或在 CI 执行。

这只是本地诊断可行性：集群证据仍须绑定精确发布源码行、Pod/容器身份、
唯一待处理请求以及前后任期，不能从任意进程级栈推断某个租约或 RPC 的状态。
尚未开启集群 pprof，也未注入网络故障。
