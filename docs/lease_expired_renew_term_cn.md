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
