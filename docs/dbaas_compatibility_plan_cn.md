# KubeBrain DBaaS 兼容性计划

本文定义 `dbaas` 分支的产品边界和验收标准。目标不是复刻 etcd 的
Raft、bbolt 和成员管理内部实现，而是对通用 etcd v3 客户端提供可依赖的
数据语义，并用 TiKV/PD 的原生能力替代不适用的维护操作。

对标源码固定为：

- KubeBrain：本仓库 `dbaas` 分支；
- etcd：`/root/etcd`，首次基线提交 `d947b2086`；
- API：`go.etcd.io/etcd/api/v3 v3.7.0`；
- 消费端验证：官方 `client/v3`、`etcdctl`，以及 `/root/etcd` 中可复用的
  集成测试。Kubernetes 和 Cilium 套件继续作为回归集，而不是完整兼容性的
  唯一依据。

## DBaaS 产品边界

一个售卖实例由独立的 KubeBrain、PD 和 TiKV 集群组成，拥有独立 endpoint、
证书、资源配额、持久卷、备份策略和计量数据。共享 TiKV 只适用于开发或低
保障套餐，不作为生产默认方案。已有 tenant keyspace 保留为内部能力，但不
作为生产 DBaaS 的计费或故障隔离边界。

兼容性分为四类：

- **兼容**：客户端可依赖 etcd 的响应、错误、revision 和原子性语义；
- **部分兼容**：已覆盖 Kubernetes/Cilium 或常见请求形状，但通用形状仍有差距；
- **平台替代**：etcd RPC 对 TiKV/PD 架构不适用，由 DBaaS 控制面提供等价能力；
- **缺失**：通用 DBaaS 需要，但当前尚未实现。

## 当前矩阵

| 服务 | 能力 | 当前状态 | DBaaS 处理 |
| --- | --- | --- | --- |
| KV | Range/Put/DeleteRange | 部分兼容 | P0：逐项对齐排序、过滤、历史读、错误和大范围删除原子性 |
| KV | Txn | 部分兼容 | P0：点键缺失 guard 已完成；继续补齐范围 phantom guard、嵌套 txn 和所有合法分支的单 revision 原子性 |
| KV | Compact | 兼容核心语义 | P0：继续对齐 logical/physical 行为、错误与异步 GC |
| KV | RangeStream | Kubernetes 请求形状兼容 | P1：对齐 etcd 3.7 支持的通用请求形状，或明确返回 Unimplemented |
| Watch | create/cancel/progress/history/prevKV | 兼容核心语义 | P0：用官方客户端做事件完整性、压缩、断线恢复和慢消费者测试 |
| Lease | grant/revoke/keepalive/ttl/list | 兼容核心语义 | P0：补并发、故障转移、事务附着及错误矩阵 |
| Auth | 用户、角色、权限、token | 缺失 | P1：实现 etcd Auth API；DBaaS mTLS/IAM 不能替代客户端期望的 key-range RBAC |
| Cluster | MemberList | 兼容表面 | 返回 KubeBrain 服务成员信息 |
| Cluster | add/remove/update/promote | 平台替代 | 由 DBaaS 控制面扩缩 KubeBrain、PD、TiKV；RPC 保持明确 Unimplemented |
| Maintenance | Status | 部分兼容 | P1：返回真实服务身份、版本、leader/revision；容量转到实例指标 |
| Maintenance | Snapshot | 平台替代 | 使用 TiKV BR/PITR；控制面提供备份、恢复和导出任务，不伪造 etcd snapshot |
| Maintenance | Defragment | 平台替代 | TiKV GC/compaction 管理，不执行 bbolt 碎片整理 |
| Maintenance | Alarm/DbSize | 平台替代 | 用 PD/TiKV 容量、磁盘、region 和配额告警；etcd 专属字段保持可解释值 |
| Maintenance | Hash/HashKV | 部分兼容 | P2：确定客户端用途；需要逻辑校验时实现可分页、固定 revision 的内容摘要 |
| Maintenance | MoveLeader/Downgrade | 平台替代 | 分别由服务选主和 DBaaS 升级编排处理 |
| Concurrency | Lock/Election recipes | 待验证 | P1：使用官方 `client/v3/concurrency` 黑盒测试，通常无需新增 RPC |

`Status.Version = 3.7.0` 只表示协议能力门槛，不能作为完整兼容声明。发布说明必须
引用本矩阵和自动化兼容测试结果。

## 实施顺序

### P0：数据正确性

1. 从 `/root/etcd/server/etcdserver/txn` 和 `api/v3rpc/key.go` 提取 KV/Txn
   行为表，建立同一组请求分别访问 etcd 与 KubeBrain 的差分测试。
2. 补齐通用 Txn：compare 读取与写提交必须处于同一可串行化判定中；成功
   分支全部写操作共享一个 revision；失败不得部分提交。
3. 覆盖 Range、DeleteRange、Watch、Lease、Compact 的错误码、header、历史
   revision 和边界输入。
4. 在真实 TiKV/PD 三节点后端运行故障注入和线性一致性检查。

P0 完成标准：官方 client/v3 的核心 KV/Watch/Lease/Txn 行为矩阵无已知语义
差异；差分、race、故障转移和持久化复读均通过。

### P0 实施记录

- **point compare 的不存在 guard**：对照
  `/root/etcd/server/etcdserver/txn/txn.go` 的 `applyCompare` 和同一 MVCC
  写事务执行方式，KubeBrain 现把“键不存在”也作为 `TxnGuard` 状态提交。
  `Version/CreateRevision(key)==0` 后创建同键，以及 compare 键与写键不同的
  事务，都会在同一个存储 batch 中断言缺失或精确 tombstone；并发创建会使
  整个事务重试并重新选择分支。覆盖测试：
  `TestTxnApplyAbsentGuardOverlappingPut`、
  `TestTxnApplyAbsentGuardDisjointFromWrite`、
  `TestTxnApplyAbsentGuardAcceptsTombstone`。
- **尚未解决**：范围 compare 的 phantom insert。当前范围内容在 RPC 层读取，
  但 batch 只 guard 已观察到的点键；compare 后在区间内插入一个新键仍可能
  使原分支错误提交。该项必须由 TiKV 事务范围读冲突或可持久化的区间 epoch
  guard 解决，不能仅给已有键补 CAS。

### P1：通用服务能力

1. 实现 etcd Auth 用户、角色、key-range 权限和 token 生命周期，并与每实例
   mTLS 配合。
2. 验证 `client/v3/concurrency` 的 mutex、election、session 失效和 leader
   切换语义。
3. 建立 DBaaS 控制面契约：创建、扩缩、升级、备份、恢复、证书轮换、销毁。
4. 建立实例级限额和计量：CPU、内存、PV、备份容量、网络、QPS、watch 数、
   value/txn 大小；限额错误必须稳定且可观测。

### P2：运维兼容和长期验证

1. 明确 `etcdctl` 命令兼容表，为平台替代命令返回可操作提示。
2. 增加 BR/PITR 恢复演练、滚动升级、跨可用区故障、磁盘满和长时间 soak。
3. 引入基于操作历史的线性一致性验证；大规模性能测试不能替代正确性证明。

## 提交规则

每个兼容性提交必须同时包含：

1. 对应的 etcd 源码或测试位置；
2. 修复前可复现的差异测试；
3. KubeBrain 单元测试和官方 client/v3 黑盒测试；
4. 涉及存储提交、watch 或 lease 时的真实 TiKV/PD 验证；
5. 本文矩阵状态更新。

不把 etcd 内部 Raft 实现、WAL、bbolt、member reconfiguration 机械移植到
KubeBrain。这些职责已由 TiKV/PD 或 DBaaS 控制面承担；需要对齐的是客户端可
观察的数据语义、错误契约和服务可靠性。
