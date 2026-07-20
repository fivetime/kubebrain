# etcdctl 兼容表

本文面向把官方 `etcdctl` 指向 KubeBrain endpoint 的运维人员。对照入口为
`/root/etcd/etcdctl/ctlv3/ctl.go` 及其 `command` 子包；服务端语义继续以
`docs/dbaas_compatibility_plan_cn.md` 为准。

状态定义：

- **支持**：命令通过 etcd v3 RPC 获得可依赖结果；
- **安全 no-op**：请求成功，但 etcd 的本地 bbolt 操作不适用于 TiKV；
- **平台替代**：RPC 返回 `Unimplemented` 和替代路径，必须改用 DBaaS 控制面；
- **客户端离线**：不访问 endpoint，只处理 etcd snapshot/data-dir，不能用于
  KubeBrain artifact；
- **非生产保证**：核心 RPC 可工作，但命令本身不是 KubeBrain 发布门禁。

## 命令矩阵

| etcdctl 命令 | 状态 | KubeBrain 契约 |
| --- | --- | --- |
| `get`、`put`、`del`、`txn`、`watch` | 支持 | 通用 KV/Txn/Watch 语义由双端差分覆盖 |
| `lease grant/list/revoke/timetolive/keep-alive` | 支持 | lease revision、持久化、过期和 failover 已覆盖 |
| `lock`、`elect` | 支持 | 官方 `client/v3/concurrency` recipe 已覆盖 |
| `auth`、`user *`、`role *` | 支持 | 需实例启用 auth；管理 API、RBAC 和 token 生命周期已覆盖 |
| `compaction` | 支持 | logical/physical compaction 和历史错误语义已覆盖 |
| `endpoint health` | 支持 | 执行线性化 proposal；正常态用于 endpoint 发布门禁；active NOSPACE 时按 etcd 契约返回 unhealthy，Pod readiness 仍可正常 |
| `endpoint status` | 支持 | 真实身份、leader、revision、term；配置 quota 时返回租户逻辑用量，否则使用兼容 sentinel |
| `endpoint hashkv` | 支持 | 同一 KubeBrain keyspace/同 revision 可比较；数值不与 bbolt hash 比较 |
| `member list` | 支持（需配置） | 生产必须通过 `--initial-cluster` 注入全部 KubeBrain 副本 |
| `alarm list` | 支持 | 返回该 keyspace 持久 sticky NOSPACE 及稳定 owner member ID；启动存量超过 quota 时 readiness 前自动恢复/激活；raw ACTIVATE 的非零显式 owner 原样保留；CORRUPT 无对应语义 |
| `alarm disarm` | 支持 NOSPACE | 可从任一副本解除 list 返回的持久 owner，达到/超过 quota 时也可解除但下一次 Put 会重新激活；错误 owner、重复及并发重复解除为空操作；提交结果不确定时回读精确旧 alarm 判定 |
| `defrag` | 安全 no-op | 返回成功且不执行 bbolt defrag；TiKV 自身 compaction/GC 由存储平台管理 |
| `member add/remove/update/promote` | 平台替代 | KubeBrain 副本无本地数据，使用 DBaaS 扩缩或重配置 |
| `move-leader` | 平台替代 | 使用 DBaaS rollout/failover；数据面选主自动完成 |
| `downgrade validate/enable/cancel` | 平台替代 | 使用版本化 rollout/rollback，不启动 etcd downgrade job |
| `snapshot save` | 平台替代 | 使用 `kubebrain.logical.v2` 备份/恢复流程 |
| `snapshot restore/status` | 客户端离线 | 只识别 etcd backend snapshot，不识别 KubeBrain logical artifact |
| `make-mirror` | 支持 | 发布门禁双向验证 prefix 基线、1001-key 分页、持续增删改、`--rev` 历史重放/compacted 错误及 source/destination 双端 RBAC；跨区域长期镜像仍需独立 soak |
| `check perf`、`check datascale` | 非生产保证 | 仅为客户端负载工具；不能替代 KubeBrain 正确性、容量或 SLO 验证 |
| `version`、`help` | 客户端离线 | 只报告本地 etcdctl 二进制信息 |

`snapshot save`、member mutation、`move-leader` 和 `downgrade` 的非零退出是稳定
产品契约，不应在自动化中忽略。Auth 开启时，这些 RPC 与 etcd 一样先鉴权：未认证或
非 root 调用返回认证/权限错误；只有 root 才能看到平台替代提示。

## 生产替代入口

- 备份、恢复：`hack/backup/logical-export.sh`、
  `hack/backup/logical-restore.sh` 和 DBaaS 备份编排；
- 容量、alarm、defrag：`deploy/production/monitoring.yaml` 中的 PD/TiKV PVC、
  leader、region 和资源告警；
- KubeBrain/PD/TiKV 扩缩、升级、回滚、故障转移：DBaaS 控制面编排；
- endpoint 发布前检查：`etcdctl endpoint health`，TLS/mTLS 使用标准
  `--cacert`、`--cert`、`--key`。

发布时使用目标支持窗口内的官方 etcdctl 版本重跑本表。当前可操作错误已用官方
client/v3 live test 固定，并在真实三副本 KubeBrain + 独立 3 PD/3 TiKV 上用
`/root/etcd/bin/etcdctl` 验证。

鉴权 `make-mirror` 门禁必须使用独立 KubeBrain `--keyspace`，并显式设置
`KUBEBRAIN_AUTH_MIRROR_ENDPOINT`；测试会拒绝该值与共享
`KUBEBRAIN_ETCD_ENDPOINT` 相同，也会在两端初始 auth 已启用或存在用户时 fail closed。
`--rev`/compaction 门禁同样要求显式 disposable
`KUBEBRAIN_MIRROR_COMPACTION_ENDPOINT`，禁止在共享主实例推进 compact revision。
