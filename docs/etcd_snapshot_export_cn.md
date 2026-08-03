# 导出可由官方 etcd 恢复的快照

KubeBrain 的在线存储是独立 TiKV/PD，不存在可直接流式复制的本地 etcd bbolt backend。
`Maintenance.Snapshot` 因此仍返回 `Unimplemented`。对于需要从 KubeBrain 迁移到官方
etcd 的场景，可以先生成全 keyspace 的 `kubebrain.logical.v2` 制品，再离线转换为带
SHA-256 尾部、可由官方 `etcdutl snapshot restore` 消费的 etcd 3.7 backend snapshot。

```bash
ENDPOINT=http://kubebrain:3379 \
PREFIX=/ \
OUTPUT=kubebrain-full.jsonl \
BATCH_SIZE=1000 \
  hack/backup/logical-export.sh

kubebrain-logical-etcd-snapshot \
  --input kubebrain-full.jsonl \
  --output snapshot.db \
  --acknowledge-auth-disabled

etcdutl snapshot status snapshot.db -w json
etcdutl snapshot restore snapshot.db --data-dir restored.etcd
```

转换器只接受：

- 当前 `kubebrain.logical.v2` 格式；
- 精确的全 keyspace 前缀 `/`，避免把局部备份伪装成完整 etcd snapshot；
- 每个 lease 都含 `granted_ttl`；
- create/mod revision、version 和 snapshot revision 之间关系可成立的记录；
- 操作者显式传入 `--acknowledge-auth-disabled`。

输出保证：

- 官方 3.7 backend schema、完整 bbolt bucket 和文件 SHA-256；
- snapshot revision 与 KubeBrain 导出点一致，包括巨大的 TiKV TSO revision；
- 当前 key/value、create revision、mod revision、version 和 lease ID 保持一致；
- lease 的 granted TTL、导出时剩余 TTL 与 attached keys 可恢复；
- 下一次官方 etcd 写入从 `snapshot revision + 1` 开始；
- snapshot 点之前的历史明确视为已压缩，而不是伪造不存在的历史版本；
- 输出使用原子 no-clobber 发布，并拒绝覆盖既有文件。

限制：

- 逻辑制品不包含密码哈希、auth revision 或 token signing state，因此输出 snapshot 强制
  `auth disabled`；不得把它当作保留 KubeBrain 认证配置的迁移方式。
- member、Raft/WAL、cluster ID 由 `etcdutl snapshot restore` 为目标集群重新生成。
- 这是一条离线迁移/导出路径，不是同步 `Maintenance.Snapshot` RPC，也不是 PITR、增量备份
  或 TiKV 物理灾备的替代品。
- `kubebrain.logical.v2` 本身仍不能直接交给 `etcdutl`；必须先通过转换器。

发布门禁应至少包含：转换器单元与 race 测试、官方 snapshot status、官方 restore、恢复后
etcd 启动、当前 KV 元数据对比、`snapshot+1` 写入 revision、compacted 边界，以及带 lease
样本的 ID/TTL/attached-key 验证。
