# 固定候选快照历史/租约恢复（2026-10-03 UTC）

`TestKubeBrainSnapshotLeaseHistoryRestoresIntoOfficialEtcd` 在真实 TiKV/PD
后端的固定候选上通过，1.81 秒，测试退出 0，无跳过。执行了官方 etcdutl
Restore 和官方 etcd 启动，核对三次更新的历史 KV、租约绑定与 TTL，以及
Watch 历史事件和 PrevKV。此用例不包含 Auth 恢复，不能判整个备份类别通过。

## 身份与入口

- 产品源码 `6c295888a8ef5ea0763eb8d0b1c0597b83211a3e`。
- 镜像 `ghcr.io/fivetime/kubebrain@sha256:76e25a1e6dea2fcd9e4bffdc01317d1c74ef43dcd0d6e797f40a257a3c48b1b8`。
- 对标工具来源 `5cd9f4ee13801e18825d661e5005ae599460bc3a`。
- 隔离 Pod `kubebrain-dbaas-test/kubebrain-snapshot-6c-20261003`，UID
  `6b2ce401-c935-41c0-888c-45142c26ecd7`。
- 后端 `kb-local-pd.kubebrain-dbaas-test.svc:2379`，既有 TopoLVM 本地盘；
  独立 keyspace `snapshot-6c-20261003-isolated`，未与控制面租约清理共享。
- 通过本机回环 port-forward 连接临时 HTTP 实例；不证明 mTLS 场景。

在独立未压缩的实例上运行，预先确认本机 23379、42479、42480 可用：

```sh
cd hack/etcd-client-compat
GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8 \
  KUBEBRAIN_ETCD_ENDPOINT=http://127.0.0.1:23379 \
  ETCDUTL_BINARY=/root/etcd/bin/etcdutl \
  REFERENCE_ETCD_BINARY=/root/etcd/bin/etcd \
  go test -v -run '^TestKubeBrainSnapshotLeaseHistoryRestoresIntoOfficialEtcd$' \
  -count=1 -timeout=120s
```

## 两次失败及必要修正

首次失败于未压缩数据库的 `compactRevision` 断言，原测试错误要求 0；
固定对标源码 `etcdutl/etcdutl/hashkv_command_test.go` 要求 -1，改为精确 -1。
第二次失败于官方 Restore 的 SHA 校验：原入口先对待恢复原件执行 hashkv，
而对标 `calculateHashKV` 打开可写 backend/mvcc store，修改了数据库。
改为对独立副本计算 hash，新增原件摘要及逐字节未变更检查；不禁用官方
恢复校验，不修改产品或放宽门限。修正后原用例实际通过。

## 证据与清理

原始证据位于 `/root/.local/state/kubebrain/snapshot-6c295888.LRX323YD/`：
`restore-real.log`、`restore-real-fixed.log` 为两次失败，
`restore-real-copy.log` 为通过；`evidence.sha256` 固定日志及修改后测试文件。
另保存 Pod 身份、产品日志和测试后键/租约检查。

测试后键为空、LeaseList 为空；核对 UID 后删除临时 Pod，停止专属转发。
测试启动的官方 etcd 由测试退出清理，临时恢复目录由 Go 测试清理，因此未
保留快照原件，只保留执行日志，不能将该记录当作持久备份制品交付。
未创建 PVC/Service、未操作产品数据卷；隔离 keyspace 内部元数据未物理擦除。
控制面自然租约清理仍为独立运行，整体交付未完成。
