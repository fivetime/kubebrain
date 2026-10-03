# 固定候选鉴权差分验收（2026-10-03 UTC）

结果：`TestAuthDifferentialAgainstEtcd` **通过，20.98 秒**，测试进程及外层脚本
均退出 0，无跳过。只证明此现有差分用例，不代表整个数据语义与鉴权类别、
故障恢复或本次交付完成。

## 候选与环境

- 产品源码：`6c295888a8ef5ea0763eb8d0b1c0597b83211a3e`。
- 镜像：`ghcr.io/fivetime/kubebrain@sha256:76e25a1e6dea2fcd9e4bffdc01317d1c74ef43dcd0d6e797f40a257a3c48b1b8`。
- 测试入口工作树提交：`c5b40e20858ad87339ee9122898c2bd12882dad8`。
- etcd 对标提交：`5cd9f4ee13801e18825d661e5005ae599460bc3a`；脚本核验 etcd
  与 etcdctl 二进制来源通过。
- 授权测试集群，命名空间 `kubebrain-dbaas-test`；独立临时 Pod
  `kubebrain-auth-6c-20261003`，UID `b2a82d6a-b97b-449f-97a0-95496ee774a3`。
- 后端 `kb-local-pd.kubebrain-dbaas-test.svc:2379`，使用既有 TopoLVM 本地盘
  TiKV/PD；隔离 keyspace `auth-6c-20261003-isolated`，不是共享实例 keyspace。
- 单副本临时实例，测试连接通过本机回环 port-forward；沿用现有入口的 HTTP
  测试模式，**不证明 mTLS 部署下的鉴权行为**。共享实例 TLS 配置没有修改。

## 复现入口与结果边界

部署同一候选的独立空 keyspace 实例，广告客户端 URL 设为
`http://127.0.0.1:23379`，将其客户端端口映射到本机回环 23379。确认本机
12379/12380 未占用，然后运行已有入口：

```sh
GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8 \
  KUBEBRAIN_AUTH_DIFF_ENDPOINT=http://127.0.0.1:23379 \
  ALLOW_DESTRUCTIVE_AUTH_DIFFERENTIAL=true \
  bash hack/etcd-client-compat/run-auth-differential.sh
```

该用例全局启停鉴权并压缩历史，只能针对独立、空、可销毁实例运行。
测试覆盖权限与错误语义、权限变更、令牌失效、嵌套事务、受保护租约、
Watch/KeepAlive/RangeStream 鉴权切换和维护接口权限；完整断言以
`hack/etcd-client-compat/auth_differential_test.go` 为准。它不证明多副本故障下
的鉴权恢复，也不代替实际备份恢复验收。

## 原始证据与清理

本机证据目录：`/root/.local/state/kubebrain/auth-6c295888.kL3M5BPc/`。
包括 `auth-real.log`、`pod-running.json`、`product.log`、测试后六类状态 JSON
和 `evidence.sha256`。测试日志 SHA-256：
`d4bcd10ab937397d95f91531a6cc934c9a8374e89d67341c6cab8328d972e2bc`。

脚本前置空状态检查及后置清理检查均通过；另行保存的 RPC 结果确认鉴权
关闭、用户/角色/键/租约/告警为空。核对 UID 后删除临时 Pod，并停止其
port-forward；本地 reference etcd 由脚本退出清理。未创建 Service/PVC，
未删除产品数据卷或重新初始化磁盘。隔离 keyspace 的内部元数据没有做
原始 TiKV 范围删除，不能把上述清理描述为物理擦除。

共享 `kubebrain-local` 保持原基线 revision `kubebrain-local-568bd68448`，
3 副本 Ready；用户已有 `docs/dbaas_acceptance_status_cn.md` 修改未动。
