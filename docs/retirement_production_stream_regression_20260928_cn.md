# 生产计时下的过期续租流回归：2026-09-28

状态：本地产品回归通过；原 30 秒真实 TiKV/PD 故障验收仍未完成。

## 修补的覆盖缺口

已有 `TestPeerRetirementCampaignProductionTimers` 只验证从故障到释放锁回调；
完整过期续租流测试则采用 4 秒 RenewDeadline。因此不能由两项分别通过，
推断完整原请求在生产选主计时下满足 30 秒要求。

新增 `TestPeerRetirementPendingExpiredStreamProductionTimers`，复用完整网络
夹具，使用 LeaseDuration=30s、RenewDeadline=25s、RetryPeriod=500ms。
从存储故障注入前固定 30 秒期限；要求新主就绪、租约及附着键、响应任期、
原始流唯一续租响应全部在期限内完成。旧成员存储此时仍不可用，不重新发送
该请求、不重建流。之后另行验证路由提示过期后的续租和存储恢复。

夹具使用 memkv、存储接口故障注入和真实 loopback gRPC/HTTP2 mTLS。
外围一分钟 RPC 生命周期包含准备与后续恢复，不延长上述独立 30 秒期限。
etcd 对标提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a`：
`server/lease/lessor.go:396` 的 Renew 在过期撤销未完成时等待，失去主身份后
返回 ErrNotPrimary；`server/etcdserver/v3_server.go` 的 renewLease 随后转发。
此回归不是 etcd 差分运行，也不是磁盘或 TiKV 网络隔离证明。

## 执行与证据

```sh
GOFLAGS='' GOWORK=off GOTOOLCHAIN=go1.26.8 go test -race -count=1 -timeout=180s ./pkg/server \
  -run '^(TestPeerRetirementFullServerNetworkLeaseHandoff|TestPeerRetirementPendingExpiredStreamDuringStorageFailure|TestPeerRetirementPendingStreamWithReloadedProxyCredentials|TestPeerRetirementPendingExpiredStreamProductionTimers)$' -v
```

- 四项测试（包含 enabled/default-disabled 子用例）全部通过，包耗时 68.306s。
- 生产计时用例：故障到原响应 26.008040805s；数据、任期等检查完成于
  26.008180974s。完整测试 34.30s 包含后续续租、恢复及夹具退出，不能将其
  与故障窗口混用。夹具关闭时两个选主循环均取消，原有 Close 逻辑记录了
  等待后继超时；该测试日志保留此信息，不作为真实集群清理通过证据。
- 基于 `6f4495d20079214eaf5e6623a1c58d779896e460` 加本次测试补丁运行。
  与已发布产品源码 `1e862c110a82081e68ffa9612de4893d74decbc6` 对比，产品
  路径只有该测试文件变化，没有修改运行逻辑或依赖；本次测试尚无新 CI 覆盖。
- 测试文件 SHA-256：`212b5d0f849749703cf40ac4d7798c52c8cec08a9ecd928343b625d9d9117631`。
- 完整日志：`/root/.local/state/kubebrain/retirement-production-stream.4B8RE0PX/network-regression.log`。
  SHA-256：`e9eb46369334a60d2812b6f99e4e19cf9e046b20146e8633bf181e0a3d94f1f9`。

未部署候选、注入集群故障或启用 1PC/async commit。实验 peer-retirement 路径
在测试中显式开启，默认关闭路径保持原断言。下一项仍是补齐具体执行入口并
在专用集群完成同候选、原 30 秒用例及恢复；不以此回归替代正式验收。
