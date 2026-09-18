# 无效 Watch 创建响应头的 revision

## 失败与原因

源码 e994f2c3 的回归 CI 35326113607 在 Watch race 分组失败：
TestWatchInvalidCreateAutomaticIDAndUnknownCancelKeepStreamAlive 预期
Put 已返回的 revision 2，但某个控制响应的 revision 为 0。此前本机对该
原始用例的 100 次 race 未复现，不构成 CI 误报证明。

无效范围拒绝路径使用 GetPublishedRevision（事件发布进度），而后台事件
发布可以晚于 Put 提交返回；该路径在正常创建的同步步骤之前返回，尚无
controlRev 补齐这个差距。新测试固定已提交 revision 为真实 Put 返回值，
但用 BackendShim 包装将可见发布进度保持为 0。旧代码接收循环和直接 Start
两条路径均稳定失败：预期 2、实际 0（70781，终态 1，包耗时 0.237 秒）。
这解释了原断言的时间窗口；原 CI 日志缺少响应索引，不能声称已从日志唯一
确定是哪一帧。原测试现在增加索引、ID、Created/Canceled 和原因的诊断，
不改变断言。

对照本地 etcd 5cd9f4ee13801e18825d661e5005ae599460bc3a：
server/etcdserver/api/v3rpc/watch.go 的创建结果（包括错误）使用
newResponseHeader(watchStream.Rev())；storage/mvcc/watcher.go 的 Rev
读取 watchable 存储 revision，不是消费者已经收到的事件水位。

## 修复边界

两处无效范围拒绝响应改用现有 committedResponseRevision：取本地已提交
revision 与已同步 controlRev 的较大值。没有新增存储读取或 leader 网络
调用，没有改变拒绝原因、Watch ID、权限校验顺序或流存活行为。
事件发布进度、Watch 起始 revision 和 progress 水位均未改动，不能以
控制响应的 revision 推断某个 Watch 已交付了该 revision 的全部事件。

测试还通过官方 client/v3 Put 和其实际 gRPC 连接上的原始 Watch 流检查
线上编码的拒绝响应；固定发布进度为 0 时仍应返回 Put 的提交 revision。
这是 bufconn 协议黑盒，不是独立 TiKV/PD 集群验收。此修复只处理无效范围
的两条拒绝路径，未把所有错误控制响应统一改写，也未放宽 Watch 回归门限。

修复后的两项定向测试 30 次 race 通过（89342，3.906 秒），随后 vet/diff
通过。加入 gRPC 黑盒后的三项测试五轮 race 通过（74609，1.873 秒）。

与失败 CI 相同选择器的完整 Watch/read-barrier race 分组通过（15539，
29.849 秒），随后 vet/diff 通过。日志位于私有
credential-expiry-txn.FXNmj6Yf/watch-fixed-regression.log。未重跑全 etcd
非 race 或 Auth/Lease 全分组，不把上述范围写成所有服务回归通过。
镜像 CI 35326113519 最后检查仍运行，修复尚未推送或部署。
