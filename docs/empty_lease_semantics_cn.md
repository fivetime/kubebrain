# 空 lease 与 apiserver 清理语义对标

## 已确认的语义

删除最后一个绑定键不等于撤销 lease。对标 `/root/etcd` 提交
`5cd9f4ee13801e18825d661e5005ae599460bc3a`，`server/lease/lessor.go` 的 Detach
只移除 itemSet/itemMap 绑定，不删除 leaseMap 中的 lease；Promote 使用剩余 TTL
恢复到期时间。不能为了让测试后全局 lease 数归零而改变产品的这一语义。

已在隔离的本机参考 etcd 上真实执行：授予 3660 秒 lease、绑定键、按 prefix
删除键，TimeToLive 返回 ttl=3659、granted-ttl=3660、keys=null；同一 lease
重新绑定成功；只撤销本测试明确 Grant 返回的 lease 后，键和 lease 集合均为空。
参考 binary 源码 SHA 校验通过，runner 退出 0，进程已停止、13479/13480 无监听。
证据：`/root/.local/state/kubebrain/empty-lease-reference.h9599fhe/`。
该参考实验没有触及专用集群残留 lease。

KubeBrain 的客户端回归 `TestClientDoOpDeleteLeasedPointKeyWithPrevKVMatchesEtcd`
已补充断言：删除后的 TTL 为正、granted TTL 不变、同一 lease 可重新绑定，
随后显式撤销删除新绑定键。race 重复三次通过（1.335 秒）。这里验证单独的
删除/重新绑定行为，不将其写成真实故障后到期回收已经通过。

## Kubernetes TTL 来源的限制

本机精确版本 `k8s.io/apiserver@v0.36.1/pkg/storage/etcd3/lease_manager.go`
表明默认复用窗口为 60 秒、比例 0.05；GetLease 会在请求 TTL 上加复用窗口后
再 Grant。因此 3600 秒请求可以形成 3660 秒授予 TTL。

该计算与观察值一致，但没有保留该 lease 的具体 Grant 调用追踪，不能仅凭
3660 反推出它由哪个 Kubernetes 对象创建，也不能据此确立撤销所有权。

## 专用集群的当前观测

2026-09-16 23:24 UTC，再次只读查询故障实验残留 lease
`0002a0ac4ccc6c01`：TTL 3049 秒、granted TTL 3660 秒、keys=null。
此前 TTL 为 3480 秒；现在仍在正常倒计时，未观察到“已经到期却不回收”的证据。
三副本 Ready、后端健康、本轮 prefix 为空。所有临时查询转发已退出。
新证据单独保存，未覆盖此前观察：
`/root/.local/state/kubebrain/lease-expiry-observation.B5Jc1T4r/`。

2026-09-17 00:02:20 UTC 再次只读核验：相同 lease 的 TTL 为 770 秒，granted
TTL 仍为 3660 秒、keys=null，LeaseList 仍列出该 lease；leader term 为 6。
测试 prefix 仍为空，endpoint health 成功，查询退出 0，18380 临时转发已退出。
证据：`/root/.local/state/kubebrain/lease-expiry-observation.gm69LG3R/`。
这仍是未到期状态，不是到期回收成功或泄漏的证明。

随后执行有时限的只读观测：00:11:28–00:15:30 UTC，TTL 依次为
222、192、161、131、101、71、41、11、-1，最后 LeaseList 为 `found 0 leases`。
整个采样期间 leader 为 3358157933、term 为 6，没有因本轮观测主动切换 leader，
也没有 KeepAlive/Revoke。最终 prefix 为空、endpoint health 成功，观测退出 0，
18380 转发已关闭。证据：`/root/.local/state/kubebrain/lease-natural-expiry.14Hm4HUl/`。
**此次残留空 lease 的自然到期回收已确认**；不能据此证明所有故障/续租场景。

不放宽原 60 秒清理门禁、不主动撤销未证明所有权的 lease，也不把上一轮
“内容检查通过但清理/归档失败”重新归类为完整成功。
