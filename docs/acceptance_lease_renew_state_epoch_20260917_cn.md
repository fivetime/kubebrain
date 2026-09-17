# 续租状态锁等待期间的领导权校验

2026-09-17，基于 `66b42fcd` 继续检查续租入场。原代码在等待状态锁之前
校验领导权；普通续租及无关回收快速路径均可能跨越主从切换后仍报告续租成功。
这与[取消入场修复](acceptance_lease_renew_state_admission_20260917_cn.md)
是两个不同问题，不能用取消测试代替领导权边界验证。

## 确定性复现

新增 `TestLeaseRenewRejectsDemotionAtStateAdmission`。测试先持有状态锁，
通过 Context.Done 在 `LockContext` 的 select 求值处发出等待信号，确认
请求已越过入口 epoch 校验；随后使其失去领导权并释放状态锁。不以 sleep
推测协程位置，不修改 TTL 或等待门限。测试直接调用续租内部路径，不声称是
真实集群故障注入或完整客户端转发测试。

产品未修正时，ordinary 返回 nil 错误，teardown-bypass 返回 renewed=true；
两个用例均失败（session 81178，包耗时 0.068s）。最终测试还覆盖仍为 leader
但 epoch 从 7 变为 8 的重新当选，要求拒绝旧 epoch 的工作。

## 修改

- 普通续租取得初始状态锁后重新检查 fresh leadership 与 epoch。
- 无关回收快速续租在状态锁保护内重新检查，同一租约不在旧 epoch 中延长。
- 普通续租最终更新 deadline 前再次检查，覆盖检查点收尾以及第二次状态锁
  等待跨越领导权变化的情况。
- 失败释放已持有的锁，普通路径返回原 `errLeaseDemotedDuringRenew`，继续由
  既有调用层决定代理或失败；快速路径返回未续期，走原安全回退。

固定参考 `/root/etcd` 源码 `5cd9f4ee13801e18825d661e5005ae599460bc3a` 的
`server/lease/lessor.go:Renew` 在 lessor 读锁内检查 primary 并取得租约。
KubeBrain 的独立 peers/lease 锁与 epoch 模型不同，本次补充的是其跨锁等待
校验，不宣称两个实现逐行相同或所有分布式故障已覆盖。

## 验证状态

完整服务测试通过（session 96101，146.324s）；四个边界用例各重复二十次
race 通过（session 7633，3.439s）。扩展续租/KeepAlive 主从切换/授权续租
race 重复三次通过（session 18978，6.024s；该次编译尚未加入两个新 epoch
用例，最终四用例以上述 session 7633 为准）。vet 通过（session 23363）。
另以 CI 相同筛选条件 `(Lease|Revoke|Expiry|Checkpoint|Attachment)` 对
`f7b18d9b` 完整执行一次 race（8 分钟超时），通过，耗时 80.989s
（session 38176）。该筛选匹配 346 个顶层测试，子用例不计入此数量。
上一提交的 CI `35220675194` 仍在运行；不为本地未验证修改取消它。
未构建或部署新镜像，原集群保持恢复后的基线，原 60 秒租约清理失败不改判。
