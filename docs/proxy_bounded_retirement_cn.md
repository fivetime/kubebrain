# 核心请求的有界旧连接退出

## 问题与复现

[96a00480 故障实验](acceptance_apiserver_fault_96a00480_20260917_cn.md)中，2000 次
ConfigMap 更新通过，但 apiserver 自身的 coordination Lease Txn 返回 leader changed。
入口旧 peer health 被取消后，代理立即关闭共享连接；这可能中断同连接上的在途请求。
该实验日志不能证明这笔真实 TiKV 事务是否提交，不能据此安全重放。

使用真实 gRPC 服务构造独立复现：旧 peer 先记录一个模拟已提交的副作用，再延迟响应；
发布健康 successor，并要求新连接先就绪，最后释放旧响应。旧实现 10 次对照中：
不切换连接均成功；切换连接均丢失响应并返回 leader changed；旧副作用始终一次，
successor 始终未执行该请求。这确认了代理自身可制造结果丢失，但不是 TiKV 提交结果
确认协议，也不说明所有故障窗口错误都来自这一机制。

参考 `/root/etcd` 提交 `5cd9f4ee13801e18825d661e5005ae599460bc3a` 的
`server/etcdserver/v3_server.go:processInternalRaftRequestOnce`：提出写提案后等待
apply 结果、上下文结束或服务器停止，不是仅凭 leader 身份发布就取消结果等待。
etcd 的 Raft 提交模型不等同 KubeBrain 的 TiKV 事务模型，不能将其结果推断方式直接套用。

## 实现约束

- 范围是走核心单次转发封装的 Put、DeleteRange、Txn、Range；不扩展为通用写重试。
- 在同一代理锁内检查当前可用连接并增加在途引用，避免“取得连接后、标记占用前”被关闭。
- 重置时立即从新请求路由移除旧连接，并通知旧 stream generation。没有在途核心请求则
  立即关闭；否则保留原 RPC 获取响应的机会，新请求可以连接 successor，无须等待旧请求。
- 默认退休计时预算 1 秒；最后一个引用释放即提前关闭。最多保留两代退休连接，快速切换
  时强制关闭最老的一代。计时回调受调度和锁竞争影响，不宣称硬实时关闭保证。
- 超时、代数上限和代理 Close 可强制结束旧 RPC，结果仍可能不确定。Close 强制关闭所有
  退休连接并等待计时回调完成；关闭错误独立加锁累计。已移除的列表项清空引用。
- 核心调用通过 defer 释放引用，包括异常退出；调用方取消/deadline 仍有效，不人为延长。
  流不增加退休引用；现有通知和各自恢复/完整性规则保留，不能靠长流无限延长旧连接寿命。
- 没有修改 TiKV 事务提交、leader fencing、权限检查、默认 2PC 或外部验收门限。
  只有已有的“明确未准入”内部信号允许原有有限重试；通用 leader changed、Canceled、
  Unavailable 仍不能证明未提交，禁止为了可用性直接重放。

## 验证范围

回归包含：旧响应保留且 successor 不重放、其他请求错误触发重置、最后一个引用才关闭、
退休超时、调用方取消/deadline、Close 强制结束、快速切换代数上限、stream 通知，
以及计时到期/引用释放/Close 并发竞争。既有不确定 Txn 不重放测试继续执行。

开发阶段初版代理全包 race 10 轮通过（34.347 秒），etcd 非 race 全包通过（141.408 秒）；
新增 deadline 与列表引用清理后的最终代理全包 race 10 轮通过（45.974 秒），
leader race（2.568 秒）、build race（3.920 秒）、proxy go vet 通过。
不将初版 etcd 全包结果算作最终源码的全包验证；最终源码还须通过 CI 的既定完整范围。
私有复现及日志：`/root/.local/state/kubebrain/txn-retirement-diagnostic.6jAV7BB5/`。
旧实现红测是诊断证据，不是通过结果。

最终源码 `602912b7ec8614175a7707aeecb349bda2682f75` 的
[回归 CI 35177817480](https://github.com/fivetime/kubebrain/actions/runs/35177817480)
attempt 1 已成功。原始日志确认上述六个新增顶层测试及既有不确定 Txn 不重放测试
实际执行通过，包括 caller deadline 子用例。etcd 非 race 全包 132.048 秒，
Auth/Lease/Watch race 分组分别 85.218、72.737、24.222 秒；代理全包 race 5.317 秒，
完整 rollout probe race 331.763 秒，均通过。
日志与摘要保存在私有 `release-602912b7.jiuiZPn9/probe-ci.log`、`probe-ci.sha256`。
这证明既定 CI 范围通过，不是额外全包全 race 或真实集群故障验收通过。

镜像 CI [35177817589](https://github.com/fivetime/kubebrain/actions/runs/35177817589)
同一源码 attempt 1 成功；独立核验进程退出 0，检查了 OCI 多架构索引、平台摘要、
发布标签、amd64 二进制版本/源码及 TiKV 客户端依赖。固定镜像为
`ghcr.io/fivetime/kubebrain@sha256:89df69763ed64b1e5c2215a7052a2320de1b3318739bb752204d56967e003cd3`。
私有证据：`release-602912b7.jiuiZPn9/audit.kaKB1pZC/verified.json`。
这是镜像身份核验，不代替运行时验收。

本修复的[专用集群故障验收](acceptance_apiserver_fault_602912b7_20260917_cn.md)
完成 2000 次 ConfigMap 更新，但整体因租约清理失败退出 70；内部 Lease Txn 仍失败，
并记录了故障前 Txn Range Count 不一致和故障后内部 Watch 超时，原镜像已恢复。
对端真正死亡、提交后丢失响应、超过退休预算、
频繁切换触发淘汰等情况仍可能返回不确定结果。它消除的是可避免的代理主动中断窗口，
不宣称实现 exactly-once，也不宣称已经解决所有 Kubernetes 写可用性差距。
