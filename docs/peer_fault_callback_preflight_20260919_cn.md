# 回调权限预检后的故障实验（2026-09-19）

私有目录 `fault-callback-preflight.EjsouyNl`，工具源码 `5dcd35d2`，
产品候选 `2ad79751ebc35291ed8caac144a6e73442b927a6`。原 30 秒门限、
默认 2PC、独立本地盘后端不变。82 个离线用例、镜像审核、预拉取及
清理、最终准入均通过。实际回调为 0700，冻结和准入均检查执行权限；
权限丢失负例已确认在集群调用及消费 HOLD 前被拒绝。

四个部署阶段通过，实际尝试 `deploy-execute.N7KUR0qD` 的故障控制器
退出 1，清理退出 0。原会话 42501 已结束，最终退出 1，四阶段逆序
恢复及 Secret 清理均退出 0。本轮验收仍为失败。

## 已观察到的证据

- 故障起点 `1789787148113528310` ns；继任者观察 sample-48 的决定时间
  `1789787177149563139` ns，差值 29.036034829 秒。权限问题未复现。
- 原 KeepAlive 日志仅有 expired_preflight 与 request_sent，没有成功
  response；清理时收到 Canceled。未执行完整响应及持久化断言。
- 基线及等待栈完成，等待栈 `stack.7IjGirup` 的摘要校验通过。输入校验
  约 0.675 秒、前快照约 0.351 秒、探针约 0.306 秒、后快照约 0.107 秒。
- 匿名通道 `stack-session.Tg7a06TY/forward-18585.log` 在
  03:05:47.411272 UTC 记录连接被对端重置，随后报告 lost connection to pod。
  该事件早于故障起点，发生于等待栈的匿名请求之后。等待栈探针仍完成
  TLS 拒绝证明及正向栈请求；两条独立通道不能保证匿名转发跨采集存活。
- 未产生第三个（降级）采栈目录。会话库在创建目录前要求两个后台
  转发仍为存活的本 shell 子任务；失效匿名通道无法满足该条件。

现有证据证明匿名转发失效、降级栈缺失，不能仅凭退出 1 排除创建采集
目录之前的其他时间检查，也不能将本轮称为产品超时或通过原门限。
下一步应测试匿名拒绝通道的生命周期管理，保留每次捕获的 TLS/SPKI/
匿名拒绝、前后完整进程身份及原截止时间约束，不在已消费目录中热修复。

## 恢复与清理

独立复查 `secret-cleanup.V3yle0Sv` 验证原 StatefulSet UID/spec、原
Secret UID/resourceVersion/data 不变，临时 Secret/策略已不存在。
generation/observedGeneration 为 122，3 Ready/updated，无故障 Pod 标签。

临时卷规划 12 例和控制器 6 例离线用例通过；只读规划
`scratch-cleanup.ocJ7Osks` 后，执行 `scratch-cleanup.Ib7fbIwi` 退出 0，
精确回收 42 个本轮临时卷。所有非目标 PV UID/spec 及命名空间 Pod
UID/spec/容器状态保持一致。再次复查 `scratch-cleanup.qamTu3g0` 目标归零。
本地卷现为 12 Bound / 80 Released，剩余均受基线保护，不批量回收。
已回收临时卷数据不可恢复。编译清理 `compiled-cleanup.NZFLYEhB`
退出 0，核对摘要、设备/inode 和非活动进程后删除 44 处构建的 ELF。
源码、证书和证据保留；二进制可重建，旧二进制清单已退役。

仓库修正 `4bd1eea8` 在故障前成功采栈后替换匿名转发，三轮会话 race
测试通过。尚须完整控制器接入及真实验证，不能把单元测试计作本轮
故障验收通过，也不能忽略仅剩约 0.964 秒的诊断预算问题。
