# 匿名诊断通道重建后的故障实验（2026-09-19）

私有目录 `fault-anonymous-rearm.uDtThDsG`，冻结工具 `57b7433c`，
产品候选 `2ad79751ebc35291ed8caac144a6e73442b927a6`。沿用独立本地盘
PD/TiKV、默认 2PC 和原 30 秒故障门限。85 个离线用例、镜像审核、
预拉取及清理、最终准入均通过；四个部署阶段通过。

实际尝试 `deploy-execute.jclo6sAy` 的故障控制器退出 **124**，故障资源
清理退出 **0**。本轮完整验收失败。执行会话 **30903 已终止，退出 124**；
四阶段逆序恢复与 Secret 清理均退出 0，独立恢复复查通过。

## 时间与采集证据

- 故障起点：`1789789601885963980` ns。
- 原始探针日志记录 response 于 `2026-09-19T03:47:10.981312391Z`，
  即故障后 **29.095348411 秒**，TTL 10、响应 term 253。这是原始记录，
  不能替代控制器尚未执行的完整响应 term/身份及持久化断言。
- successor sample-49 观察结束于 `1789789631039991308` ns，即
  故障后 **29.154027328 秒**。
- 基线 `stack.bsudijZ9`、等待栈 `stack.1nfhizLP` 均有 COMPLETE，
  摘要复查通过；两者各有一次成功的 `rearm-anonymous` 阶段。
- 降主采集 `stack.P6KD1GJd` 已创建：输入校验开始于
  `1789789631.087711` 秒，此时原窗口剩余约 **0.798 秒**；
  校验结束于 `1789789631.745958` 秒，耗时 **0.658247 秒**。
  随后的 `snapshot-before` 阶段退出 124，没有 COMPLETE，亦未进入
  受保护栈探针阶段。只有 namespace/部分 STS 文件及计时轨迹，不能
  宣称降主身份、TLS 拒绝或栈分类完成。

与上一轮相比，此次通过了两个通道的采集前存活检查并开始第三次采集，
没有重现故障前匿名转发失效导致无法创建降主采集目录的问题。但这不
证明第三次 TLS 交互成功，也不证明产品已满足完整 30 秒验收。
当前失败直接定位于剩余预算不足以完成前置诊断；不能扩大门限、删除
身份/摘要检查或用原始响应时间替代完整验收。下一步须在恢复完成后
评估领导权切换与诊断关键路径的时间分配，保留全部安全和持久化断言。

## 恢复与清理边界

全程跟踪同一会话至终态，未重启或修改冻结运行时。
`secret-cleanup.eyViarNA` 独立核验原 StatefulSet UID/spec 和原 Secret
UID/resourceVersion/data 不变；generation/observedGeneration 均为 130，
3 Ready/updated，临时 Secret/策略不存在，三个 Pod 无故障标签。

临时卷规划 12 个用例、控制器 6 个用例在本轮独立通过
（`HmlrPkDo` / `rKhd5Bci`）。只读规划 `scratch-cleanup.vtWqXQfz`
确认 42 个精确目标后，`scratch-cleanup.g2YqAJhW`（session 46572）
执行退出 0，回收本轮 42 个已释放临时卷；数据不可恢复。所有非目标
PV UID/spec 及命名空间 Pod UID/spec/容器状态不变。再次只读复查
`scratch-cleanup.KJq90nlc` 目标归零，本地卷为 12 Bound / 86 Released；
剩余均受基线保护，未批量回收，也未操作旧 Ceph 实例。

编译残留只读规划 `compiled-cleanup.S7E0Zxhv` 确认 44 处已知摘要的
本轮 ELF；`compiled-cleanup.GLx6YADP`（session 25625）退出 0，逐一
核验文件身份及非活动进程后删除。源码、证书、日志与证据保留，二进制
可重建；依赖这些已删除二进制的旧清单现已退役，不据其缺失重跑实验。
本轮没有仍在运行的执行/清理会话，也没有启动后续实验。

## 后续代码检查：退让通知的 peer 预算分配

对发送关键路径的检查发现独立问题：串行访问配置 peer 时，首个无响应
peer 原先能用尽全部发送预算，后面的健康 peer 不会被尝试。新增真实
mTLS HTTP/1 和 HTTP/2 回归，在 `4ac62296` 旧源码上两种协议均失败
（私有证据 `peer-retirement-budget-regression.1pQhq3Tk/before-fix.log`）。
这证明该问题可复现，**不证明它是本轮集群时序的原因**。

发送器现在仍在一个总截止时间下串行访问，每个 peer 至多一次，但
将剩余时间除以剩余 peer 数作为本次上限。父级取消终止后续访问，
本次或总预算过期后的 204 都不作为成功。没有额外后台请求、重试队列、
新 holder 读取或 claim 替换；mTLS/pin、后端 scope/CAS、不可逆退让及
正常租约选举兜底均保持。单个慢 peer 可能比以前更早超时，这项修改
解决后续健康 peer 无机会被访问的问题，不承诺所有单次通知都会成功。

验证：`go test -race ./pkg/server -run '^TestPeerRetirement' -count=3`
通过（105.302 秒）；补充迟到 204 测试后，发送器及预算边界三轮 race
通过（8.101 秒）。`pkg/server/service/leader` 与 `pkg/backend/election`
完整 race 分别通过（4.224 / 2.017 秒），三包 vet 及 diff 检查通过。
边界测试覆盖较短父级截止时间、不同尝试的共同截止约束、冻结 claim
不变、取消不再访问后续 peer、迟到成功拒绝。此修改未构建发布镜像、
未运行 CI 或部署，不能据此宣称原 30 秒故障验收已经通过。
