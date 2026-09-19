# 标签同步修正后的完整实验（2026-09-19）

产品候选 `2ad79751ebc35291ed8caac144a6e73442b927a6`，工具源码
`96df8958`；原 30 秒响应及诊断窗口、默认 2PC、独立本地盘后端不变。
私有目录 `fault-label-convergence.I4rP4zl4`，实际尝试
`deploy-execute.Y4XWGiWz`。

四个部署阶段及标签准备均验证通过。本轮进入真实隔离实验，但故障
控制器退出 124，清理退出 0；**整体失败**，不是生产就绪结论。

## 已取得与缺失的证据

- 故障开始时钟为 `1789783154713476358` ns。
- 同进程策略 present 观察通过；采集并匹配到 PD 2379 端口 903 条、
  TiKV 20160 端口 1144 条丢包事件。计数仅描述采样窗口。
- 继任者通过健康成员在故障后 `28.892579802` 秒观察到，member
  `3358157933`、term 206。
- 原始流记录响应时间 `2026-09-19T01:59:43.677014567Z`，距故障开始
  `28.963538209` 秒，TTL 10、term 206。这是日志时间核对，并不代替
  未执行完的整套断言。
- `stack.JTLRycIy` 已保存命名空间、StatefulSet、Pod 前快照，但
  受保护栈探针输出为空，没有 goroutine 文件、后快照、摘要或 COMPLETE。
  控制器在原总窗口内尝试诊断时返回 124，未得到所需的降级栈证据。

继任者被观察到时窗口仅余约 1.107 秒；该事实解释了诊断预算紧张，
但尚不能单凭它归因于某个网络请求、哈希操作或服务端步骤。不得把
诊断挪到窗口外仍称原验收通过，也不得补造缺失的栈或持久化断言。
后续应检查诊断调度与耗时证据，保持原安全断言和截止时间。

## 恢复状态

原执行器会话 61286 已终止，最终退出 124（保留故障失败结果）。
四个逆序恢复阶段均验证通过，`recovery-exit-code=0`；临时 Secret
清理验证通过，`secret-cleanup-exit-code=0`。故障清理退出 0。
独立复查 `secret-cleanup.4iDoqzEN` 验证原 StatefulSet UID/spec、
3 Ready/updated、原 Secret UID/resourceVersion/data 不变，以及临时
Secret/策略不存在；当前 generation/observedGeneration 均为 106，
原固定镜像已恢复。采集的 Pod 列表无故障 owner 标签。

临时卷规划 12 例及控制器 6 例离线测试通过；只读规划
`scratch-cleanup.8jYwSWYl` 确认 42 个本轮独占、已释放且未挂载的
临时卷。执行 `scratch-cleanup.F7dKTLmH` 退出 0，42 卷已回收，
所有非目标 PV UID/spec 保持不变，命名空间 Pod UID/spec/容器状态
前后相同。独立复查 `scratch-cleanup.fWs6ye6L` 目标归零。
本地 StorageClass 现为 12 Bound / 68 Released；68 个剩余卷受基线
保护（包括本轮滚动释放的 6 个原 Bound 临时卷），不批量回收。
删除的临时卷数据不可恢复。编译残留经 `compiled-cleanup.ylsxeVns`
只读核验，执行 `compiled-cleanup.g1VKykKi` 退出 0：按已记录摘要、
设备/inode 和非活动进程检查，删除 44 处本轮构建的 ELF 文件；源码、
证书、日志和证据保留。二进制可从源码重建，旧二进制摘要清单则已
退役，不得用于重新运行或消费此实验目录。
