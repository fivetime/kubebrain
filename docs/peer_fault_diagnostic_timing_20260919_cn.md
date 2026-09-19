# 带分阶段计时的故障实验（2026-09-19，失败后已恢复）

本轮私有证据目录 `fault-diagnostic-timing.m64Fa1W2`，产品候选仍为
`2ad79751ebc35291ed8caac144a6e73442b927a6`，工具冻结于 `fbeffa72`。
原 30 秒验收窗口、默认 2PC、独立本地盘后端和全部安全断言不变。
仅补充采栈会话阶段及探针请求计时，不据此宣称修复了上轮超时。

## 准备与准入证据

- 初始 StatefulSet generation/observedGeneration 106、3 Ready/updated，
  UID/spec 与已恢复基线一致；本地卷 12 Bound / 68 Released。
- PD 同进程前后核验，精确集群 ID `7686251028133611667`，keyspace
  `kubebrain-local`。六个工具从冻结源码重新构建，没有复用已退役二进制。
- 三成员独立证书新建，叶证书有效期至 2026-09-20 02:19:38 UTC；
  原/扩展信任材料逐成员离线验证通过。集群挂载的公有证书另行核验。
- 候选镜像索引和子摘要、来源提交、三项 CI 及隔离版本输出审核通过。
- 50 个运行时依赖路径及恢复/清理依赖清单冻结；部署材料预检
  `stage-preflight.jCiQZ4Ls` 通过。
- 82 个离线用例全部通过，包含取消与清理失败。准备期间曾因漏拷贝
  测试辅助文件停止；补齐后重新运行通过，没有触及集群。
- `prepull-execute.Rc6c7a4Q` 主流程/清理均退出 0，独立重新查询确认
  三个临时 Job 名称及其 Pod owner UID 已不存在；原工作负载未变。
- `final-admission.741hvJt0` 通过，HOLD 已消费，原执行器会话 37325
  负责部署、故障、逆序恢复和临时 Secret 清理，不得重复启动。

## 当前状态与判定边界

四个部署阶段均通过（最终 generation 110），实际尝试目录
`deploy-execute.wORrJpwC`。故障控制器 `run.exit=1`、`cleanup.exit=0`；
原会话 37325 已结束，最终退出 1；四阶段逆序恢复和临时 Secret 清理
均退出 0。本轮仍为失败，不因恢复成功而改变验收结论。

失败原因是新目录的 `successor-status-request.sh` 为 0644，未保留原
脚本的执行权限。继任者观察器要求回调是可执行的普通非符号链接文件，
在创建观察目录和发起 Status 请求之前拒绝执行。因此没有 successor
目录、降级栈或完整响应/持久化断言；不能将此归因于产品 30 秒性能。
原 KeepAlive 流在清理时被取消，未取得成功响应。这是准备与准入遗漏，
不是继续放宽门限的理由。摘要校验不覆盖执行权限，离线 fixture 又为
模拟回调设置了权限，故此前 82 个用例未覆盖实际目录的这一遗漏。

故障前基线采栈计时及摘要验证有效：输入摘要校验约 0.750 秒，前置
资源快照约 0.655 秒，受保护探针约 0.209 秒，六个阶段总跨度约
1.873 秒。探针内 ping/匿名请求/栈请求分别约 0.020/0.015/0.128 秒。
这些仅是故障前样本，不证明故障窗口内耗时或真实超时根因。

后续新实验须保留回调执行权限，并在部署前调用仓库观察器的
`--check-callback /绝对路径/回调脚本` 检查；实际观察时仍重复检查。
预检不运行回调、不访问集群、不修复权限，也不代替源码摘要绑定。
当前已消费的冻结目录不得修改后重跑。生产就绪目标仍未完成。

## 独立复查与残留清理

`secret-cleanup.qB8wBpiP` 独立复查通过：原 StatefulSet UID/spec、
原 Secret UID/resourceVersion/data 一致，临时 Secret 与故障策略不存在。
generation/observedGeneration 均为 114，3 Ready/updated，无故障 Pod 标签。

临时卷规划 12 例和控制器 6 例离线测试通过。只读规划
`scratch-cleanup.Up5MqDLL` 后，执行 `scratch-cleanup.rajUdwOQ` 退出 0，
精确回收 42 个本轮独占临时卷，所有非目标 PV UID/spec 与命名空间
Pod UID/spec/容器状态不变。再次复查 `scratch-cleanup.F2YtDbwY` 目标归零。
当前本地卷 12 Bound / 74 Released；剩余卷均受基线保护，不能批量回收。
已回收临时卷数据不可恢复。编译清理 `compiled-cleanup.MJn9cOhM`
退出 0：按摘要、设备/inode 和非活动进程核验后删除 44 处构建的 ELF
文件，源码、证书与证据保留。二进制可以重建，旧二进制清单已退役。
