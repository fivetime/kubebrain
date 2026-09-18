# 2ad79751 重测：核心响应断言完成，整体仍未通过

## 结论

本轮实际驱动退出 1，整体未通过。原续租流在故障起点后
28.698617070 秒返回 TTL 10、任期 156；降主采栈和键/租约持久化检查
完成。但撤销隔离后的端点采集误将 Ready 恢复判断为进程变化，后续
最终稳定性断言未执行。不能把这些已完成断言写成整个验收通过。

故障清理退出 0，四阶段恢复均退出 0，临时 Secret 清理退出 0。
最终只读复查原 StatefulSet UID 和完整 spec 已恢复，generation 与
observedGeneration 均为 82、3/3 Ready、3/3 updated、revision 一致。
整体 DBaaS 生产就绪目标仍未完成。

## 本轮身份

- 私有 owner：`/root/.local/state/kubebrain/fault-2ad-recheck.HegMKL6X`。
- 唯一实际 attempt：`deploy-execute.Pcc4VKVk`，已消费，禁止重跑。
- 产品源码：`2ad79751ebc35291ed8caac144a6e73442b927a6`。
- 镜像 index：`sha256:f2cfb61dbe27956566245ebce1ba43df203c925d3fde545d2f9c9f1e6d360794`。
- 工具源码：`287cbe46062a03b49d0d48c7511b81e63fdab822`，40 个运行文件
  及恢复、清理依赖分别冻结；运行期间未修改。
- 最终准入 `admission-check.nHA5m613` 重新确认三个对应源码 CI 成功、
  全部摘要、原配置、基线卷、原 Secret、证书有效期及新资源不存在。

原 30 秒门限、默认 2PC、选举参数不变。新 owner 使用独立的新证书、
策略和租约，不复用前一轮 claim 或创建凭证。预拉取使用精确镜像摘要，
三个隔离 Job 及 Pod 已清理；此历史缓存准备不保证未来镜像仍驻留。

## 实际观测

四个部署阶段依次完成实际验证后才运行故障驱动。原始请求只有一条
KeepAlive 流，没有应用重试或第二次连接。故障起点为
`1789774643899245084` ns，响应时刻为
`2026-09-18T23:37:52.597862154Z`。

- 健康成员观察到继任者 2176893298、任期 156，响应头任期同为 156。
- 源码绑定的过期续租等待帧从 1 变为 0。`stack.jHZhPxIe` 的采集完成
  标记在原预算内生成，完整摘要复核通过。
- 隔离撤销前读取租约 ID `1789774000123`，TTL 8、grantedTTL 10；
  预期键仍存在，值为 `fixture`，绑定该租约。
- 采样中 TiKV 20160 被拒绝 1027 条、PD 2379 被拒绝 783 条；这是
  采样窗口的证据，不外推为全故障窗口的逐包证明。

## 失败与修复范围

`measured-remove.log` 证明自有策略已删除。紧随其后的单次 absent
观察 `policy-observation.KTHdCW5R` 在 `endpoint.7az7R6au` 的 Pod
前后身份检查退出 1：只有 Ready false→true，完整 spec、UID、IP、
容器 ID/镜像 ID、重启次数 0、启动时间均未变。该脚本仍比较整个
`containerStatuses`，是前次采栈修复没有覆盖到的另一处检查。

清理流程随后重试观察并成功，但不能回填失败观察的缺失后半段证据，
更不能补写尚未执行的最终后端和目标 Pod 稳定性断言。

仓库提交 `8686572c` 将专用集群端点采集纳入版本管理，为应用和 Cilium
agent 复用同一个进程身份规则；12 个模拟场景、三轮 race 测试及 vet
通过。它未应用到本轮冻结脚本，也还没有经过新的真实实验。
详见[身份规则与验证边界](pod_process_identity_cn.md)。

## 恢复及剩余收尾

主执行进程终态 1，故障 cleanup 0；diagnostics、protocol、members、
roots 逆序恢复全部 0，Secret cleanup 0。`restoration-check.FdKj56L2`
确认原完整配置和 3/3 Ready，两个临时 Secret、自有策略及故障标签
均不存在。原失败退出码保留，凭据和原始栈仅保存在私有证据目录。

本轮临时 scratch 卷已按归属核验并回收 42 个。只读核验记录为
`scratch-cleanup.k3PJpdGr`，执行记录为 `scratch-cleanup.NhIIU4PQ`，
两者退出 0。清理前同一批工具的 12 个归属/CSI 标识测试及 6 个控制器
测试通过。逐卷重新检查历史归属、Released、无引用/挂载、独占 CSI
标识，并以 UID/RV/spec/phase 前置条件更新回收策略；全部基线及非
目标卷 UID/spec 保留，命名空间已有 Pod 的 UID/spec/containerStatuses
未变化。最终本地盘卷为 12 Bound、50 Released，临时卷数据不可恢复。
多出的六个 Released 属于实验前基线保护集合，不删除。

编译产物已按记录摘要和 inode 核验并确认未被进程使用后清理。本 owner
删除 43 个二进制路径，前一轮 owner 删除 94 个，合计 137 个（含测试
及验证器副本）。执行记录 `/root/.local/state/kubebrain/compiled-cleanup.LTHqec1t`
退出 0，`plan.tsv`、`plan.sha256` 和 `removed.sha256` 保存逐路径证据。
源码、脚本、证书、日志及原始实验记录保留；二进制可从冻结源码重编译。
旧构建摘要中这些已退役路径将不再通过文件存在性检查，不是重启已消费
实验的理由。

后续需完整检查故障阶段身份比较的使用范围，再以新 owner、新工具冻结
及原门限验收。
