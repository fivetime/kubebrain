# 统一观察器实验：标签准备阶段失败（2026-09-19）

候选仍为 `2ad79751ebc35291ed8caac144a6e73442b927a6`，原 30 秒响应
门限、独立本地盘 PD/TiKV 和默认 2PC 均未改变。私有实验目录为
`fault-unified-observer.83IKaf7V`，实际尝试 `deploy-execute.enoJypnF`。

信任根扩展、成员独立证书、候选协议和受保护诊断四个阶段均验证通过。
故障前标签准备失败，控制器退出 70，随后故障控制器清理退出 0。
没有取得故障响应证据，本轮不能判断产品是否满足原响应门限。

## 原因与修正边界

首次采集 `endpoint.G0OxfZPN` 中，Cilium 端点处于
`waiting-for-identity`，不是采集器要求的 `ready`。其端点 ID 为 3633，
CEP 与端点身份 ID 均为 211368，IP 也一致；采集器在读取后续快照前
退出 1，标签观察器将其转换为身份观察失败。标签准备自身的回滚观察
也失败，但外层故障清理随后确认标签已撤销。

修正仅给标签观察显式启用 `--allow-identity-pending`：允许采集
`waiting-for-identity` 的完整前后快照，Pod/agent 进程、CEP UID、
端点身份、IP 和网络信息一致性要求不变。标签观察将该状态返回 75，
即使目标标签已出现也不能返回成功。未知状态、外来标签、Pod 替换
或 CEP/端点身份不一致仍失败。策略与丢包采集不启用该选项，仍要求
`ready`。运行中的实验使用冻结源码，不就地修改或重试。

## 恢复与验证状态

外层执行器最终退出 70，保留准备失败结论；逆序恢复与临时 Secret
清理均退出 0。新鲜复查 `restoration-check.prDzNpFn` 确认原完整
StatefulSet spec/UID 恢复，generation/observedGeneration 为 90，
3 Ready / 3 updated，revision 回到 `kubebrain-local-568bd68448`；
原 Secret UID/resourceVersion/data 未变，两份实验 Secret、策略及
本轮标签均不存在。执行句柄已终止，不复用本轮目录重新实验。

存储复查为 12 Bound / 98 Released；实验前为 12 Bound / 50 Released。
新增对象仍须逐一按本轮创建凭据识别和清理，不能删除原有 Released
PV 或把配置恢复等同于全部测试残留已清理。编译残留也尚未进行本轮
终态清理。

修正的第一轮端点/标签/丢包 race 测试通过
（41.444 秒）。追加拒绝场景后的端点、标签/丢包、策略/恢复等待三轮
race 回归通过（183.152 秒），`go vet ./deploy/test-cluster`、Bash
语法及 diff 检查通过。测试使用模拟 API，不是对失败现场的补验收。
