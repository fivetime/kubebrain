# apiserver Watch 完整性门禁

此前 standalone runner 仅等待 MODIFIED 总数达到 `OBJECTS * UPDATES`，重复事件
可能掩盖缺失事件；in-cluster runner 逐行调用 `json.loads`，不能解析 kubectl
`-o json` 的多行事件对象。即使修改为能解析 JSON，仅把重新 list 的 ADDED
计入更新集合，也不能证明中间版本通过 Watch 送达。

现在两个 runner 在停止并等待 Watch 输出进程退出后，共用
`hack/dev/verify-apiserver-watch.jq` 解析整个 JSON 流：

- MODIFIED 的 `(name, data.version)` 必须精确等于 `soak-1..OBJECTS × 1..UPDATES`，
  不能有缺失、重复或多余组合。
- 数据事件必须来自指定 namespace、指定 ConfigMap 名，具有非空且稳定的 UID。
- MODIFIED resourceVersion 严格递增，按十进制字符串长度与字典序比较，不转换为
  可能丢失大整数精度的浮点数。
- ERROR、DELETED、未知类型和残缺 JSON 都失败；BOOKMARK 不计入更新。
- ADDED 不补足 MODIFIED。即使允许进程重连，也不会放宽完整性门限。

等待循环的文本计数仅用于判断何时尝试最终校验，不再作为通过依据。in-cluster
入口已去除按行 JSON 解析及其 Python 依赖，使用 jq 的完整流门禁；它的原始
`observed_updates` 日志字段在等待阶段现在只是 MODIFIED 计数，不能单独引用为
完整性证据。最终绿色必须包含 `integrity: passed`。

本门禁针对每次单对象 patch 都产生独立 revision 的测试工作负载，不应直接用于
同事务多 key、资源重建或允许过滤更新的其他 workload。初始 ADDED 的 revision
无需按输出顺序递增，避免把列表返回顺序当作提交顺序。

回归覆盖完整多行流、大于 2^53 的 revision、BOOKMARK、初始 ADDED、未知对象、
重复补缺、额外重复、缺失、错误 namespace、UID 重建、错误 kind、revision
倒退/重复/非法、以重列举代替修改、ERROR 和截断 JSON。

已用仓库校验器重放上一轮真实 v1.36.1 原始流（SHA-256
`b55399ca96af15eec91489423358d01923741b4a15a360ab15644609d4149741`），
20 对象 × 10 更新通过。此为保存证据的重新核验，不是重新部署或新一轮故障验收。
真实接入边界见[原验收记录](acceptance_local_apiserver_v1361_20260916_cn.md)。

本地 `hack/etcd-client-compat` 测试：runner/apiserver/version-matrix 相关 race
测试通过（9.151 秒）；最终完整性与接线测试重复三次通过（1.429 秒）。两个 shell
入口通过 `bash -n`，`git diff --check` 通过。本轮未变更测试集群。
