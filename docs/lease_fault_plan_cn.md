# 原生故障实验身份配置校验

`hack/production/cmd/lease-fault-plan` 只校验实验计划的身份和指标预期部分，
不连接集群、不启动子进程、不获取认领，也没有 `--execute`。它不是完整
故障执行 CLI；成功输出明确为 `LOCAL_BINDINGS_VALID_NOT_EXPERIMENT_ADMISSION`。

```sh
go run ./hack/production/cmd/lease-fault-plan \
  --bindings /absolute/private/bindings.json \
  --approve-sha256 INDEPENDENTLY_APPROVED_SHA256
```

摘要必须来自独立审阅的配置，不应读取任意输入后即时计算摘要当作授权。
加载器要求私有普通文件、规范绝对路径、不跟随最终软链接、最大 1 MiB；
严格拒绝未知和重复 JSON 字段。

配置类型是 `leasefault.NativeExperimentBindings`：

- `version` 固定为 1；`network`、`protocol` 使用已有恢复意图的 JSON 格式，
  所有 owner、Namespace/StatefulSet 身份必须一致。
- `initial_term` 和 `observer_member_id` 是十进制字符串；protocol 中的
  cluster/member/lease ID 也使用字符串，避免 JSON 浮点精度损失。
  独立观察成员不得等于原 leader；初始配置不能指定 origin 或继任 term。
- `source` 为完整 40 位小写提交号，`source_hash` 为已审阅 lease 源文件
  的 SHA-256；`image` 必须是 `ghcr.io/fivetime/kubebrain@sha256:…`。
  格式校验不证明源码允许列表、CI 成功、镜像来源或实际 Pod 镜像匹配。
- `stack_ports` 为四个互不相同的 1024–65535 端口，顺序是 before 的
  info/anonymous，再是 after 的 info/anonymous。尚未检查端口可用性。
- `metrics` 使用 `retirementmetrics.WorkerExpectation` 的字段名：
  `Binding`、`Offset`、`Key`、`MinimumCount`、`RequireDuration`。
  `Offset` 单位为纳秒，必须小于 30 秒；最小计数必须显式填写，零也不能省略。
  各项绑定同一 Namespace/StatefulSet、spec SHA-256 和 cluster 标签；
  允许不同 Pod，具体 Pod 与 worker 命令映射仍需独立验证。

后续完整执行入口仍须提供命令及全部依赖的固定哈希、连接凭据、在线
源码/镜像/Pod/任期准入、实际认领、证据持久化、子进程 Join、故障后恢复
和恢复验证后的显式释放。此命令不替代这些要求，也不代表原 30 秒真实
验收已通过。测试覆盖本地配置及 CLI，不接触专用测试集群。
