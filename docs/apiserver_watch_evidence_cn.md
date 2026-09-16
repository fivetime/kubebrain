# apiserver Watch 原生证据归档

为避免[上一轮故障实验](acceptance_apiserver_leader_failure_20260916_cn.md)依赖外部
文件描述符采集导致原始流丢失，standalone `apiserver-watch-soak.sh` 现在直接归档。
in-cluster runner 尚未接入本功能；本变更也不追溯修复上一轮已经丢失的原始流。

默认 `EVIDENCE_DIR=${WORK_DIR}.evidence`，可显式指定，但必须是 WORK_ROOT 下
不存在的新目录，且与 WORK_DIR 不相同、不互相包含。复用已有目录、目录重叠或
目标符号链接在访问后端前拒绝。归档目录权限 0700，文件权限 0600。

退出清理顺序：

1. 停止并等待 Watch、apiserver 写入进程，完成已有的后端 prefix/lease 清理检查。
2. 将 `configmap-watch.jsonl` 和 `kube-apiserver.log` 复制到证据目录，拒绝覆盖
   已有归档或跟随源符号链接。只复制这两项，不复制 PKI、私钥或 kubeconfig。
3. 归档成功后删除 WORK_DIR；归档失败则保留原 mode-0700 私有目录并明确报错，
   避免丢失唯一证据。此时目录仍可能含私钥，需要操作者在取证后另行安全清理。
4. 保存 `result.json`，分别记录 `operation_exit`、`cleanup_failed`、
   `archive_failed` 和 `runner_exit`。清理/归档失败使最终状态为 70，即使测试
   正文已打印 completed，也不能认定整体通过。结果写入失败同样返回 70。

早期启动失败时某些诊断文件可能尚不存在，不生成虚假空白流来冒充采集成功。
归档不是生产审计日志系统；日志及测试对象内容仍可能敏感，应保留在受限目录，
不要整目录提交到仓库。

回归使用真实 cleanup 函数验证：正文失败码 42 被保留；日志归档在工作目录删除
前完成；归档失败保留工作目录并报告 70；凭据文件不被复制；拒绝覆盖和符号
链接；证据目录重叠/复用提前失败。没有重新注入故障或变更专用集群。

本地验证：runner/apiserver/version-matrix 相关 race 测试通过（9.232 秒）；
最终归档与 cleanup 回归重复三次通过（1.447 秒）；两个变更 shell 文件通过
`bash -n`，`git diff --check` 通过。尚未将新归档功能用于下一轮真实故障实验。

上一轮长 TTL 空 lease 仍需到期后独立只读确认。本功能只解决证据归档和终态
可辨识性，不延长清理窗口、不改变 lease 语义，也不把失败实验重新归类为成功。
