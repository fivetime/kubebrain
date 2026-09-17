# 故障实验的单 Pod 日志采集

`hack/production/cmd/pod-log-capture` 是只读诊断工具，不是产品服务。
用于在故障注入**之前**打开前端成员的日志流，将已收到的日志留在私有证据目录；
不再依赖删除 Pod 后才能发现日志已无法读取。它只采集一次、一个容器，不按名称自动重连。

## 身份和完整性边界

要求显式 kubeconfig/context、namespace UID、Pod UID、StatefulSet controller UID。
打开日志流前后各核对 namespace/Pod 身份，要求目标非 terminating、指定容器 Running，
node/containerID/imageID/restartCount 一致。若期间发生替换，不发布就绪标记。
只保存这些精简身份字段，不复制整个 Pod spec、环境变量或 kubeconfig。

**Kubernetes Pod log API 没有 UID 前置条件。** 上述检查是前后身份核对，不是对 kubelet
的原子 UID 约束；容器状态更新也可能滞后。日志轮转、网络断开和进程突然退出都可能造成
缺口。因此 `complete_history` 固定为 false，不能凭 EOF、退出 0 或摘要声称日志完整。
摘要只能验证所保存字节未被改变，不能证明源端未丢日志。

日志请求为显式 container、follow、timestamps，回看 120 秒，不关闭后端 TLS 验证。
默认最多 15 分钟、64 MiB；可配置上限为 30 分钟、1 GiB。到时或容量耗尽返回非零，
已收到的前缀日志和终态保留。结束时另有最多 10 秒的只读身份检查。
SIGTERM/SIGINT 是主动停止，正常取消可以退出 0；仍然不是完整性通过。
源 Pod 结束后同名新 Pod 必须独立重新识别并使用新的证据目录，不能续写旧文件。

## 使用与故障准入

在仓库根目录编译到私有临时目录，运行形如：

```sh
pod-log-capture \
  --kubeconfig=/absolute/private/kubeconfig --context=test-context \
  --namespace=test-namespace --namespace-uid=EXPECTED_NAMESPACE_UID \
  --pod=kubebrain-0 --pod-uid=EXPECTED_POD_UID \
  --statefulset-uid=EXPECTED_STATEFULSET_UID --container=kubebrain \
  --directory=/absolute/private/owner/new-attempt \
  --duration=15m --max-bytes=67108864
```

UID 必须来自本轮新鲜读取，不能照抄历史报告。证据目录必须是新路径，父目录私有且没有
符号链接组成部分。文件权限 0600、目录 0700；JSON 同目录写入并 fsync 后通过原子硬链接
发布，拒绝覆盖已有文件。日志本身可能含敏感信息，只放私有证据区，不直接提交仓库。

驱动要为三个成员分别启动采集器，并在注入故障之前：

1. 等待三个 `ready.json`，解析并核对本轮期望身份，同时检查采集子进程仍存活。
2. 再读集群身份，核对单次故障目标 UID/resourceVersion，并检查负载和采集器仍在运行。
3. 保存旧 Pod 的流；对重建 Pod 使用新 UID、新目录启动另一条流，同时保存新 PVC/PV
   绑定快照。采集器不执行故障注入、恢复或卷清理。
4. 无论负载成功与否，停止并等待所有采集器，审核每条 `result.json` 和日志摘要。
   任何缺流、容量截断或身份错误必须如实标注，不能用驱动退出 0 代替证据覆盖检查。

产物为 `before.json`、`ready.json`、`container.log`、`result.json`。
结果区分 setup_error / read_error / byte_limit / deadline / cancelled / eof；
结束时源身份已变化会记录 `end_identity_error`，不会把旧流重新标成新 Pod。

## 验证记录（2026-09-17）

- `go test -race -count=3 -timeout=2m ./hack/production/cmd/pod-log-capture` 通过，10.510s。
- `go vet ./hack/production/cmd/pod-log-capture` 通过。
- HTTP 层测试覆盖 namespace/Pod/owner/container/restart/image 漂移、terminating、缺少
  runtime identity、部分传输、容量耗尽、主动取消、超时、结束时 Pod 替换、禁止重连、
  文件权限、拒绝覆盖、清理临时 JSON 文件和参数边界。它们不模拟 kubelet 的所有状态延迟。
- 最终原子发布版于 01:38:13–01:38:15 UTC 对专用集群 `kubebrain-local-0` 做只读 smoke：
  成功就绪、SIGTERM 正常停止、27572 字节日志摘要独立复算一致，前后 Pod UID/spec/
  containerStatuses 不变，外层及采集器退出 0。**没有重启、删除或部署 Pod，尚未用它
  完成新的故障实验。**

最终 smoke 私有证据 `/root/.local/state/kubebrain/pod-log-atomic-smoke.8w11Zlex/`，
会话 22836 已终态 0；原子发布前的 smoke 为 `pod-log-capture-smoke.FHFIsuGm/`，
会话 38524 终态 0，不混用两个版本。生成二进制按各自摘要核对后清理，原始证据保留。

下一轮实验仍需新鲜候选镜像准入、保证恢复、三成员日志采集编排及重建期绑定证据；
本工具就绪不代表 [KubeBrain 可用性差异](acceptance_apiserver_continuous_fault_20260917_cn.md)
已修复，也不能补造上一轮已经消失的 Pod 日志。
