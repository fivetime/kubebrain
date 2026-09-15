# 滚动验收的只读诊断采样

诊断不是验收结果，不得延长原操作次数、完成窗口或 Watch 恢复门限。
`rollout-diagnostic-sampler.sh` 只在 stable 阶段调用可信回调，每次最多
60 秒、最多八次；结束后再次核对阶段。失败或阶段变化不能作为有效样本。

现有 `capture-rollout-tls-metrics.sh` 采集 KubeBrain 指标、探针进度和运行身份。
若不设置 `DIAGNOSTIC_TIKV_RECEIPT`，行为不变。

## 可选 TiKV 指标

将 `DIAGNOSTIC_TIKV_RECEIPT` 设置为仓库外、已核验身份记录的绝对路径。
记录必须来自本次受控环境的预检，不能沿用旧 Pod 重启前的身份，也不能把
采集失败后的新身份自动当作原基线。格式如下（示例占位符需替换）：

```json
{
  "format": "kubebrain.tikv-metrics-receipt.v1",
  "namespace": "test-namespace",
  "namespace_uid": "pinned-namespace-uid",
  "statefulset": {"name": "kb-tikv", "uid": "pinned-statefulset-uid"},
  "pods": [
    {
      "name": "kb-tikv-0",
      "uid": "pinned-pod-uid",
      "container": "tikv",
      "image": "the-actual-spec-image-reference",
      "imageID": "the-actual-runtime-image-id-ending-in-sha256-and-64-hex-digits",
      "containerID": "the-actual-runtime-container-id",
      "restartCount": 0,
      "startedAt": "the-actual-running-start-time"
    }
  ]
}
```

`pods` 必须按 ordinal 从零排列，覆盖控制器的全部副本（1–9 个）。控制器须
已观察当前 generation 且全部 Ready。采样核对命名空间、控制器 UID、Pod UID、
owner、spec image、容器名、实际 imageID/containerID、restartCount 和 startedAt。
采样前后均核对身份；控制器 generation 变化同样使本次采样失败。

辅助脚本仅执行容器内**已有** curl，访问固定回环地址
`http://127.0.0.1:20180/metrics`，不创建 Pod、不安装工具、不更改配置。
该方式适用于已验证的 TiKV 状态端口；其他端口或 TLS 配置不能直接套用。
每个响应最多 4 MiB，curl 最多 10 秒，命令最多 20 秒，仍受外层整个回调
60 秒截止约束。TiKV 可选采集失败会使整个回调失败，不留下成功完成标记。
原有目录不能覆盖。

输出位于本次样本的 `capture/tikv/`，含冻结的身份记录、前后身份、采集时间
和各 Pod 指标。只有再次核验两份样本的运行身份、实验阶段及先后顺序后，
才能计算单个 Pod 的计数器差值；不能拼接跨重启计数器，也不能把无效样本
或缺失指标补成零。

单次累计指标、不同 Pod 非原子抓取、或基线下的采集通路测试都不能证明候选
版本的延迟根因。Raft 同步、persist、commit 和 RPC 等阶段存在重叠，不能相加。
响应详情统计可能包含后台任务、重试和多个 Region，不能直接等同逻辑 Put。

定向回归：

```sh
go test -race ./hack/production -run '^TestCaptureRollout(TiKV|TLS)Metrics' -count=1
```

这条命令不替代完整部署工具回归或真实滚动验收。
