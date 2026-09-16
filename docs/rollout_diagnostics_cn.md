# 滚动验收的只读诊断采样

诊断不是验收结果，不得延长原操作次数、完成窗口或 Watch 恢复门限。
`rollout-diagnostic-sampler.sh` 只在 stable 阶段调用可信回调，每次最多
60 秒、最多八次；结束后再次核对阶段。失败或阶段变化不能作为有效样本。

现有 `capture-rollout-tls-metrics.sh` 采集 KubeBrain 指标、探针进度和运行身份。
若不设置 `DIAGNOSTIC_TIKV_RECEIPT`，行为不变。

## 临时候选验收后的恢复

普通 `TARGET_IMAGE` 升级默认在失败时恢复原配置，成功时保留候选版本。
专用测试集群需要成功后也恢复时，可显式设置
`RESTORE_ORIGINAL_AFTER_SUCCESS=true`。该选项不启用 1PC 或 async commit，
不改变源提交协议、请求次数、完成窗口或延迟门限，因此可用于默认 2PC 的
临时候选验收。仍需正常的变更授权、不可变目标镜像和运行时摘要。

此选项默认 false，只接受 true/false；启用时必须有 `TARGET_IMAGE`，并拒绝
OBSERVE_ONLY、HARD_FAILOVER、HTTP readiness 或连接老化迁移的组合。
既有临时 1PC/async commit 模式仍自行保证成功后恢复，不依赖此选项。
恢复复用原 UID/resourceVersion/spec 围栏、收敛期限及逐 Pod 运行身份核验；
漂移、恢复失败或清理失败仍使执行器失败，不因探针先前成功而放行。
独立后端观察器仍在恢复与清理完成后停止。执行前后应另行核对完整 spec、
实际运行镜像和本轮资源清理回执；此开关不替代真实恢复证据。

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
已观察当前 generation，默认还要求全部 Ready。采样核对命名空间、控制器 UID、Pod UID、
owner、spec image、容器名、实际 imageID/containerID、restartCount 和 startedAt。
采样前后均核对身份；控制器 generation 变化同样使本次采样失败。

辅助脚本仅执行容器内**已有** curl，访问固定回环地址
`http://127.0.0.1:20180/metrics`，不创建 Pod、不安装工具、不更改配置。
该方式适用于已验证的 TiKV 状态端口；其他端口或 TLS 配置不能直接套用。
每个响应最多 4 MiB，curl 最多 10 秒，命令最多 20 秒，仍受外层整个回调
60 秒截止约束。TiKV 可选采集失败会使整个回调失败，不留下成功完成标记。
原有目录不能覆盖。

### 失去就绪状态时的诊断

独立故障观察器直接调用 `capture-rollout-tikv-metrics.sh` 时，可显式设置
`DIAGNOSTIC_TIKV_ALLOW_UNREADY=true`。默认（未设置、空值或 `false`）仍要求
控制器及容器 Ready；其他非空值一律拒绝。该选项只允许从尚在 Running 的
同一容器采集指标，不放宽任何身份、重启次数、generation 或副本数校验，
也不会使停止运行的容器成为有效样本。

输出 `allow-unready` 记录实际模式，前后快照保留控制器 readyReplicas 和容器
ready 状态。采样中途就绪状态变化仍使样本不完整；该模式的成功不证明健康，
更不证明验收通过。不要自动将失败后的新容器身份替换成既有计数器基线。

此选项不会改变现有 stable-only 调度器的生命周期。需要观察故障及恢复时，
独立调用方必须另行提供整体超时、停止/回收、阶段记录及执行器清理职责隔离，
不能仅设置这个变量就声称已经覆盖故障窗口。后端身份连续也不等于工作负载
阶段连续；跨稳定、故障和恢复阶段的计数器差值不能作为某一阶段的性能结论。

### 独立后端观察器

`observe-rollout-backend.sh` 是独立的只读观察进程，不启动执行器、不部署、不回滚。
用 `bash` 调用，依次传入三个绝对路径：本轮专用证据目录、可信可执行采样回调、
执行器的 `diagnostic-phase.json`。回调收到新样本目录和本次 phase-before.json，
必须自行校验后端身份及指标；未知阶段不得妨碍它采集诊断。

观察器在专用目录中新建 `backend-observer/`，拒绝复用已消费目录。阶段文件尚未
出现、无效、超限或含多个 JSON 文档时记录 unknown；回调仍执行。仅回调成功且
前后合法阶段快照相等时写 `phase-consistent`。这只是两个快照一致，不代表样本
健康或验收通过；cleanup 阶段一致也不表示恢复已完成，不提供中间无变化的证明。

默认最多 180 次、2400 秒，每次回调最多 60 秒，样本间隔 10 秒；对应变量为
`BACKEND_OBSERVER_MAX_SAMPLES`、`BACKEND_OBSERVER_MAX_SECONDS`、
`BACKEND_OBSERVER_SAMPLE_SECONDS`、`BACKEND_OBSERVER_INTERVAL_SECONDS`。
上限分别是 180、2400、60、30，须为无前导零的正整数。回调超时先 TERM，1 秒后
可强制终止；整体边界同样受该终止宽限影响，不改变执行器的任何验收期限。

外部所有者在专用证据目录创建 `backend-observer-stop` 表示停止。当前采样有自身
超时，停止标记不会中断它；TERM/INT 则终止并回收当前回调。停止退出 0，耗尽观察
边界退出 75，信号退出 143/130；这些都不是执行器或验收结果。回调失败单独记入
`callback.exit` 后继续观察，不把失败样本提升成有效阶段证据。

集成时，执行器必须独占回滚/清理职责，观察器与其并行，执行器不应等待采样后再
恢复。应在执行器完成自身恢复/清理后停止并回收观察器，并分别保存两者退出码。
阶段路径必须来自本轮执行器的证据目录，禁止自动搜索旧实验目录；阶段未知时不能
根据后端 Ready 或候选镜像自行猜测。

现有滚动执行器可通过 `ROLLOUT_BACKEND_DIAGNOSTIC_SAMPLER` 显式接入，值须为
可信回调的绝对可执行路径；默认不启动。它不同于原 `ROLLOUT_DIAGNOSTIC_SAMPLER`
的 stable-only 回调。执行器在建立退出清理职责后、准备镜像前启动独立观察器，
直接传入本轮阶段文件，不解析旧日志或搜索其他实验。

启用时执行器另外创建私有目录，输出 `BACKEND_OBSERVER_DIRECTORY=...`；该目录
始终保留，不受 `KEEP_RUNTIME_EVIDENCE` 删除内部运行证据的影响。执行器先完成
自身恢复/fixture 清理和镜像预拉取清理，再创建停止标记、等待观察器并保存
`executor-result` 和 `observer-result`。观察器的失败或边界耗尽不改写执行器退出码，必须单独审查
证据完整性；当前样本的限时等待可能延长清理后的进程退出，不应延迟恢复本身。
若原阶段文件已被清理，末尾采样记录 unknown，不能称为恢复阶段完整证据。

### TiKV 独立回调配置

仓库提供可执行回调 `hack/production/capture-rollout-backend-metrics.sh`。
将其绝对路径设为 `ROLLOUT_BACKEND_DIAGNOSTIC_SAMPLER`，并显式导出以下环境：
`KUBECONFIG`、`KUBECTL_CONTEXT`、`KUBEBRAIN_NAMESPACE`、
`DIAGNOSTIC_NAMESPACE_UID`、`DIAGNOSTIC_TIKV_RECEIPT`。后两项必须对应本轮预检
核实的命名空间与 TiKV 身份记录；可用 `DIAGNOSTIC_KUBECTL_BIN` 指定 kubectl。
若要采集非 Ready 后端，再显式设置 `DIAGNOSTIC_TIKV_ALLOW_UNREADY=true`。
不要将阶段文件当作 TiKV 身份记录：独立回调从专门的 receipt 环境变量读取身份，
只由观察器判断应用阶段，因此 unknown 或 cleanup 阶段仍可采集同一后端。

该回调只执行既有的 TiKV 指标采样器，不要求应用探针或 KubeBrain Info 端口就绪，
也不自动更新身份基线。独立调用时需要调用方提供整体超时；通过观察器调用时受
观察器的单次限时约束。仅启用回调不会授权部署，执行器的全部变更许可和验收门限
仍须满足。一次回调成功不代表连续观察已覆盖故障窗口。

TLS 联合回调的输出位于本次样本的 `capture/tikv/`，独立回调输出位于 `tikv/`，
均含冻结的身份记录、前后身份、采集时间
和各 Pod 指标。只有再次核验两份样本的运行身份、实验阶段及先后顺序后，
才能计算单个 Pod 的计数器差值；不能拼接跨重启计数器，也不能把无效样本
或缺失指标补成零。

单次累计指标、不同 Pod 非原子抓取、或基线下的采集通路测试都不能证明候选
版本的延迟根因。Raft 同步、persist、commit 和 RPC 等阶段存在重叠，不能相加。
响应详情统计可能包含后台任务、重试和多个 Region，不能直接等同逻辑 Put。

定向回归：

```sh
go test -race ./hack/production -run '^Test(CaptureRollout(TiKV|Backend|TLS)Metrics|RolloutBackendObserver|RolloutDiagnostic)' -count=1
```

这条命令不替代完整部署工具回归或真实滚动验收。
