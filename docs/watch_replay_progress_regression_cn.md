# Watch 回放与旧进度标记的交接

2026-09-18，提交 `7fa4a6aa` 的探针 CI
[35349468426](https://github.com/fivetime/kubebrain/actions/runs/35349468426)
在 `TestClientFilteredWatchProgressCoversSuppressedPut` 失败：watch 已消费
revision 33，随后收到 progress revision 32，被完整性检查取消。失败在
作业内的测试断言，不是 Runner 接单失败。镜像作业是独立门禁，不能用其
成功替代该探针测试。

本地原测试连续 30 次通过，未复现随机调度窗口。代码检查发现，后端先
订阅 live 队列，再从缓存或历史回放事件；交给 `processEvents` 时，已回放
版本之前的事件会被过滤，但之前入队的旧 progress marker 却原样送出。
这能产生与 CI 相同的倒退；CI 日志本身不能证明具体线程调度。

新增确定性测试 `TestProcessEventsDropsProgressBeforeReplayFloor`：模拟
回放已到 33、live 队列仍有 marker 32。修复前实际输出包含 32，断言失败；
修复只过滤小于固定起点 `revision-1` 的 marker，保留等于边界的 marker。
不随 live 事件提升过滤边界，因此真实 live 倒退仍暴露给 RPC 层；没有
删除或放宽 `TestWatchRejectsRegressingProgressRevision` 对应的产品检查。

这不是 TiKV 隔离故障或原始 KeepAlive 连续性验收；不改变 30 秒门限，
也不改变测试集群的镜像、存储、选主参数或事务模式。

本地修复后验证全部通过：

- processEvents 两项测试 race 5 轮：1.759s。
- 原 CI 失败测试 race 50 轮：11.438s。
- RPC 拒绝 progress/batch 倒退测试 race 5 轮：1.503s。
- 后端全量：54.505s；etcd 服务全量：146.090s。
- 两个包的 `go vet` 和 `git diff --check`。

这些本地结果不覆盖上次 CI 的失败终态。记录时旧镜像作业
35349468415 仍运行，暂未推送，以免取消它；修复提交仍需自己的 CI。
现场只读复核仍为 generation 56、3/3 Ready、原固定镜像且 pprof 关闭。
