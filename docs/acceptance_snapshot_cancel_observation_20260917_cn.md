# 快照流取消测试的计时边界

2026-09-17，精确源码 `1f0986c1f69ed963949e30be0bbbd7ae7a089d18` 的 CI
`35223543694` attempt 1 在服务整包测试失败（140.176s）。失败用例为
`TestMaintenanceSnapshotCancellationInterruptsStalledStream/after_first_response`：
取消后 250ms 内未返回。后续 Auth/Lease/Watch race 和完整探针没有执行。
自动发布审计 watcher 73716 退出 1，没有执行镜像审计或部署。

## 已证明的测量问题与尚未证明的原因

旧夹具在首块历史数据发送完成后通知测试取消；此时消费者尚可能没有完成
同步的本地 bbolt Builder 初始化。因此原 250ms 不只测量阻塞流等待的取消，
还可能包含文件初始化和调度时间。Builder 当前使用 NoSync；不能将此次失败
武断归因于 fsync、磁盘故障或 Runner 故障。

原测试本地 race 重复二十次通过（session 9867，2.407s），没有直接复现 CI。
隔离 Go overlay 仅让 Builder 初始化前延迟 300ms，保留旧取消时机和门限，
after_first_response 即失败（session 79384，0.368s）。这是受控复现的可致失败
测量边界，不是 CI 唯一原因的证明；CI 没有记录 Builder 阶段耗时。

## 测试修正及负例

仅修改测试和 CI，不修改产品快照、文件同步或取消逻辑：

- 首块之后，先等待 Builder 成功初始化，再发出取消；首块之前的用例仍直接
  在流停顿后取消。保留原 250ms 取消门限和 5s 准备门限。
- 保留 300ms 慢初始化，确保初始化时间不会混入取消测量。
- 临时目录由测试主协程创建；准备失败、断言失败也会取消、释放夹具并回收
  snapshot 协程；初始化提前退出会报告实际错误。
- CI 新增快照取消专用 race 命令，并更新 workflow 合约检查，避免仅普通
  整包覆盖这条路径。

另一隔离 overlay 仅移除产品在后续历史流等待中的 ctx.Done 分支，保留测试
修正：用例仍明确失败（session 79262，0.615s）。因此修正没有把真正的流
取消失效改成通过。实际产品文件未被 overlay 替换或修改。

workflow 合约 race 和服务 vet 通过（session 12237，合约 3.086s）。
修正后的二十次 race 通过（session 38070，8.538s）；完整服务测试通过
（session 73891，144.740s）。新 CI 结果仍待验证，不把本地通过写成发布成功。
这不证明任意阻塞文件 I/O 可在 250ms 内取消，也不修改线上验收门限。

CI 完整失败日志位于
`/root/.local/state/kubebrain/release-1f0986c1.KByj7KaL/probe-failure-35223543694.log`，
SHA-256 `3755cb5a9cad66cc2ff2d97ff8e703f7fb9ed1f9e216143b9e608f1d20503507`。
受控 overlay 证据保存在 `snapshot-cancel-boundary.B2Ta9d6v` 私有目录。
原镜像作业 `35223543461` 独立继续；即使成功也不能绕过本次失败的回归门禁。
