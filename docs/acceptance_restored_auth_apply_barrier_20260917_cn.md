# 恢复后跨成员认证矩阵：应用进度前置条件

2026-09-17，`4ada3d15` 的回归 CI `35213373516` attempt 1 **失败**。
失败发生在全探针 race 的 `TestRestoredAuthTokensAcrossAllMemberPairs`：
`snapshot_auth_cross_member_test.go:175` 的 Watch Created 且未 Canceled 断言
不成立。此前记录 source=1、token_index=19、actual_entry_index=20。
旧断言没有输出目标成员、Created/Canceled 明细或 CancelReason，不能据此
认定具体取消原因，也不能将这次失败归因于 Runner。

新增控制面夹具（含 Pod 替换）、服务普通测试、Auth/Lease/Watch race、代理、
revision 与日志采集均成功；全探针包最终失败（289.814s），不得部分通过替代
整体 CI 成功。没有部署新一轮共享后端测试，也未直接重跑失败 CI。

## 源码依据与修正范围

固定参考 `/root/etcd` 的 `server/etcdserver/apply/backend.go` 向认证 token
分配传入 ConsistentIndex，而不是当前 ApplyingIndex；`server/auth/simple_token.go`
仅等待 token 后缀中的 index。已有受控三成员实验
`TestRestoredAuthDelayedFollowerApplyWindow` 已证明：目标成员应用该认证
日志前，同一 simple token 的 Watch/KeepAlive 可以失败；应用后成功。
这一机制与本次 CI 现象一致，但旧诊断不足以证明两者原因完全相同。

修正仅在 `_test.go`：将原矩阵明确为“应用完成后的成员互通”，对每个目标
成员等待已捕获的真实 Authenticate entryIndex，而非 tokenIndex。等待计入
原有 5 秒操作 context；不增加 RPC 重试、不刷新 token、不改产品 JWT 认证
或生产恢复验证器策略。仍要求全部 9 条 Watch 与 9 条 KeepAlive 成员边通过。
Watch 失败诊断补齐 source/target、Created/Canceled、原因及 entry/applied index，
不输出 token 内容。

负向传播窗口测试继续使用同一 token：保持 follower 暂停时，真实 RPC 必须
失败，新增应用屏障也必须超时；释放暂停后，屏障与 RPC 必须成功。未删除
原有失败路径或将错误视为通过。新增屏障单测覆盖零 index、已取消 context、
前一个 index 不足以放行、超时及实际应用后放行，并在退出前回收测试 goroutine。

## 本地结果及后续门限

矩阵、受控延迟及屏障三项 `go test -race -count=3 -timeout=5m` 通过，
session 45117 退出 0，65.869s。随后仅整理单测 goroutine 回收方式；屏障单测
10 轮 race 通过（1.381s），探针包 go vet 通过，session 59460 退出 0。
随后在最终 `e87ee17695b65123ed30e1dcec7312d3b9228dad` 上运行完整
`go test -race -count=1 -timeout=20m ./hack/production/cmd/rollout-availability-probe`
通过（session 47250 退出 0，321.603s）。新 CI `35215373370` 后来失败在
最先执行的服务整包测试，尚未执行认证矩阵所在的探针包；详见
[周期进度测试修正记录](acceptance_watch_progress_observation_20260917_cn.md)。
不能把本地整包成功写成远端验收通过，也不能将另一测试失败认作认证修正失败。
这些结果不是新的 CI 成功，也不能追溯改变 `35213373516` 的失败结论。

完整旧 CI 日志归档 session 93870 退出 0：
`replacement-candidate-4ada3d15.GBTxJG95/ci-failure.9eZFHtyL/run.log`，SHA-256
`378f2cdc4feb873b258bcf70be3ee366ca308a7165565799569be5c31b75ac01`。
旧准备目录继续 HOLD，旧源码/CI 准入不再可用；需新源码 CI 完整成功、重新
核验只含测试差异，以及原租约自然归零，才能准备共享后端 Pod 替换对照。
