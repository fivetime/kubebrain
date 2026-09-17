# 周期进度响应测试的观察时序

2026-09-17，源码 `e87ee17695b65123ed30e1dcec7312d3b9228dad` 的
CI `35215373370` attempt 1 失败在最先执行的服务整包测试（140.490s）：
`TestPeriodicProgressTickRehomesQuietSelfFencedGeneration` 的一秒 Eventually
未满足 `len(responses)==1`。完整服务失败后，后续 Auth/Lease/Watch race 和
全探针检查未执行。控制面夹具 race 已通过（9.657s），不代表 CI 整体成功。

CI watcher 54654 退出 1；等待部署的 coordinator 73068 随即退出 75，HOLD
仍在，无 admission/deployment claim，没有修改集群。原租约观察独立继续。

## 可复现的测试缺陷与边界

本测试允许每 5ms 发送周期进度。安静 Watch 可以多次报告同一合法 watermark，
不要求测试 goroutine 恰好在第一条与第二条之间运行。KubeBrain 周期分支和
固定参考 etcd `server/etcdserver/api/v3rpc/watch.go` 都按周期请求/发送进度。
旧断言要求缓冲区永久只含一条，一旦观察晚于第二条便无法再成功。

原测试本地 `-race -count=50 -cpu=1,4` 共 100 次通过（session 67873，
15.189s），未直接复现 CI 调度。只将测试观察周期改为 50ms、保留旧断言和
原一秒门限，受控复现失败（session 32447 退出 1，1.293s），失败时共记录
200 条响应。CI 没有记录当时的响应数量/内容，因此这是已证实的可致失败
时序缺陷，不是对 CI 唯一根因的无条件认定，也不是 Runner 故障结论。

## 修正

只改 `pkg/server/etcd/watch_test.go`，不改产品发送逻辑或验收延迟门限：

- 保留 50ms 慢观察和原一秒期限，要求至少三次周期响应。
- 停止并回收 Watch 后，检查**全部**已记录响应：revision 10、Watch ID 7、
  非 Created/Canceled/Fragment、无 compact revision、取消原因或事件。
- 原有 self-fence、从 revision 10 恢复、旧 generation 不得发送进度的检查保留。
- 断言失败也会 cancel 并等待 Watch goroutine 退出，避免污染后续测试；
  失败日志补充记录到的响应数量。

修正后同样 100 次 race 重复测试通过（session 18933，20.052s）。原 CI
Watch race 的名称筛选不包含 `PeriodicProgress`，这次同时补入该测试族并
更新 workflow 合约测试，确保它不仅被普通整包测试覆盖。扩展后的 Watch
race 通过（session 14886，30.518s），workflow 合约 race 通过（session
94514，2.503s），完整服务测试通过（145.503s），随后 go vet 也通过
（两者所在 session 34031 终态 0）。
尚未得到新 CI 成功结论。

完整失败日志归档 session 22362 退出 0：
`/root/.local/state/kubebrain/auth-apply-e87ee176.mAe9VOxF/ci-failure.4BkFJUw0/run.log`，
SHA-256 `61dea505541a94f86c4f7252aed8f11aa314f174b2e9df78f640f377c0bfc5ac`。
共享后端对照仍需后续固定源码的完整 CI 成功和新鲜零租约准入。
