# KubeBrain 测试策略

## 核心原则

KubeBrain 的目标不是"内部函数看起来对"，而是**"对 Kubernetes 来说像 etcd"**。因此最有价值的测试，一定是拿 Kubernetes / etcd **官方 client 作为消费端**去打 KubeBrain，而不是自己写 mock 自证。

内部单测与 mock 有其位置，但仅用于锁住黑盒难以确定性复现的精确 bug，**不能替代**真实消费端测试，更不能作为兼容性证明。

## 测试分层（按生产就绪价值排序）

### 1. 真实 Kubernetes 消费端（价值最高）

用真实 `kube-apiserver` / `k3s` 接 KubeBrain + TiKV，验证 apiserver、watch cache（cacher）、reflector、controller 等真实使用路径。

- **测试目标**：证明真实控制面能在 KubeBrain 上正常工作。
- **覆盖场景**：LIST / WATCH、consistent-list-from-cache、分页（continue token）、namespace 删除、delete-collection、Lease/租约、滚动重连、apiserver 版本兼容矩阵。
- **测试方法**：
  - `hack/dev/apiserver-smoke.sh` —— 从 kind 节点拷出真实 `kube-apiserver` 二进制，指向 KubeBrain 起独立实例，跑 create/update/watch/label-select 冒烟。
  - `hack/dev/apiserver-watch-soak.sh` / `incluster-apiserver-*.sh` —— watch soak、集群内 apiserver。
  - `hack/dev/k3s-datastore-smoke.sh` —— k3s 以 KubeBrain 为 datastore。
  - `hack/dev/k8s-version-matrix.sh` / `apiserver-version-matrix.sh` —— 多 k8s 版本矩阵。
- **关键验证信号**：apiserver 日志无 `RequestWatchProgress feature is not supported` / `Failed to parse etcd version`；指标 `apiserver_watch_cache_consistent_read_total{success="true",fallback="false"}` 递增（一致性 LIST 走缓存、零 etcd 回退）。

### 2. etcd 官方 client 黑盒（价值很高）

用 etcd v3 官方 client（含 `go.etcd.io/etcd/client/v3/kubernetes`）黑盒调用 KubeBrain，证明 etcd v3 API 行为对官方客户端兼容。

- **测试目标**：证明 Txn / Range / Watch / Lease / Compact、错误码、revision 语义与 etcd 一致。
- **覆盖场景**：对象生命周期、分页与历史读、watch + lease、key 元数据与 compare、progress notify、compact 单调性、错误码（如 `ErrCompacted`）。
- **测试方法**：`hack/etcd-client-compat/`（普通 live compat 用 `ENDPOINT=<node>:<nodeport> hack/etcd-client-compat/run.sh`）。runner 不再隐式连接 `127.0.0.1:3379`：必须显式设置 `KUBEBRAIN_ETCD_ENDPOINT`，或使用兼容别名 `ENDPOINT`。直接执行无 endpoint 的 `go test` 时 live 用例立即 skip，静态/runner 单测仍会运行。新增兼容性回归请加到这里。reference etcd 双端差分请用 `hack/etcd-client-compat/run-differential.sh` 或 `RUN_ETCD_CLIENT_DIFFERENTIAL=true hack/dev/verify.sh`，并只对一次性实例设置 destructive approval；普通 runner 会拒绝 reference/differential opt-in 环境变量，避免误跑 Compact 差分。
- **约定**：每个修复应优先在这里加一个**打真实 endpoint** 的黑盒用例（例如：超大 mod_revision 不冻结集群、空闲 watch 的 progress 不超前、慢 watcher 收到无缺口前缀、compact 后低版本读被拒为 compacted）。

### 3. 内部单测（仅用于锁 bug，不替代 1/2）

仅当某个已知 bug 的最小复现用黑盒**难以稳定、快速、确定性**复现时，才写内部单测锁住它，防止回归。

- **适用场景**：修订号流水线卡死、goroutine 泄漏、nil panic、watcherhub 跳批、compact 水位倒退等——这些依赖内部时序/失败注入，黑盒难以稳定触发。
- **要求**：单测应能**证明修复前失败、修复后通过**（提交前用 `git stash` 或临时回退验证过），并在涉及并发时跑 `-race`。
- 现有示例：`pkg/backend/revision_leak_test.go`、`pkg/backend/watcherhub_test.go`、`pkg/backend/processevents_leak_test.go`、`pkg/backend/compact_regress_test.go`、`pkg/storage/tikv/batch_test.go`、`pkg/server/etcd/{watch,maintenance}_test.go`。

### 4. mock / 故障注入（最窄用途）

只用于覆盖很窄的错误分支（如注入一次存储读失败），**不能作为兼容性证明**。

- 示例：`revision_leak_test.go` 的 `flakyIterKV` 注入一次 `Iter` 失败，验证 update 元数据读失败路径不卡死流水线。

### 长期 soak / 故障测试

证明"不是只跑通一次"，而是在重启、leader 切换、compaction、慢 watcher、TiKV 抖动下持续正确。

- `hack/dev/{watch-soak,compact-soak,fault-smoke,compact-fault-smoke,lease-fault-smoke,ha-smoke,rollout-smoke,tikv-persistence-smoke}.sh`。

## 开发闭环

```
改代码
  → go build ./... && go vet ./... && go test ./pkg/... # 编译 + 静态检查 + 内部单测（含 -race）
  → hack/dev/up.sh 从宿主 git 注入版本、提交 SHA 和 UTC build date 后执行 docker build
  → kind load docker-image kubebrain:dev --name kubebrain-dev
  → kubectl -n kubebrain-dev rollout restart deploy/kubebrain
  → 黑盒验证：ENDPOINT=<node-ip>:30079 hack/etcd-client-compat/run.sh
  → 需要真实控制面时：hack/dev/apiserver-smoke.sh（或 k3s-datastore-smoke.sh）
```

**注意**：滚动重启刚完成时，第一次黑盒测试可能因新 leader 尚未稳定（选主 / 修订号同步）而瞬时失败，重跑即可，并非回归。

## 每个修复的验收清单

1. 是否有真实消费端（etcd client / apiserver）黑盒用例覆盖该路径？——若能黑盒复现，**必须**有。
2. 若 bug 黑盒难以确定性复现，是否有内部单测证明"修前失败、修后通过"？
3. 涉及并发的改动是否跑过 `-race`？
4. 是否部署到 dev 集群、对 live endpoint 验证过（而非只跑 `go test`）？
5. 对 etcd 语义有疑问时，是否查阅了 `/root/kubernetes`、`/root/etcd` 官方源码而非猜测？
