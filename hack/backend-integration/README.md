# 后端 TiKV 协议集成回归（mock-only）

此独立 Go 模块将真实 KubeBrain TiKV 存储适配器和后端连接到进程内 unistore，
不接入 Kubernetes、PD 或真实 TiKV，不需要 kubeconfig。

```sh
bash hack/backend-integration/run-onepc.sh --count 10
bash hack/backend-integration/run-onepc.sh --race --count 3
```

入口固定 Go 1.26.8、只读模块解析、测试范围和 180 秒测试超时，只接受上面的选项
及 `--help`，次数范围为 1–100。环境需要 Bash、Go、jq、patch 和 GNU coreutils/find。
不传递任意测试参数，不读取真实集群端点。测试中的 1PC 开关仅作用于测试进程，
清理时恢复；产品配置没有改变。每种故障都分别测试禁用 RPC 重试和默认重试，
仅前者启用 `noRetryOnRpcError` failpoint，并在清理时撤销；后者验证它没有遗留。
客户端全局 failpoint 总开关在 `TestMain` 中、任何客户端创建前只初始化一次，
避免逐用例重复写入该非原子开关与上一用例的后台锁解析发生数据竞争。

覆盖提交前未送达、实际服务端 1PC 提交后响应丢失两种普通传输错误，检查：

- 客户端及适配器返回不确定结果，只有带用户上下文标记的提交消耗故障。
- 后端按持久见证解析为未提交或已提交，两键不会部分可见。
- 未提交不推进公共修订号，后续写入复用候选修订号；已提交后只递增一次。
- 已提交两键以同一修订号、同一 watch 批次发布；下一次确认写入之前没有额外事件。
  内部 API 区分 CREATE/PUT，对外 etcd 适配层将两者映射为 PUT。

默认重试用例在 loopback 随机端口启动真实 gRPC Health 服务，并给相同 mock StoreID
注册该地址，让客户端默认的健康检查和重试实际发生；数据 RPC 仍使用进程内 unistore。
用例验证健康检查被调用、恰好发送两次、事务起始时间戳不变、不会出现两个提交时间戳。
发送前故障在重试后返回成功；本版本 mock 中提交后丢响应在重试后仍返回不确定结果，
由后端见证解析为已提交。禁用重试场景只能发送一次。健康服务在后端关闭后停止并回收。

Region 分裂用例在带用户标记的第一次 1PC prewrite 发送前分裂 mock Region，
使已完成分组的请求携带旧 epoch。要求客户端保留事务起始时间戳，实际向至少两个
Region 成功 prewrite 并执行两阶段 commit，不能出现 1PC 提交结果。后端两键读取和
同一 watch 批次使用同一修订号，下一次写入只递增一次。这是受控客户端回退测试，
没有操作真实 TiKV Region，也不是真实 Raft 持久性或故障恢复证明。

`go.mod` 的客户端远程替换必须与根模块完全一致，入口会拒绝漂移；根模块只用
相对路径 `../..` 引用，不依赖 `/root/tikv-client-go` 或任何私有诊断目录。
旧 TiDB mock 依赖在 Go 1.26 下引用私有 runtime 符号，因此入口校验模块与源码摘要，
只在自己的临时副本中应用 `compat/tidb-printer-go126.patch`，改用公开 runtime API。
不修改模块缓存，不关闭链接器检查，不把 TiDB 源码引入产品。
维护客户端版本时同步两个模块并重新验证，不通过替换成本机副本绕过检查。

非 root 清理契约可独立运行：

```sh
bash hack/backend-integration/run-onepc-cleanup-test.sh
bash hack/backend-integration/run-onepc-entry-test.sh
```

清理契约检查嵌套只读目录清理、退出码保留和外部符号链接目标不被修改，禁止以 root 运行。
入口契约检查无效次数、真实端点参数、任意测试参数、客户端版本漂移及双方同时误用
本机替换路径时提前拒绝；它不运行 Go 集成测试，也不准备 mock 依赖。
临时目录前缀与客户端 fork 的 mock 入口相同，但每次创建独立随机目录，清理只针对
该次调用的确切路径。兼容补丁和清理策略来自客户端 fork 提交 `2155365`。

边界：这不是真实 TiKV Raft 持久性、完整 TiKV 网络协议、外部 etcd gRPC Watch、真实跨 Region
事务或 30 秒恢复／900 秒升级验收的证明，不能据此启用生产 1PC。
