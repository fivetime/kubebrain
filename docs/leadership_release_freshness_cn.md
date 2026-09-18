# 释放领导权前关闭本机请求准入

2026-09-18，基于 `e3623644961b3f35aecc9f9f4a026c25f93c2258`。
这与初始化错误延长有效期是两个独立边界。

client-go v0.36.2 的 `release` 在 `OnStoppedLeading` 回调之前，以空
HolderIdentity 更新共享选主锁。原 `renewStampingLock` 将任何成功更新都
计作续约；释放成功后可能重新延长旧 leader 的本机有效期。在释放操作阻塞
期间，仅依靠尚未执行的生命周期回调也不能保证请求准入已经关闭。

修复在向非自身或空 holder 发起锁写入前清除 `lastRenew`，无论后续存储写入
成功还是失败都先停止基于新鲜度的准入。只有非空且属于本机的成功更新才
刷新续约时间。保留记录观察、锁变更通知及 Campaign 的生命周期清理、join
顺序；不伪造存储释放成功，不提前抢占其他 leader，不改变选主时限。

## 验证范围

- 包装层负例：已过期本机状态执行空 holder 更新后错误变为有效，原代码
  在预期断言失败；真正续约到自身的记录仍为有效正例。
- 真实 client-go Campaign 加存储替身：成功获取领导权后发起取消，将释放
  调用阻塞在底层 Update 内。此时生命周期仍报告 leader，但有效期必须失效。
  原代码在释放最终成功、失败两个子例均失败。
- 释放解除阻塞并完成后，检查时间戳保持无效；失败时共享记录仍属于原节点，
  成功时为空，不能把本机隔离等同于持久释放成功。失败路径也取消并 join。

隔离补丁中，上述用例及存储故障后重新竞选用例 race 重复十次通过
（13.996 秒）。合入工作区后完整 leader 包 race 重复五次通过（8.577 秒），
vet 与 diff 检查通过。完整 etcd 普通回归通过（151.578 秒），
Lease/Revoke/Expiry/Checkpoint/Attachment 扩展 race 回归通过（81.563 秒）；
连续执行任务终态为 0。

这些是内存存储替身测试，不是实际 TiKV 分区或分布式双主证据。旧版
`e3623644` 的 CI 不包含本修复。本修复的发布核验见下文，尚未部署。原真实集群
30 秒故障切换门限及总体生产就绪仍未通过。

## 同源码发布核验（2026-09-18）

源码 `4479078a93188b097af98e897b9b57094a2550e1` 的
[回归 CI](https://github.com/fivetime/kubebrain/actions/runs/35304294701) 和
[镜像 CI](https://github.com/fivetime/kubebrain/actions/runs/35304294727)
均在 attempt 1 成功。回归日志明确包含两个释放用例、两个初始化新鲜度用例、
两个延迟退位回调用例及过期续租运行时栈用例的 PASS；未触发该源码的后端集成 CI，
不以其他提交的后端结果替代。

独立发布核验退出 0，确认索引及两个平台摘要：

- 索引：`sha256:d3b992e92148979cae703804193ac2be7dcd0cd16b098876b7aebb71a948a729`
- amd64：`sha256:6c0edc53b0ae9c7ffb1c3a16ff7092fa694dc58e8c17c17e24ec1c2e5d31bb4b`
- arm64：`sha256:178c95c9367f88ca320550f32e0d399ba5fdf0a2cb58c0caa8f4f6ba502c7f16`

实际执行的是 amd64 镜像中的二进制，确认版本 `0.0.0-dbaas-4479078a9318`、
源码 SHA、Go 1.26.8、TiKV 存储类型、非 root 配置和 OCI 标签，以及独立 fork
`github.com/fivetime/tikv-client-go/v2 v2.0.8-0.20260909023231-832b70fd622f`
与 gRPC v1.83.2。arm64 仅核验清单身份，不宣称实际运行。核验时发布标签与索引一致。
临时容器和提取二进制已清理并确认不存在。

证据目录：
`/root/.local/state/kubebrain/release-4479078a.j3RC1sRq/audit.QJqo7f2V`。
这仅证明发布镜像身份，不是部署验收。只读复查时 `kubebrain-local` 仍为
generation 38、3 个副本 Ready，使用原固定镜像
`sha256:50b9938fe3e5ad379136957438abe3c2d4bf250dc6c5973c869e0ef1341d1537`；
本轮未切换镜像、修改选主参数或注入故障。
