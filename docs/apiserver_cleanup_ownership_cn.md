# Kubernetes 接入测试：lease 清理所有权

## 已确认的问题

真实后端接入前审核发现，以下三个 runner 会计算测试前后全局 lease ID 集合的差值，
并撤销所有新增 lease：

- `hack/dev/apiserver-smoke.sh`
- `hack/dev/apiserver-watch-soak.sh`
- `hack/dev/incluster-apiserver-smoke.sh`（也被 in-cluster watch/rollout runner 调用）

全局集合差值不证明资源属于测试。即使测试使用独立的 etcd key prefix，其他客户端
仍可能在同一后端创建 lease。根据当前 attached keys 再判断也不足以安全撤销：
检查后其他客户端仍能给该 lease 绑定新 key。

新增回归直接执行这三个脚本的实际 cleanup 函数，模拟测试期间另一个客户端创建
`bb` lease。旧实现三次均发出了 `lease revoke bb`，测试失败；没有在真实集群执行
这个破坏性复现。

## 修复及限制

三个脚本共用 `hack/dev/apiserver-lease-cleanup.sh`：停止自身 apiserver、清除原有
测试 prefix 后，仅只读观察 lease 集合，给自然到期提供 60 秒重试窗口。基线恢复后
通过；列表读取失败或到期仍不匹配，保留清理失败退出码 70，不主动撤销任何
所有权未经证明的 lease。窗口不包含在途 etcdctl 命令的执行时间。

这也可能因无关客户端新增 lease、基线 lease 正常到期或测试 lease TTL 超过窗口
而失败。失败是“无法证明清理已收敛”，不等于产品泄漏，更不允许为取得绿色结果
删除无关数据。不要将本修复描述为能够识别和回收所有测试 lease。

测试覆盖三种 runner × 五种状态：基线不变、新增无关 lease、基线 lease 消失、
列表读取失败、自然到期恢复基线。检查仍删除原测试 prefix、绝不执行 lease revoke、
不确定时返回 70，并更新原来错误地要求全局差集撤销的静态安全契约。

本地验证通过：相关 apiserver/安全契约测试 0.775 秒；扩大至
`go test -race . -run 'Test.*(Runner|APIServer|KubernetesVersionMatrix)' -count=1`
通过（8.270 秒，在 `hack/etcd-client-compat` 模块运行）；四个变更 shell 文件
均通过 `bash -n`，补丁通过 `git diff --check`。

## 真实 Kubernetes 接入的剩余准备

本轮只读查询专用集群：`kubebrain-local`、`kb-local-pd`、`kb-local-tikv` 均为
3/3 Ready。没有部署临时 apiserver，也没有修改该集群。

独立进程 smoke/watch runner 已新增可选的[独立临时 PKI 模式](apiserver_ephemeral_pki_cn.md)，
默认 kind 模式仍复制 kind 控制面 PKI；in-cluster runner 仍约束 HTTP NodePort。
后续需核验目标 apiserver 二进制，并准备服务端身份、客户端证书、管理连接和精确资源所有权，
再执行对象生命周期、分页 list/watch、lease、apiserver 重启及 KubeBrain 切换测试。
不得用 `hack/scale-lab/setup.sh all` 覆盖现有专用测试环境，也不得将本轮脚本单测
作为真实 Kubernetes 接入成功证据。
