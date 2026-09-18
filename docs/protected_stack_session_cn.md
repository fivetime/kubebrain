# 受保护采栈会话工具

`hack/production/protected-stack-session.sh` 将此前实验目录中的会话逻辑
纳入版本管理，并接入 `same-pod-process.jq`。Ready/启动探针标志不再
用作进程身份；Pod UID、完整 spec、IP、容器 ID/镜像标识、重启次数
和启动时间仍必须相同。这不改变产品代码，也不重写既有失败实验。

## 调用约定

仅在新实验 owner、已完成诊断部署后使用。控制器先启用
`set -euo pipefail`、`umask 077`，在同一拥有子进程的 Bash 中 source
该文件，并设置以下变量，不能在命令替换/子 shell 中调用会话函数：

- `stack_owner`：私有证据目录；要求 `deployment-claimed` 存在、`HOLD`
  和 `final-exit-code` 不存在。调用者仍负责新 owner 的单次消费约束。
- `stack_kubeconfig`、`stack_context`：明确的目标集群，不使用隐式上下文。
- `stack_namespace`、`stack_namespace_uid`、`stack_sts`、`stack_sts_uid`：
  本轮重新核验的资源身份。
- `stack_tls`：包含 `ca.crt`、`probe.crt`、`probe.key` 的私有目录。
- `stack_server_name`：info 服务 TLS 名称。

owner 下准备 `diagnostic-spec.json`（完整 StatefulSet spec）、`info.crt`
（预先核验的服务端证书）及 `bin/info-diagnostic-probe`。固定工具目录
应同时包含库文件和 jq 身份规则。`tools.sha256` 必须覆盖这两个文件
及探针，`diagnostic-inputs.sha256` 必须覆盖 spec 和证书；路径使用
对应实际绝对路径。清单完整覆盖和摘要均在 prepare/capture 时检查。
清单自身的可信来源、源码/镜像绑定由外层实验准入验证，不能靠重新
计算清单将未知修改视为已审核。

外层控制器必须安装 EXIT 清理 `stack_session_close`，TERM/INT 分别
退出 143/130，并负责其余故障资源的恢复。调用顺序：

1. `stack_session_prepare <StatefulSet名称-序号>`，预先建立两个指向
   精确 Pod 的 localhost info 转发，固定端口 18584/18585。
2. `stack_session_capture before-fault`，输出目录由 `stack_capture` 返回。
3. 故障激活前记录原始纳秒起点；所有后续 capture 传同一个起点。
4. 读取采集结果后执行外层原有等待栈/降主栈分类与响应断言，最后恢复。

仅支持单应用容器模板、info 端口 8080。每次采集检查 namespace/STS
身份、诊断配置、采集前后进程身份以及与预热时进程的一致性。TLS
正向鉴权、匿名拒绝、服务端公钥 pin 验证由实际 protected-stack 探针
执行。此库不以 Ready 作为采栈条件，也不替代服务可用性门限。

## 原时间预算与失败处理

故障后总预算始终为原起点加 30 秒；单个外部操作上限 25 秒且受剩余
预算限制。禁止重新传入 before-fault 或更换起点。控制器必须继续用
原截止时间限制整个故障流程，不把探针完成标记当作全流程验收通过。

同步命令使用 foreground timeout，外层控制器应由可取消整个进程组的
监护进程运行。关闭只处理当前 shell 拥有的后台任务，避免对陈旧 PID
误发信号。任何失败都不得继续执行成功断言。完整采集及最后期限检查
通过后才写 `COMPLETE`；部分目录保留作为失败证据。

## 验证范围与剩余工作

仓库测试使用合成 Kubernetes 响应和假探针，覆盖 Ready 前后变化、
重启、资源身份/spec 不符、端口占用、断开的通道、探针失败/错误模式、
原预算过期/未来时钟/重置、已消费 owner、清单漏项和探针篡改。外层
取消测试确认两个转发及阻塞探针退出且不生成完成标记。

`go test -race -count=3 ./hack/production -run '^Test(ProtectedStackSession|SamePodProcess)'`
通过（41.935 秒），`go vet ./hack/production` 和 Bash 语法检查通过。
会话链路包含 20 个场景，另有多文档输入拒绝及外层取消测试。保存的
真实实验 Pod 与诊断模板容器定义也经只读核对一致，不据此推断新实验
的未来运行身份或 Ready 状态。

这些测试不证明真实 mTLS、数据面隔离或 30 秒故障验收。新实验驱动
尚须使用新的冻结工具目录接入本库，完成准入、真实实验及恢复；不能
在已消费的 `fault-2ad79751.N8nMZWbk` 中替换脚本后重跑。
