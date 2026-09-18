# 原 30 秒故障门限的接管观测

旧现场 owner `local-term-framed-3e6f3b15.wxeup3vz` 的原 KeepAlive 请求
在门限内未返回；旧脚本只保留最后一次 new-status.json，未完整保存
fault_start_ns 和 leader 采样历史。该 owner 已结束，不修改或重跑。

`hack/production/observe-successor.sh` 为下一轮实验提供独立观测组件。
接口为七个显式参数：新输出目录、故障开始 UTC 纳秒时间、cluster ID、
观测成员 ID、旧 leader ID、旧 term、绝对路径的请求回调。
回调接收 `BUDGET_SECONDS OUTPUT_JSON`，必须以已有 mTLS 身份对固定成员
执行一次 Status 请求，不重试、不改路由；输出原响应到指定文件。

组件不会注入/解除故障、创建租约或启动 KeepAlive 流。输出目录必须新建，
禁止覆盖。每个 sample-N 保存原 JSON、stdout/stderr、请求退出码、起止
UTC 纳秒和请求预算；输入、截止时间、回调文件摘要另存。纳秒和 uint64
身份作为十进制字符串保存，term 比较使用长度与字典序，不经浮点转换。

固定截止时间为传入 fault_start_ns 加 30 秒；每次回调最多五秒且不超过
剩余时间，采样间隔最多 200ms。回调超时可有最多一秒终止宽限，但任何
超出原截止时间的响应或解析决定都不接受。过期门限不发请求，未来开始
时间或观测到时间倒退则退出。它沿用原 UTC 墙钟门限，不声称能检测所有
时钟跳变；现场仍须记录时钟来源/同步状态。

接管判据要求：唯一 JSON 对象、响应文件不超过 64KiB、无 RPC error、
精确匹配 cluster 和观测成员、新 leader 非零且不同于旧 leader，term
严格增加。拒绝数值型/超出 uint64 的身份，禁止接受最后一个 JSON 覆盖
前面的异常数据。成功时仅记录 successor-sample，不覆盖任何采样。

测试使用可控回调，覆盖失败部分响应→旧 leader→新 leader 的完整历史、
大于 2^53 的 term、错误身份/term、多 JSON、迟到响应、已过期/未来时钟及
输出目录重用。它们不证明 Kubernetes、TLS、Cilium 或真实 TiKV 接管。

**尚未接入真实故障驱动**。下一轮必须在安装策略前持久化原开始时间，
将同一时间传给观测器，并在策略恢复和原始 KeepAlive 响应校验中继续
使用同一截止时间；不得把“观测到新 leader”当作整个实验成功。仍须
保持原单条流、确认服务端等待、绑定 Pod/容器与实际网络丢包、验证响应
term/租约结果，并有独立恢复和清理路径。新镜像需要新的源码栈绑定。

本轮定向 race 三轮通过（4.532s），相关 Go 包 vet、Bash 语法及 diff
检查通过。没有修改已结束的旧实验，也没有执行新的现场故障。
