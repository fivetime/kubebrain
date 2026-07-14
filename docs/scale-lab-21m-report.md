# KubeBrain 千万级单集群压测报告

> 日期:2026-07-14 &nbsp;|&nbsp; 被测对象:KubeBrain(main,含 etcd 3.7 升级 / BatchGet 回放 / compact 批量 GC / hint-cache 字节上界等本轮全部修复) &nbsp;|&nbsp; 环境:3 台独立机混部

---

## 一、结论先行

**KubeBrain 存储层轻松承载 2107 万 Kubernetes 对象:leader 进程内存仅 7.4 GB、TiKV 数据量仅 35 GB/副本、写入全程零失败、p99 < 70 ms。**

规模墙不在存储层,而在**单集群 Kubernetes 控制面**——kube-apiserver + kube-controller-manager + kube-scheduler 的 informer/watch-cache 内存(211 GB),以及 kcm 在巨型 watch-cache 全关下的 reconcile 吞吐。这正是 KubeBrain 存在的意义:**让存储不再是超大规模单集群的短板,把瓶颈显式地留给需要横向分片(kubegateway / cluster-mesh)去解的控制面。**

---

## 二、环境拓扑(3 机混部)

| 机器 | 配置 | 承载角色 |
|---|---|---|
| `10.224.0.13` | 80c / 300G / 2T | PD + TiKV + **KubeBrain leader** |
| `10.224.0.14` | 72c / 269G / 2T | PD + TiKV + KubeBrain follower |
| `10.224.0.15` | 72c / 362G / 2T | PD + TiKV + KubeBrain follower + **k3s(apiserver+kcm+scheduler)+ KWOK** |

- **磁盘**:均为 2 TB 单盘(vda),TiKV + KubeBrain + k3s 共盘。`dd` 4k dsync 实测 **630–745 MB/s**(本地 NVMe 级 fsync,非慢云盘)。
- **存储层**:tiup 部署 PD×3 + TiKV×3(v8.5.7,每节点 1 个,`storage.block-cache.capacity=50GB` 限混部内存,`replication.max-replicas=3`);podman 部署 KubeBrain×3(明文连 PD,经存储层 election 选主,非 raft peer)。
- **控制面**:k3s 走 external etcd 模式(非 kine),datastore 指向 3 台 KubeBrain,对象存标准 `/registry/…`;**巨型资源 watch-cache 全关**(`--watch-cache-sizes=secrets#0,configmaps#0,pods#0,deployments.apps#0,replicasets.apps#0,…`)。
- **KWOK**:v0.8 独立 binary,只在对象状态层面模拟 node/pod(不涉及真实 kubelet / CNI / 容器)。

---

## 三、存储压测数据(核心)

### 3.1 分资源写入表现

| 资源 | 数量 | 写入速率 | p99 | 失败 |
|---|---|---|---|---|
| nodes | 10 万 | 2888/s | 88 ms | 0 |
| namespaces | 100 万 | 3452/s | 68 ms | 0 |
| serviceaccounts | 100 万 | 3413/s | 59 ms | 0 |
| roles | 100 万 | 3980/s | 39 ms | 0 |
| services | 100 万 | 3781/s | 52 ms | 0 |
| endpointslices | 100 万 | 3624/s | 43 ms | 0 |
| secrets | 500 万 | 3973/s | 40 ms | 0 |
| configmaps | 500 万 | 3464/s | 49 ms | 0 |
| deployments | ~490 万 | 3070/s | 61 ms | 0 |
| **合计**(含 kcm 衍生 RS/pod) | **2107 万** | | | **0** |

### 3.2 资源占用(铁证)

| 组件 | 内存 / 数据量 |
|---|---|
| **KubeBrain leader 进程** | **7.4 GB** |
| KubeBrain follower 进程 | 0.5 GB |
| **TiKV 数据量** | **35 GB / 副本** |
| 纯存储层机器(.13 / .14)整机 | 40–51 GB |
| **k3s 控制面(.15)** | **211 GB** |

> KubeBrain leader 用 **7.4 GB** 进程内存服务 2107 万对象;控制面(apiserver+kcm+scheduler)用了 **211 GB**。同一份数据,存储侧与控制面侧的内存差约 **28 倍**。

---

## 四、三大发现

### 发现 1:存储层从不是瓶颈

KubeBrain leader 用 **7.4 GB** 进程内存承载 2107 万对象,TiKV 数据量仅 **35 GB/副本**,写入全程 0 失败、p99 < 70 ms、3000–4000/s。纯存储层机器(.13/.14)整机才 40–51 GB。这用具体数字坐实了 KubeBrain 一贯的命题:**大规模单集群的存储层可以做到既轻又快,不成为规模墙。**

### 发现 2:墙在单集群控制面的 informer 内存

k3s(apiserver + kcm + scheduler)缓存约 410 万个"可管理对象"(node/ns/SA/svc/endpointslice/deploy;不含 secret/configmap——这些资源没有 controller watch)就吃到 **123 → 216 GB**,约 **30 KB/对象**(informer store + apiserver watch-cache 副本叠加)。

这是**单集群 Kubernetes 控制面的固有规模墙**,与存储后端无关。解法是横向分片 apiserver(kubegateway 按资源类型路由到不同 apiserver 子集,每个只 cache 一部分资源),而不是靠单个控制面硬扛。

### 发现 3:kcm reconcile 是比 OOM 更深刻的真瓶颈

为省内存,apiserver 的巨型 watch-cache 全部关闭。这带来一个反常识的连锁:

1. kube-controller-manager 的 informer 要 LIST deployment 时,watch-cache 关了 → 直接走 KubeBrain 的 range scan(相对慢);
2. kcm 的 reconcile 吞吐**跟不上** loadgen 3200/s 的 deployment 写入速度;
3. `deployment → ReplicaSet → Pod` 这条链**断裂**:490 万 deployment 只 reconcile 出约 50 万 RS/pod;
4. **反常识的结果**:正因为 kcm 处理不过来,k3s 的 informer 没有大量缓存新 RS/pod,内存反而被**挡在 216 GB**,撞不到 362 GB 的物理墙。

**结论:单集群控制面的真正瓶颈不止是 informer 内存,还有 kcm 在 watch-cache 全关下的 reconcile 吞吐——后者会先饱和,把内存墙"推后"。** 但代价是集群不健康(pod 生成不出来)。

---

## 五、运维教训

- **⚠️ 单盘 IO 是唯一约束**:测试中途手动执行 `dd if=/dev/zero of=/swapfile bs=1G count=300`(建 300 GB swapfile)时,那 8 分半钟把 vda 的 IO 带宽占满 → PD 的 raft `apply request` 飙到 5–11 秒 → raft 选举超时反复丢 leader → KubeBrain 拿不到 TSO(PD 无 leader 时 `generate timestamp failed`)→ apiserver bootstrap 失败陷入 restart loop。**`dd` 结束、IO 释放后,`tiup cluster restart -R pd` 让 PD 干净重新选主即完全恢复。** 教训:混部单盘下,任何满速独占 IO(dd / 冲量高峰)都会拖垮 PD 的 raft 写盘,进而拖垮整个存储栈。

- **swap 无害(在 swappiness=1 下)**:上述 300 GB swapfile 配 `vm.swappiness=1`,意味着 swap 只在濒临 OOM 时才启用(全程 `used 0`),成为纯 OOM 安全网,不影响性能。数据库集群"禁 swap"的铁律,在 swappiness=1 下被有效缓解。

- **CNI 不参与**:KWOK 的 node/pod 是纯 API 对象,fake pod 的 `podIP` 由 loadgen 直接 PATCH 到 status,没有走 CNI(测试禁用了 flannel)。这次压的是控制面 + 存储层,与 CNI 无关。真实生产的 500 万 pod 是另一堵墙(每节点 pod 数 + 单集群 ~5000 节点上限),需要多集群 Cluster Mesh 分片——而那时每个分片的存储后端正可以是 KubeBrain。

- **KWOK v0.8 卡点**:独立 binary 需要显式 `--enable-crds=Stage` 才读 CRD stages;即便如此,其 node-initialize stage 引擎在本次未触发(node 未变 Ready),已搁置。所幸 loadgen 的 `runpods` 模式会自己 PATCH pod status 成 Running(IP / conditions / containerStatuses 全套),替代了 KWOK 的 pod-running 功能。

---

## 六、附录:可复用部署配方

### PD + TiKV(tiup)

```yaml
# topology.yaml
global: { user: root, ssh_port: 22, deploy_dir: /data/tidb-deploy, data_dir: /data/tidb-data, arch: amd64 }
server_configs:
  tikv: { "storage.block-cache.capacity": "50GB", "server.grpc-concurrency": 8 }
  pd:   { "replication.max-replicas": 3 }
pd_servers:   [ {host: 10.224.0.13}, {host: 10.224.0.14}, {host: 10.224.0.15} ]
tikv_servers: [ {host: 10.224.0.13}, {host: 10.224.0.14}, {host: 10.224.0.15} ]
```
```
tiup cluster deploy kbtest v8.5.7 topology.yaml --user root -i ~/.ssh/id_rsa --yes
tiup cluster start kbtest --init
```
> ⚠️ `start` 之后 `display` 那一刻 TiKV 常显 Down(还没注册到 PD),稍等即 Up。

### KubeBrain(podman,每台 advertise-host 钉本机 IP)

```
podman run -d --name kubebrain --network host --restart=always \
  ghcr.io/fivetime/kubebrain:latest \
  --pd-addrs=10.224.0.13:2379,10.224.0.14:2379,10.224.0.15:2379 \
  --advertise-host=<本机IP> --port=3379 --peer-port=3380 --info-port=8080 \
  --compatible-with-etcd=true --key-prefix=/registry --cluster-name=prod \
  --enable-count-index=true --count-index-max-keys=30000000 \
  --enable-storage-metrics=true
```

### k3s apiserver(external datastore = KubeBrain,巨型 cache 全关)

```
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server \
  --datastore-endpoint=http://10.224.0.13:3379,http://10.224.0.14:3379,http://10.224.0.15:3379 \
  --disable=traefik,servicelb,local-storage,metrics-server \
  --flannel-backend=none --disable-network-policy --disable-cloud-controller \
  --kube-apiserver-arg=watch-cache-sizes=secrets#0,configmaps#0,pods#0,deployments.apps#0,replicasets.apps#0,services#0,endpointslices.discovery.k8s.io#0,serviceaccounts#0,roles.rbac.authorization.k8s.io#0 \
  --kube-apiserver-arg=default-watch-cache-size=100" sh -
```

### KWOK(独立 binary,须 --enable-crds=Stage)

```
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/download/v0.8.0/kwok.yaml
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/download/v0.8.0/stage-fast.yaml
kwok --kubeconfig=/etc/rancher/k3s/k3s.yaml \
  --manage-nodes-with-annotation-selector=kwok.x-k8s.io/node=fake \
  --enable-crds=Stage
```

### loadgen(client-go 直连 apiserver)

- 所有资源模式**都用 `-count`**(不是 `-ns`);
- per-ns 资源:`-count N -per-ns M`,分布在 N/M 个 namespace(ns 名 `ns-%07d`,与 namespaces 模式对应);
- deployment:`-mode workload -ns N -deploy M -replicas R -skip-ns -ns-start X`(deploy 的 ns 必须在已建 namespace 内);
- `runpods`:把存量 Pending pod 批量 PATCH 成 Running。
