// kwok-scale-lab loadgen: 批量创建 fake Node / Namespace / Deployment,
// client-go 直连 apiserver,可控并发与规模,记录延迟分布与错误。
//
// 模式:
//
//	nodes    --count N [--start i]                   创建 fake nodes
//	workload --ns N --deploy M --replicas R [--ns-start i]  创建 ns + deployments
//	cleanup  --ns N                                  删除 workload namespaces
//	services --count N --per-ns M [--ns-start i]     创建无 selector 的极小 Service
//	secrets  --count N --per-ns M [--ns-start i]     创建极小 Secret
//	createpods --count N --per-ns M [--ns-start i]   直接建独立 pod 对象并标 Running(绑 fake node,绕开 kcm)
//	status                                           对象计数
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/api/resource"
	metav1m "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type lat struct {
	mu sync.Mutex
	us []int64
}

func (l *lat) add(d time.Duration) { l.mu.Lock(); l.us = append(l.us, d.Microseconds()); l.mu.Unlock() }
func (l *lat) pct() (p50, p95, p99 int64, n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.us) == 0 {
		return 0, 0, 0, 0
	}
	v := append([]int64(nil), l.us...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	at := func(p float64) int64 { return v[int(float64(len(v)-1)*p)] }
	return at(.5), at(.95), at(.99), len(v)
}

var zones = []string{"zone-a", "zone-b", "zone-c"}

func main() {
	mode := flag.String("mode", "status", "nodes|workload|cleanup|status")
	kubeconfig := flag.String("kubeconfig", os.Getenv("HOME")+"/.kube-lab/config", "")
	count := flag.Int("count", 1000, "node count (mode=nodes)")
	start := flag.Int("start", 0, "node start index")
	nsN := flag.Int("ns", 100, "namespace count")
	nsStart := flag.Int("ns-start", 0, "namespace start index")
	deployN := flag.Int("deploy", 10, "deployments per namespace")
	replicas := flag.Int("replicas", 10, "replicas per deployment")
	workers := flag.Int("workers", 64, "concurrent workers")
	qps := flag.Float64("qps", 0, "client-side QPS cap (0 = unlimited)")
	perNs := flag.Int("per-ns", 4, "objects per namespace (services/secrets)")
	shards := flag.Int("shards", 6, "KWOK shard count (nodes get lab/shard=idx%%shards)")
	shuffle := flag.Bool("shuffle", true, "shuffle item order to spread sequential-key region hotspots")
	skipNs := flag.Bool("skip-ns", false, "workload: skip namespace creation (already exists)")
	flag.Parse()

	cfg, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}
	cfg.QPS = 5000
	cfg.Burst = 10000
	cfg.ContentType = "application/vnd.kubernetes.protobuf"
	cli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	var gap time.Duration
	if *qps > 0 {
		gap = time.Duration(float64(time.Second) / *qps)
	}

	run := func(total int, fn func(i int) error) {
		var done, failed int64
		l := &lat{}
		t0 := time.Now()
		ch := make(chan int, *workers)
		var wg sync.WaitGroup
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range ch {
					t := time.Now()
					err := fn(i)
					if err != nil && !errors.IsAlreadyExists(err) {
						atomic.AddInt64(&failed, 1)
						if atomic.LoadInt64(&failed) < 6 {
							fmt.Fprintf(os.Stderr, "ERR item %d: %v\n", i, err)
						}
						continue
					}
					l.add(time.Since(t))
					atomic.AddInt64(&done, 1)
				}
			}()
		}
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		go func() {
			for range tick.C {
				fmt.Printf("[%4ds] done=%d failed=%d\n", int(time.Since(t0).Seconds()),
					atomic.LoadInt64(&done), atomic.LoadInt64(&failed))
			}
		}()
		order := make([]int, total)
		for i := range order {
			order[i] = i
		}
		if *shuffle {
			r := rand.New(rand.NewSource(42)) // 固定种子:重跑幂等同序
			r.Shuffle(total, func(a, b int) { order[a], order[b] = order[b], order[a] })
		}
		for _, i := range order {
			ch <- i
			if gap > 0 {
				time.Sleep(gap)
			}
		}
		close(ch)
		wg.Wait()
		p50, p95, p99, n := l.pct()
		fmt.Printf("DONE mode=%s total=%d ok=%d failed=%d elapsed=%s rate=%.0f/s p50=%.1fms p95=%.1fms p99=%.1fms\n",
			*mode, total, atomic.LoadInt64(&done), atomic.LoadInt64(&failed),
			time.Since(t0).Round(time.Second), float64(n)/time.Since(t0).Seconds(),
			float64(p50)/1000, float64(p95)/1000, float64(p99)/1000)
	}

	switch *mode {
	case "nodes":
		run(*count, func(i int) error {
			idx := *start + i
			name := fmt.Sprintf("kwok-node-%06d", idx)
			node := &corev1.Node{
				ObjectMeta: metav1m.ObjectMeta{
					Name: name,
					Labels: map[string]string{
						"type":                         "kwok",
						"kubernetes.io/hostname":       name,
						"node-role.kubernetes.io/kwok": "true",
						"topology.kubernetes.io/zone":  zones[idx%len(zones)],
						"lab/rack":                     fmt.Sprintf("rack-%03d", idx%60),
						"lab/shard":                    fmt.Sprintf("%d", idx%*shards),
					},
					Annotations: map[string]string{"kwok.x-k8s.io/node": "fake"},
				},
				Spec: corev1.NodeSpec{
					Taints: []corev1.Taint{{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule}},
				},
				Status: corev1.NodeStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceCPU: metav1.MustParse("8"), corev1.ResourceMemory: metav1.MustParse("32Gi"),
						corev1.ResourcePods: metav1.MustParse("110"),
					},
					Allocatable: corev1.ResourceList{
						corev1.ResourceCPU: metav1.MustParse("8"), corev1.ResourceMemory: metav1.MustParse("32Gi"),
						corev1.ResourcePods: metav1.MustParse("110"),
					},
				},
			}
			_, err := cli.CoreV1().Nodes().Create(ctx, node, metav1m.CreateOptions{})
			return err
		})

	case "workload":
		// namespaces 先建(幂等)
		if !*skipNs {
			run(*nsN, func(i int) error {
				ns := fmt.Sprintf("ns-%07d", *nsStart+i)
				_, err := cli.CoreV1().Namespaces().Create(ctx,
					&corev1.Namespace{ObjectMeta: metav1m.ObjectMeta{
						Name: ns, Labels: map[string]string{"workload-type": "fake"}}},
					metav1m.CreateOptions{})
				return err
			})
		}
		if false {
			run(*nsN, func(i int) error {
				ns := fmt.Sprintf("ns-%07d", *nsStart+i)
				_, err := cli.CoreV1().Namespaces().Create(ctx,
					&corev1.Namespace{ObjectMeta: metav1m.ObjectMeta{
						Name: ns, Labels: map[string]string{"workload-type": "fake"}}},
					metav1m.CreateOptions{})
				return err
			})
		}
		total := *nsN * *deployN
		rep := int32(*replicas)
		run(total, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *deployN)
			name := fmt.Sprintf("app-%03d", i%*deployN)
			d := &appsv1.Deployment{
				ObjectMeta: metav1m.ObjectMeta{
					Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"},
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: &rep,
					Selector: &metav1m.LabelSelector{MatchLabels: map[string]string{"app": name}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1m.ObjectMeta{Labels: map[string]string{"app": name, "workload-type": "fake"}},
						Spec: corev1.PodSpec{
							Tolerations: []corev1.Toleration{{
								Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpEqual,
								Value: "fake", Effect: corev1.TaintEffectNoSchedule}},
							NodeSelector: map[string]string{"type": "kwok"},
							Containers: []corev1.Container{{
								Name: "fake", Image: "nginx:latest",
								Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
									corev1.ResourceCPU:    metav1.MustParse("10m"),
									corev1.ResourceMemory: metav1.MustParse("32Mi")}},
							}},
						},
					},
				},
			}
			_, err := cli.AppsV1().Deployments(ns).Create(ctx, d, metav1m.CreateOptions{})
			return err
		})

	case "derive":
		// 为每个尚无 RS 的 deployment 直灌 1 RS + 1 pod(生产中"千万对象"是累积
		// 稳态,不是 controller 消化百万积压;真实派生链路已由 kcm 的 138 万样本
		// 验证)。ownerRef/labels/template 与 kcm 产物语义一致,controller 收编
		// 后逐个 no-op;pod 预绑 nodeName 绕过调度,KWOK 接管 Running 状态机。
		covered := map[string]struct{}{}
		{
			opts := metav1m.ListOptions{Limit: 10000}
			for {
				l, err := cli.AppsV1().ReplicaSets("").List(ctx, opts)
				if err != nil {
					panic(err)
				}
				for i := range l.Items {
					rs := &l.Items[i]
					for _, o := range rs.OwnerReferences {
						if o.Kind == "Deployment" {
							covered[rs.Namespace+"/"+o.Name] = struct{}{}
						}
					}
				}
				if l.Continue == "" {
					break
				}
				opts.Continue = l.Continue
			}
			fmt.Printf("deployments already covered by an RS: %d\n", len(covered))
		}
		type dep struct {
			ns, name string
			uid      k8stypes.UID
		}
		var pendingD []dep
		{
			opts := metav1m.ListOptions{Limit: 10000}
			pages := 0
			for {
				l, err := cli.AppsV1().Deployments("").List(ctx, opts)
				if err != nil {
					panic(err)
				}
				for i := range l.Items {
					d := &l.Items[i]
					if _, ok := covered[d.Namespace+"/"+d.Name]; ok {
						continue
					}
					pendingD = append(pendingD, dep{ns: d.Namespace, name: d.Name, uid: d.UID})
				}
				pages++
				if pages%100 == 0 {
					fmt.Printf("scanned %d deployment pages, pending=%d\n", pages, len(pendingD))
				}
				if l.Continue == "" {
					break
				}
				opts.Continue = l.Continue
			}
			fmt.Printf("deployments to derive: %d\n", len(pendingD))
		}
		if *count > 0 && len(pendingD) > *count {
			pendingD = pendingD[:*count] // 小批验证用;全量传 -count 0
		}
		rep := int32(1)
		ctrl := true
		podTemplate := func(app string) corev1.PodTemplateSpec {
			return corev1.PodTemplateSpec{
				ObjectMeta: metav1m.ObjectMeta{Labels: map[string]string{"app": app, "workload-type": "fake"}},
				Spec: corev1.PodSpec{
					Tolerations: []corev1.Toleration{{
						Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpEqual,
						Value: "fake", Effect: corev1.TaintEffectNoSchedule}},
					NodeSelector: map[string]string{"type": "kwok"},
					Containers: []corev1.Container{{
						Name: "fake", Image: "nginx:latest",
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
							corev1.ResourceCPU:    metav1.MustParse("10m"),
							corev1.ResourceMemory: metav1.MustParse("32Mi")}},
					}},
				},
			}
		}
		run(len(pendingD), func(i int) error {
			d := pendingD[i]
			rsName := d.name + "-ldg"
			rs := &appsv1.ReplicaSet{
				ObjectMeta: metav1m.ObjectMeta{
					Name: rsName, Namespace: d.ns,
					Labels: map[string]string{"app": d.name, "workload-type": "fake"},
					OwnerReferences: []metav1m.OwnerReference{{
						APIVersion: "apps/v1", Kind: "Deployment", Name: d.name, UID: d.uid,
						Controller: &ctrl, BlockOwnerDeletion: &ctrl}},
				},
				Spec: appsv1.ReplicaSetSpec{
					Replicas: &rep,
					Selector: &metav1m.LabelSelector{MatchLabels: map[string]string{"app": d.name}},
					Template: podTemplate(d.name),
				},
			}
			var rsUID k8stypes.UID
			created, err := cli.AppsV1().ReplicaSets(d.ns).Create(ctx, rs, metav1m.CreateOptions{})
			if err != nil {
				if !errors.IsAlreadyExists(err) {
					return err
				}
				got, gerr := cli.AppsV1().ReplicaSets(d.ns).Get(ctx, rsName, metav1m.GetOptions{})
				if gerr != nil {
					return gerr
				}
				rsUID = got.UID
			} else {
				rsUID = created.UID
			}
			tpl := podTemplate(d.name)
			pod := &corev1.Pod{
				ObjectMeta: metav1m.ObjectMeta{
					Name: rsName + "-0", Namespace: d.ns,
					Labels: tpl.ObjectMeta.Labels,
					OwnerReferences: []metav1m.OwnerReference{{
						APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rsName, UID: rsUID,
						Controller: &ctrl, BlockOwnerDeletion: &ctrl}},
				},
				Spec: tpl.Spec,
			}
			pod.Spec.NodeName = fmt.Sprintf("kwok-node-%06d", i%100000)
			_, perr := cli.CoreV1().Pods(d.ns).Create(ctx, pod, metav1m.CreateOptions{})
			if perr != nil && !errors.IsAlreadyExists(perr) {
				return perr
			}
			return nil
		})

	case "runpods":
		// 存量 Pending pod 直接补 Running status(KWOK 的存量收编太慢,它只留作
		// 增量状态机)。List 走 pods-shard cache;PATCH status 子资源无冲突。
		type pp struct {
			ns, name string
			idx      int
		}
		var pend []pp
		{
			opts := metav1m.ListOptions{Limit: 10000}
			pages := 0
			for {
				l, err := cli.CoreV1().Pods("").List(ctx, opts)
				if err != nil {
					panic(err)
				}
				for i := range l.Items {
					p := &l.Items[i]
					if p.Status.Phase == corev1.PodPending {
						pend = append(pend, pp{ns: p.Namespace, name: p.Name, idx: len(pend)})
					}
				}
				pages++
				if pages%100 == 0 {
					fmt.Printf("scanned %d pod pages, pending=%d\n", pages, len(pend))
				}
				if l.Continue == "" {
					break
				}
				opts.Continue = l.Continue
			}
			fmt.Printf("pending pods to run: %d\n", len(pend))
		}
		if *count > 0 && len(pend) > *count {
			pend = pend[:*count]
		}
		now := metav1m.Now()
		run(len(pend), func(i int) error {
			p := pend[i]
			ip := fmt.Sprintf("10.%d.%d.%d", 66+(p.idx>>16)&0x3F, (p.idx>>8)&0xFF, p.idx&0xFF)
			patch := fmt.Sprintf(`{"status":{"phase":"Running","podIP":"%s","podIPs":[{"ip":"%s"}],"startTime":%q,"conditions":[{"type":"Initialized","status":"True","lastTransitionTime":%q},{"type":"Ready","status":"True","lastTransitionTime":%q},{"type":"ContainersReady","status":"True","lastTransitionTime":%q},{"type":"PodScheduled","status":"True","lastTransitionTime":%q}],"containerStatuses":[{"name":"fake","state":{"running":{"startedAt":%q}},"ready":true,"restartCount":0,"image":"nginx:latest","imageID":"fake"}]}}`,
				ip, ip, now.Format("2006-01-02T15:04:05Z"), now.Format("2006-01-02T15:04:05Z"), now.Format("2006-01-02T15:04:05Z"), now.Format("2006-01-02T15:04:05Z"), now.Format("2006-01-02T15:04:05Z"), now.Format("2006-01-02T15:04:05Z"))
			_, err := cli.CoreV1().Pods(p.ns).Patch(ctx, p.name, k8stypes.StrategicMergePatchType, []byte(patch), metav1m.PatchOptions{}, "status")
			return err
		})

	case "createpods":
		// 直接向存储写独立 pod 对象并标 Running,绕开 deployment→RS→pod 的
		// controller 链(kcm 在 apiserver watch-cache 全关下 reconcile 跟不上,
		// 是控制面吞吐瓶颈,不是存储瓶颈)。每 pod:Create(绑 nodeName、容忍 kwok
		// taint,直接 bound 不经 scheduler)后立即 Patch status Running。pod 轮询
		// 绑 10 万 fake node。用于纯粹压 KubeBrain 承载 pod 对象的能力。
		nowS := metav1m.Now().Format("2006-01-02T15:04:05Z")
		noAutomount := false
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart+i/(*perNs))
			name := fmt.Sprintf("pod-%03d", i%*perNs)
			nodeName := fmt.Sprintf("kwok-node-%06d", i%100000)
			pod := &corev1.Pod{
				ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"}},
				Spec: corev1.PodSpec{
					NodeName: nodeName,
					// 用已建的 sa-000(每 ns 都有)+ 关 token 挂载,避开 default SA
					// 尚未被 SA controller 创建导致的 "serviceaccount not found"。
					ServiceAccountName:           "sa-000",
					AutomountServiceAccountToken: &noAutomount,
					Containers:                   []corev1.Container{{Name: "fake", Image: "nginx:latest"}},
					Tolerations:                  []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
				},
			}
			if _, err := cli.CoreV1().Pods(ns).Create(ctx, pod, metav1m.CreateOptions{}); err != nil {
				return err
			}
			ip := fmt.Sprintf("10.%d.%d.%d", 66+(i>>16)&0x3F, (i>>8)&0xFF, i&0xFF)
			patch := fmt.Sprintf(`{"status":{"phase":"Running","podIP":"%s","podIPs":[{"ip":"%s"}],"startTime":%q,"conditions":[{"type":"Initialized","status":"True","lastTransitionTime":%q},{"type":"Ready","status":"True","lastTransitionTime":%q},{"type":"ContainersReady","status":"True","lastTransitionTime":%q},{"type":"PodScheduled","status":"True","lastTransitionTime":%q}],"containerStatuses":[{"name":"fake","state":{"running":{"startedAt":%q}},"ready":true,"restartCount":0,"image":"nginx:latest","imageID":"fake"}]}}`,
				ip, ip, nowS, nowS, nowS, nowS, nowS, nowS)
			_, err := cli.CoreV1().Pods(ns).Patch(ctx, name, k8stypes.StrategicMergePatchType, []byte(patch), metav1m.PatchOptions{}, "status")
			return err
		})

	case "storm":
		// 发布风暴:给 ns 段内每个 deployment 的 pod template 加 annotation 触发
		// 滚动更新(kcm 造新 RS+新 pod、缩旧 RS、删旧 pod ≈ 每 deploy 4+ 写)。
		total := *nsN * *deployN
		run(total, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *deployN)
			name := fmt.Sprintf("app-%03d", i%*deployN)
			patch := []byte(`{"spec":{"template":{"metadata":{"annotations":{"storm":"v1"}}}}}`)
			_, err := cli.AppsV1().Deployments(ns).Patch(ctx, name, k8stypes.StrategicMergePatchType, patch, metav1m.PatchOptions{})
			if err != nil && errors.IsNotFound(err) {
				return nil // ns 内 deploy 数不均,缺的跳过
			}
			return err
		})

	case "services":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *perNs)
			name := fmt.Sprintf("svc-%03d", i%*perNs)
			svc := &corev1.Service{
				ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"}},
				Spec: corev1.ServiceSpec{
					// 无 selector:endpoints controller 不产生派生对象。
					// headless(ClusterIP None):跳过 apiserver 的全局串行 IP
					// allocator(实测有 IP 分配时创建速率只有 ~4/s)。
					Type:      corev1.ServiceTypeClusterIP,
					ClusterIP: corev1.ClusterIPNone,
					Ports:     []corev1.ServicePort{{Name: "p", Port: 80}},
				},
			}
			_, err := cli.CoreV1().Services(ns).Create(ctx, svc, metav1m.CreateOptions{})
			return err
		})

	case "secrets":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *perNs)
			name := fmt.Sprintf("sec-%03d", i%*perNs)
			sec := &corev1.Secret{
				ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"}},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{"k": []byte("v0123456789")},
			}
			_, err := cli.CoreV1().Secrets(ns).Create(ctx, sec, metav1m.CreateOptions{})
			return err
		})

	case "configmaps":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *perNs)
			name := fmt.Sprintf("cm-%03d", i%*perNs)
			_, err := cli.CoreV1().ConfigMaps(ns).Create(ctx,
				&corev1.ConfigMap{ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"}},
					Data: map[string]string{"k": "v0123456789"}},
				metav1m.CreateOptions{})
			return err
		})

	case "serviceaccounts":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *perNs)
			name := fmt.Sprintf("sa-%03d", i%*perNs)
			_, err := cli.CoreV1().ServiceAccounts(ns).Create(ctx,
				&corev1.ServiceAccount{ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"}}},
				metav1m.CreateOptions{})
			return err
		})

	case "roles":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *perNs)
			name := fmt.Sprintf("role-%03d", i%*perNs)
			_, err := cli.RbacV1().Roles(ns).Create(ctx,
				&rbacv1.Role{ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake"}},
					Rules: []rbacv1.PolicyRule{{
						APIGroups: []string{""}, Resources: []string{"pods"},
						Verbs: []string{"get", "list", "watch"}}}},
				metav1m.CreateOptions{})
			return err
		})

	case "endpointslices":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart + i / *perNs)
			name := fmt.Sprintf("eps-%03d", i%*perNs)
			pn := "p"
			pp := int32(80)
			_, err := cli.DiscoveryV1().EndpointSlices(ns).Create(ctx,
				&discoveryv1.EndpointSlice{ObjectMeta: metav1m.ObjectMeta{Name: name, Namespace: ns,
					Labels: map[string]string{"workload-type": "fake", "kubernetes.io/service-name": "svc-fake"}},
					AddressType: discoveryv1.AddressTypeIPv4,
					Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.1"}}},
					Ports:       []discoveryv1.EndpointPort{{Name: &pn, Port: &pp}}},
				metav1m.CreateOptions{})
			return err
		})

	case "namespaces":
		run(*count, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart+i)
			_, err := cli.CoreV1().Namespaces().Create(ctx,
				&corev1.Namespace{ObjectMeta: metav1m.ObjectMeta{
					Name: ns, Labels: map[string]string{"workload-type": "fake"}}},
				metav1m.CreateOptions{})
			return err
		})

	case "services-cleanup":
		run(*count, func(i int) error {
			idx := *nsStart*(*perNs) + i
			ns := fmt.Sprintf("ns-%07d", idx / *perNs)
			name := fmt.Sprintf("svc-%03d", idx%*perNs)
			err := cli.CoreV1().Services(ns).Delete(ctx, name, metav1m.DeleteOptions{})
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		})

	case "nodes-cleanup":
		run(*count, func(i int) error {
			name := fmt.Sprintf("kwok-node-%06d", *start+i)
			err := cli.CoreV1().Nodes().Delete(ctx, name, metav1m.DeleteOptions{})
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		})

	case "cleanup":
		run(*nsN, func(i int) error {
			ns := fmt.Sprintf("ns-%07d", *nsStart+i)
			err := cli.CoreV1().Namespaces().Delete(ctx, ns, metav1m.DeleteOptions{})
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		})

	case "nodes-ready":
		t := time.Now()
		var total, ready int
		cont := ""
		for {
			opts := metav1m.ListOptions{Limit: 5000, Continue: cont}
			if cont == "" {
				opts.ResourceVersion = "0" // cache List:观测用,别打存储
			}
			l, e := cli.CoreV1().Nodes().List(ctx, opts)
			if e != nil {
				fmt.Printf("ERR %v\n", e)
				break
			}
			for i := range l.Items {
				total++
				for _, c := range l.Items[i].Status.Conditions {
					if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
						ready++
						break
					}
				}
			}
			cont = l.Continue
			if cont == "" {
				break
			}
		}
		fmt.Printf("nodes ready=%d/%d (%s)\n", ready, total, time.Since(t).Round(time.Millisecond))

	case "status":
		for _, x := range []struct {
			what string
			fn   func() (int, error)
		}{
			{"nodes", func() (int, error) {
				l, e := cli.CoreV1().Nodes().List(ctx, metav1m.ListOptions{Limit: 1})
				if e != nil {
					return 0, e
				}
				if l.RemainingItemCount != nil {
					return int(*l.RemainingItemCount) + len(l.Items), nil
				}
				return len(l.Items), nil
			}},
			{"namespaces", func() (int, error) {
				l, e := cli.CoreV1().Namespaces().List(ctx, metav1m.ListOptions{Limit: 1})
				if e != nil {
					return 0, e
				}
				if l.RemainingItemCount != nil {
					return int(*l.RemainingItemCount) + len(l.Items), nil
				}
				return len(l.Items), nil
			}},
			{"pods(all)", func() (int, error) {
				l, e := cli.CoreV1().Pods("").List(ctx, metav1m.ListOptions{Limit: 1})
				if e != nil {
					return 0, e
				}
				if l.RemainingItemCount != nil {
					return int(*l.RemainingItemCount) + len(l.Items), nil
				}
				return len(l.Items), nil
			}},
			{"services", func() (int, error) {
				l, e := cli.CoreV1().Services("").List(ctx, metav1m.ListOptions{Limit: 1})
				if e != nil {
					return 0, e
				}
				if l.RemainingItemCount != nil {
					return int(*l.RemainingItemCount) + len(l.Items), nil
				}
				return len(l.Items), nil
			}},
			{"secrets", func() (int, error) {
				l, e := cli.CoreV1().Secrets("").List(ctx, metav1m.ListOptions{Limit: 1})
				if e != nil {
					return 0, e
				}
				if l.RemainingItemCount != nil {
					return int(*l.RemainingItemCount) + len(l.Items), nil
				}
				return len(l.Items), nil
			}},
			{"deployments", func() (int, error) {
				l, e := cli.AppsV1().Deployments("").List(ctx, metav1m.ListOptions{Limit: 1})
				if e != nil {
					return 0, e
				}
				if l.RemainingItemCount != nil {
					return int(*l.RemainingItemCount) + len(l.Items), nil
				}
				return len(l.Items), nil
			}},
		} {
			t := time.Now()
			n, e := x.fn()
			if e != nil {
				fmt.Printf("%-12s ERR %v\n", x.what, e)
				continue
			}
			fmt.Printf("%-12s %8d  (%s)\n", x.what, n, time.Since(t).Round(time.Millisecond))
		}
	}
}
