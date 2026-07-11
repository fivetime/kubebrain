// qlat characterizes KubeBrain write latency, for the single- vs 3-replica
// (raft quorum) comparison of #53. Two phases:
//  1. bare unconditional PUT latency (p50/p99/max), sequential and concurrent
//  2. contended optimistic-concurrency updates (mod_revision CAS on a small hot
//     set) — the #44 head-of-line scenario: losers get a CAS failure and retry,
//     so the committed watermark's handling of failed revisions is on the path.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoint := flag.String("endpoint", "10.224.0.12:4379", "KubeBrain etcd endpoint")
	label := flag.String("label", "run", "label for this run (e.g. r1 or r3)")
	nseq := flag.Int("nseq", 500, "sequential bare PUTs")
	nconc := flag.Int("nconc", 3000, "concurrent bare PUTs")
	conc := flag.Int("conc", 32, "concurrency for the concurrent phases")
	casOps := flag.Int("casops", 4000, "total contended CAS update attempts")
	hotkeys := flag.Int("hotkeys", 8, "hot keys the CAS writers contend on")
	flag.Parse()

	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{*endpoint}, DialTimeout: 5 * time.Second})
	must(err)
	defer cli.Close()

	pfx := fmt.Sprintf("/qlat/%s/%d/", *label, time.Now().UnixNano())

	// Phase 1a: sequential bare PUT latency.
	seqLat := make([]time.Duration, 0, *nseq)
	for i := 0; i < *nseq; i++ {
		k := fmt.Sprintf("%sseq-%06d", pfx, i)
		t := time.Now()
		_, err := cli.Put(ctx, k, "v")
		must(err)
		seqLat = append(seqLat, time.Since(t))
	}

	// Phase 1b: concurrent bare PUT latency + throughput.
	concLat := make([]time.Duration, *nconc)
	var idx int64 = -1
	t0 := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := atomic.AddInt64(&idx, 1)
				if int(i) >= *nconc {
					return
				}
				k := fmt.Sprintf("%sconc-%06d", pfx, i)
				t := time.Now()
				_, err := cli.Put(ctx, k, "v")
				must(err)
				concLat[i] = time.Since(t)
			}
		}()
	}
	wg.Wait()
	concDur := time.Since(t0)

	// Phase 2: contended optimistic-concurrency CAS on hotkeys.
	// Each attempt reads the key's mod_revision then Txn-puts guarded on it;
	// a lost race returns Succeeded=false (CAS failure) and is retried. This
	// is the apiserver's write pattern and the #44 head-of-line generator.
	for h := 0; h < *hotkeys; h++ {
		_, err := cli.Put(ctx, fmt.Sprintf("%shot-%d", pfx, h), "0")
		must(err)
	}
	var casIdx int64 = -1
	var casFail int64
	casLat := make([]time.Duration, *casOps)
	t1 := time.Now()
	var wg2 sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for {
				i := atomic.AddInt64(&casIdx, 1)
				if int(i) >= *casOps {
					return
				}
				hk := fmt.Sprintf("%shot-%d", pfx, int(i)%*hotkeys)
				t := time.Now()
				for { // retry until this attempt commits
					gr, err := cli.Get(ctx, hk)
					must(err)
					var mod int64
					if len(gr.Kvs) > 0 {
						mod = gr.Kvs[0].ModRevision
					}
					txn, err := cli.Txn(ctx).
						If(clientv3.Compare(clientv3.ModRevision(hk), "=", mod)).
						Then(clientv3.OpPut(hk, fmt.Sprintf("%d", i))).
						Commit()
					must(err)
					if txn.Succeeded {
						break
					}
					atomic.AddInt64(&casFail, 1)
				}
				casLat[i] = time.Since(t)
			}
		}()
	}
	wg2.Wait()
	casDur := time.Since(t1)

	fmt.Printf("=== qlat label=%s endpoint=%s ===\n", *label, *endpoint)
	report("seq PUT    ", seqLat, 0)
	report("conc PUT   ", concLat, concDur)
	fmt.Printf("  conc PUT throughput: %.0f writes/s\n", float64(*nconc)/concDur.Seconds())
	report("CAS update ", casLat, casDur)
	fmt.Printf("  CAS attempts=%d fails=%d fail_rate=%.1f%% throughput=%.0f ops/s\n",
		*casOps, casFail, 100*float64(casFail)/float64(int64(*casOps)+casFail), float64(*casOps)/casDur.Seconds())
}

func report(name string, lat []time.Duration, dur time.Duration) {
	s := append([]time.Duration(nil), lat...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	p := func(q float64) time.Duration { return s[min(int(float64(len(s))*q), len(s)-1)] }
	fmt.Printf("  %s n=%d p50=%s p90=%s p99=%s max=%s\n",
		name, len(s), p(0.50).Round(time.Millisecond), p(0.90).Round(time.Millisecond),
		p(0.99).Round(time.Millisecond), s[len(s)-1].Round(time.Millisecond))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func must(err error) {
	if err != nil {
		fmt.Println("FATAL:", err)
		os.Exit(1)
	}
}
