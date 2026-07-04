// watchgen — stress KubeBrain's watch path at scale.
//
// Two modes (combine freely):
//   - FANOUT:  many watchers from "now" + a writer -> one event fans to N watchers.
//   - HERD (#30): many watchers all starting from an OLD revision at once ->
//     each needs watch-history catch-up. If the old revision is beyond the
//     in-memory watch cache (WatchCacheSize, default 200k events) it forces a
//     STORAGE history scan, bounded by historyScanConcurrency=8. This is the
//     reconnect-thundering-herd an apiserver restart triggers.
//
// It reports per-watcher created/caught-up latency and total events, so a herd
// that overloads (huge tail, errors, or a KubeBrain restart) is visible.
package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	var ep, prefix string
	var nWatch, offset, durS, writeConc int
	flag.StringVar(&ep, "ep", "172.18.0.2:30079", "endpoint")
	flag.StringVar(&prefix, "prefix", "/registry", "watch prefix")
	flag.IntVar(&nWatch, "watchers", 200, "concurrent watchers")
	flag.IntVar(&offset, "offset", 0, "start watchers at (current-offset) revisions; 0 = from now. >WatchCacheSize forces storage history scans (herd)")
	flag.IntVar(&durS, "d", 60, "seconds to run")
	flag.IntVar(&writeConc, "write", 0, "concurrent writers generating events (0 = none)")
	flag.Parse()

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durS)*time.Second)
	defer cancel()

	// Pin the current revision so "caught up" is well-defined.
	gr, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		fmt.Printf("initial get failed: %v (using rev 0)\n", err)
	}
	cur := gr.Header.Revision
	startRev := int64(0)
	if offset > 0 {
		startRev = cur - int64(offset)
		if startRev < 1 {
			startRev = 1
		}
	}
	fmt.Printf("watchgen: watchers=%d prefix=%q startRev=%d (current=%d, offset=%d) writers=%d dur=%ds\n",
		nWatch, prefix, startRev, cur, offset, writeConc, durS)

	// optional writers
	var writeOps int64
	var wwg sync.WaitGroup
	for w := 0; w < writeConc; w++ {
		wwg.Add(1)
		go func(id int) {
			defer wwg.Done()
			n := 0
			for ctx.Err() == nil {
				wctx, cc := context.WithTimeout(context.Background(), 3*time.Second)
				_, e := cli.Put(wctx, fmt.Sprintf("%s/watchgen/w%d/k%d", prefix, id, n), "v")
				cc()
				if e == nil {
					atomic.AddInt64(&writeOps, 1)
				}
				n++
			}
		}(w)
	}

	// watchers
	created := make([]time.Duration, nWatch)
	caught := make([]time.Duration, nWatch)
	events := make([]int64, nWatch)
	var errs int64
	var wg sync.WaitGroup
	t0 := time.Now()
	for i := 0; i < nWatch; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wctx, wc := context.WithCancel(ctx)
			defer wc()
			opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithProgressNotify()}
			if startRev > 0 {
				opts = append(opts, clientv3.WithRev(startRev))
			}
			ch := cli.Watch(wctx, prefix, opts...)
			start := time.Now()
			gotCreated, gotCaught := false, false
			for resp := range ch {
				if resp.Err() != nil {
					atomic.AddInt64(&errs, 1)
					return
				}
				if !gotCreated && resp.Created {
					created[idx] = time.Since(start)
					gotCreated = true
				}
				atomic.AddInt64(&events[idx], int64(len(resp.Events)))
				if !gotCaught && resp.Header.Revision >= cur {
					caught[idx] = time.Since(start)
					gotCaught = true
				}
			}
		}(i)
	}
	wg.Wait()
	wwg.Wait()

	report := func(name string, ds []time.Duration) {
		v := make([]time.Duration, 0, len(ds))
		for _, d := range ds {
			if d > 0 {
				v = append(v, d)
			}
		}
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
		if len(v) == 0 {
			fmt.Printf("  %-14s (none)\n", name)
			return
		}
		p := func(q float64) time.Duration { return v[int(float64(len(v)-1)*q)] }
		fmt.Printf("  %-14s n=%d p50=%s p99=%s max=%s\n", name, len(v),
			p(0.5).Round(time.Millisecond), p(0.99).Round(time.Millisecond), v[len(v)-1].Round(time.Millisecond))
	}
	var totEv int64
	for _, e := range events {
		totEv += e
	}
	fmt.Printf("=== RESULT (%.0fs) writers_ops=%d errors=%d total_events_delivered=%d ===\n",
		time.Since(t0).Seconds(), writeOps, errs, totEv)
	report("created", created)
	report("caughtUp", caught)
}
