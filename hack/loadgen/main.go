package main

import (
	"context"
	"flag"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var conc, dur int
	var ep string
	var rangeEvery int
	flag.IntVar(&conc, "c", 200, "concurrency")
	flag.IntVar(&dur, "d", 30, "seconds")
	flag.StringVar(&ep, "ep", "127.0.0.1:3379", "endpoint")
	flag.IntVar(&rangeEvery, "rangeEvery", 20, "1 Range per N Puts")
	flag.Parse()
	var ops, errs int64
	lat := make([][]time.Duration, conc)
	end := time.Now().Add(time.Duration(dur) * time.Second)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cli, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
			if err != nil {
				atomic.AddInt64(&errs, 1)
				return
			}
			defer cli.Close()
			n := 0
			for time.Now().Before(end) {
				n++
				t0 := time.Now()
				var e error
				if rangeEvery > 0 && n%rangeEvery == 0 {
					ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
					_, e = cli.Get(ctx, fmt.Sprintf("/loadtest/w%d/", id), clientv3.WithPrefix(), clientv3.WithLimit(100))
					c()
				} else {
					ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
					_, e = cli.Put(ctx, fmt.Sprintf("/loadtest/w%d/k%d", id, n), "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
					c()
				}
				lat[id] = append(lat[id], time.Since(t0))
				if e != nil {
					atomic.AddInt64(&errs, 1)
				} else {
					atomic.AddInt64(&ops, 1)
				}
			}
		}(w)
	}
	wg.Wait()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	p := func(q float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all))*q)]
	}
	fmt.Printf("conc=%d dur=%ds ops=%d errs=%d  throughput=%.0f ops/s  p50=%v p99=%v max=%v\n",
		conc, dur, ops, errs, float64(ops)/float64(dur), p(0.5), p(0.99), all[len(all)-1])
}
