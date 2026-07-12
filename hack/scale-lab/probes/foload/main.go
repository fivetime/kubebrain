// failover-under-load: continuous puts, report ops/errs every 3s (fine gap detection).
package main

import (
	"context"
	"flag"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var c, durS int
	var ep, prefix string
	flag.IntVar(&c, "c", 100, "")
	flag.IntVar(&durS, "d", 420, "")
	flag.StringVar(&ep, "ep", "172.18.0.2:30079", "")
	flag.StringVar(&prefix, "prefix", "/foload", "")
	flag.Parse()
	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{ep}, DialTimeout: 5 * time.Second})
	defer cli.Close()
	var ok, err int64
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durS)*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < c; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for ctx.Err() == nil {
				wc, cc := context.WithTimeout(context.Background(), 2*time.Second)
				_, e := cli.Put(wc, fmt.Sprintf("%s/w%d/k%d", prefix, id, n), "v")
				cc()
				if e != nil {
					atomic.AddInt64(&err, 1)
				} else {
					atomic.AddInt64(&ok, 1)
				}
				n++
			}
		}(w)
	}
	// reporter every 3s: delta ok/err
	go func() {
		var po, pe int64
		tk := time.NewTicker(3 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				o := atomic.LoadInt64(&ok)
				e := atomic.LoadInt64(&err)
				fmt.Printf("%s ok=+%d err=+%d (rate=%d/s)\n", time.Now().Format("15:04:05"), o-po, e-pe, (o-po)/3)
				po, pe = o, e
			}
		}
	}()
	wg.Wait()
	fmt.Printf("END ok=%d err=%d\n", atomic.LoadInt64(&ok), atomic.LoadInt64(&err))
}
