// bulk writer for massive-scale test: write -n keys to <prefix>/w{id}/k{n}.
package main
import (
	"context";"flag";"fmt";"sync";"sync/atomic";"time"
	clientv3 "go.etcd.io/etcd/client/v3"
)
func main(){
	var n,c int; var ep,prefix,val string
	flag.IntVar(&n,"n",10000000,"total keys"); flag.IntVar(&c,"c",300,"concurrency")
	flag.StringVar(&ep,"ep","172.18.0.2:30079",""); flag.StringVar(&prefix,"prefix","/scale-test","")
	flag.StringVar(&val,"val","vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv","value")
	flag.Parse()
	cli,_:=clientv3.New(clientv3.Config{Endpoints:[]string{ep},DialTimeout:5*time.Second});defer cli.Close()
	var done,errs int64; per:=n/c; t0:=time.Now()
	// progress reporter
	stop:=make(chan struct{})
	go func(){ tk:=time.NewTicker(15*time.Second); defer tk.Stop()
		for{select{case <-stop:return;case <-tk.C:
			d:=atomic.LoadInt64(&done); el:=time.Since(t0).Seconds()
			fmt.Printf("[%s] written=%d (%.0f/s) errs=%d\n",time.Since(t0).Round(time.Second),d,float64(d)/el,atomic.LoadInt64(&errs))
		}}}()
	var wg sync.WaitGroup
	for w:=0;w<c;w++{wg.Add(1);go func(id int){defer wg.Done()
		for i:=0;i<per;i++{
			ctx,cc:=context.WithTimeout(context.Background(),10*time.Second)
			_,e:=cli.Put(ctx,fmt.Sprintf("%s/w%d/k%08d",prefix,id,i),val);cc()
			if e!=nil{atomic.AddInt64(&errs,1)}else{atomic.AddInt64(&done,1)}
		}}(w)}
	wg.Wait(); close(stop)
	fmt.Printf("DONE written=%d errs=%d in %s (%.0f/s)\n",atomic.LoadInt64(&done),atomic.LoadInt64(&errs),
		time.Since(t0).Round(time.Second),float64(atomic.LoadInt64(&done))/time.Since(t0).Seconds())
}
